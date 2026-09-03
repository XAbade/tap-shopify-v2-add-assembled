"""GraphQL client handling, including shopifyStream base class."""

import backoff
import requests
import urllib3

from typing import Any, Optional, Callable
from hotglue_singer_sdk.authenticators import APIKeyAuthenticator
from backports.cached_property import cached_property
from hotglue_singer_sdk.streams import GraphQLStream
from tap_shopify_beta.auth import ShopifyAuthenticator
from hotglue_singer_sdk.exceptions import RetriableAPIError
import psutil
import os
import http.client
import re

class shopifyStream(GraphQLStream):
    """shopify stream class."""

    query_name = None

    #: Connection fields that must be selected as ``edges { node { ... } }``
    #: instead of a plain object selection, mapped to their page size.
    #: ``page_size`` only applies to the plain GraphQL path - the Bulk API
    #: ignores pagination arguments and always returns every node.
    default_paginated_fields = {"metafields": 50, "refundLineItems": 50}

    #: Per-stream additions to (or overrides of) ``default_paginated_fields``.
    #: Declaring a connection here is enough for the GraphQL path to select it
    #: correctly, paginate any overflow and flatten it into a plain list.
    extra_paginated_fields: dict = {}

    @property
    def paginated_fields(self) -> dict:
        """Return every connection field needing an edges/node selection."""
        return {**self.default_paginated_fields, **self.extra_paginated_fields}

    def get_shop_name(self) -> str:
        """Return the shop name, configurable via tap settings."""
        shop_no_https = self.config["shop"].replace("https://", "")
        shop_no_extra_slashes = re.sub('/.*', '', shop_no_https)
        shop = shop_no_extra_slashes[:-len(".myshopify.com")] if shop_no_extra_slashes.endswith(".myshopify.com") else shop_no_extra_slashes
        return shop

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        shop = self.get_shop_name()
        return f"https://{shop}.myshopify.com/admin/api/2026-07/graphql.json"

    @property
    def authenticator(self) -> ShopifyAuthenticator:
        """Return a new authenticator object."""
        if self.config.get("client_id"):
            shop = self.get_shop_name()
            return ShopifyAuthenticator(
                self, self._tap.config, f"https://{shop}.myshopify.com/admin/oauth/access_token"
            )
        else:
            return APIKeyAuthenticator.create_for_stream(
            self,
            key="X-Shopify-Access-Token",
            value=str(self.config.get("api_key")),
            location="header",
        )

    @cached_property
    def selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and (value.selected or value.inclusion == "automatic"):
                field_name = key[-1]
                selected_properties.append(field_name)
        return selected_properties

    @property
    def gql_selected_fields(self):
        schema = self.schema["properties"]
        catalog = {k: v for k, v in schema.items() if k in self.selected_properties}

        output = []
        for key, value in catalog.items():
            if "items" in value:
                value = value["items"]
            if key == "lineItems":
                # Handle lineItems pagination
                if hasattr(self, 'first_line_item'):
                    after = self.after_line_item if hasattr(self, "after_line_item") else None
                    query = self.get_field_query(
                        key,
                        value["properties"],
                        is_paginated=True,
                        page_size=self.first_line_item,
                        after=after,
                    )
                else:
                    query = self.get_field_query(key, value["properties"])
                output.append(query)
            elif key in self.paginated_fields:
                query = self.get_field_query(
                    key,
                    value["properties"],
                    is_paginated=True,
                    page_size=self.paginated_fields[key],
                )
                output.append(query)
            elif "properties" in value:
                query = self.get_field_query(key, value["properties"])
                output.append(query)
            else:
                output.append(key)

        return "\n".join(output)

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the GraphQL API request."""
        params = self.get_url_params(context, next_page_token)
        query = self.query.lstrip()
        request_data = {
            "query": (" ".join([line.strip() for line in query.splitlines()])),
            "variables": params,
        }
        # self.logger.info(f"Attempting request with variables {params} and query: {request_data['query']}")
        return request_data

    def get_field_query(
        self,
        field_name: str,
        schema: dict,
        is_paginated: bool = False,
        page_size: int = None,
        after: Optional[str] = None,
    ) -> str:
        """Generate a GraphQL query string for a given field based on its schema."""
        output = []

        if is_paginated:
            pagination = f"(first: {page_size})" if page_size else ""
            if after:
                pagination = f"{pagination[:-1]}, after: {after})" if pagination else f"(after: {after})"
            output.append(f"{field_name}{pagination} {{")
            output.append("edges {")
            output.append("cursor")
            output.append("node {")
        else:
            output.append(f"{field_name} {{")

        for key, value in schema.items():
            if "items" in value:
                value = value["items"]
            if "properties" in value:
                nested_query = self.get_field_query(key, value["properties"])
                output.append(nested_query)
            else:
                output.append(key)

        if is_paginated:
            output.append("}")  # Close node
            output.append("}")  # Close edges
            output.append("pageInfo { hasNextPage }")
        output.append("}")

        return "\n".join(output)

    def request_decorator(self, func: Callable) -> Callable:
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                urllib3.exceptions.HTTPError,       
                http.client.HTTPException,
                requests.exceptions.RequestException,
            ),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator
    
    def _build_schema_fields_query(self, schema: dict) -> str:
        """Build a GraphQL field selection string from a JSON schema properties dict."""
        parts = []
        for key, value in schema.items():
            if "items" in value:
                value = value["items"]
            if "properties" in value:
                parts.append(" ".join(self.get_field_query(key, value["properties"]).split()))
            else:
                parts.append(key)
        return " ".join(parts)

    def _fetch_paginated_connection(
        self,
        record: dict,
        field_name: str,
        resource_type: Optional[str] = None,
        page_size: int = 250,
        default_fields: str = "id",
    ) -> list:
        """Fetch all nodes from a paginated connection field via the node interface."""
        resource_gid = record["id"]
        if resource_type is None:
            resource_type = resource_gid.split("/")[-2]

        connection = record.get(field_name, {})
        all_edges = list(connection.get("edges", []))
        has_next = connection.get("pageInfo", {}).get("hasNextPage", False)

        if not has_next:
            return [e["node"] for e in all_edges]

        self.logger.info(f"Fetching additional {field_name} pages for {resource_gid}")
        decorated_request = self.request_decorator(self._request)

        field_schema = (
            self.schema.get("properties", {})
            .get(field_name, {})
            .get("items", {})
            .get("properties", {})
        )
        node_fields = (
            self._build_schema_fields_query(field_schema) if field_schema else default_fields
        )

        while has_next:
            after_cursor = all_edges[-1]["cursor"]
            query = (
                f'query {{ node(id: "{resource_gid}") {{'
                f' ... on {resource_type} {{'
                f' {field_name}(first: {page_size}, after: "{after_cursor}") {{'
                f' edges {{ cursor node {{ {node_fields} }} }}'
                f' pageInfo {{ hasNextPage }}'
                f' }} }} }} }}'
            )
            headers = {**self.http_headers, **(self.authenticator.auth_headers or {})}
            prepared = self.requests_session.prepare_request(
                requests.Request(
                    method="POST",
                    url=self.url_base,
                    headers=headers,
                    json={"query": query},
                )
            )
            resp = decorated_request(prepared, {})
            node_data = resp.json().get("data", {}).get("node", {})
            page = node_data.get(field_name, {})
            all_edges.extend(page.get("edges", []))
            has_next = page.get("pageInfo", {}).get("hasNextPage", False)

        return [e["node"] for e in all_edges]

    def _fetch_all_metafields(self, record: dict) -> list:
        """Fetch all metafields for a record, paginating through additional pages via the node interface."""
        return self._fetch_paginated_connection(
            record,
            "metafields",
            default_fields="id key namespace value type",
        )

    def _fetch_all_refund_line_items(self, record: dict) -> list:
        """Fetch all refund line items, paginating through additional pages via the node interface."""
        return self._fetch_paginated_connection(
            record,
            "refundLineItems",
            resource_type="Refund",
            default_fields="id quantity restockType",
        )

    def log_memory_usage(self, tag=""):
        process = psutil.Process(os.getpid())
        mem = process.memory_info().rss / (1024 * 1024)  # In MB
        self.logger.info(f"[MEMORY] {tag}: {mem:.2f} MB")
