"""Tests for bundle component selection on the product variant stream."""

import functools
import importlib
import json
import sys
import types

import pytest


@pytest.fixture
def hotglue_sdk_stubs(monkeypatch):
    """Stub the pieces of the SDK the client modules import at load time."""
    streams_stub = types.ModuleType("hotglue_singer_sdk.streams")
    streams_stub.GraphQLStream = object

    exceptions_stub = types.ModuleType("hotglue_singer_sdk.exceptions")
    exceptions_stub.RetriableAPIError = Exception

    authenticators_stub = types.ModuleType("hotglue_singer_sdk.authenticators")

    class APIKeyAuthenticator:
        @classmethod
        def create_for_stream(cls, *args, **kwargs):
            return cls()

    authenticators_stub.APIKeyAuthenticator = APIKeyAuthenticator

    jsonpath_stub = types.ModuleType("hotglue_singer_sdk.helpers.jsonpath")
    jsonpath_stub.extract_jsonpath = lambda *args, **kwargs: iter(())

    auth_stub = types.ModuleType("tap_shopify_beta.auth")
    auth_stub.ShopifyAuthenticator = object

    backports_stub = types.ModuleType("backports")
    cached_property_stub = types.ModuleType("backports.cached_property")
    cached_property_stub.cached_property = functools.cached_property
    backports_stub.cached_property = cached_property_stub

    backoff_stub = types.ModuleType("backoff")
    backoff_stub.expo = lambda *args, **kwargs: None
    backoff_stub.on_exception = lambda *args, **kwargs: (lambda func: func)

    simplejson_stub = types.ModuleType("simplejson")
    simplejson_stub.JSONDecodeError = json.JSONDecodeError
    simplejson_stub.loads = json.loads

    monkeypatch.setitem(
        sys.modules, "hotglue_singer_sdk", types.ModuleType("hotglue_singer_sdk")
    )
    monkeypatch.setitem(
        sys.modules,
        "hotglue_singer_sdk.helpers",
        types.ModuleType("hotglue_singer_sdk.helpers"),
    )
    monkeypatch.setitem(sys.modules, "hotglue_singer_sdk.helpers.jsonpath", jsonpath_stub)
    monkeypatch.setitem(sys.modules, "hotglue_singer_sdk.streams", streams_stub)
    monkeypatch.setitem(sys.modules, "hotglue_singer_sdk.exceptions", exceptions_stub)
    monkeypatch.setitem(sys.modules, "hotglue_singer_sdk.authenticators", authenticators_stub)
    monkeypatch.setitem(sys.modules, "tap_shopify_beta.auth", auth_stub)
    monkeypatch.setitem(sys.modules, "backports", backports_stub)
    monkeypatch.setitem(sys.modules, "backports.cached_property", cached_property_stub)
    monkeypatch.setitem(sys.modules, "backoff", backoff_stub)
    monkeypatch.setitem(sys.modules, "simplejson", simplejson_stub)
    yield
    for module_name in ("tap_shopify_beta.client", "tap_shopify_beta.client_bulk"):
        sys.modules.pop(module_name, None)


@pytest.fixture
def client(hotglue_sdk_stubs):
    sys.modules.pop("tap_shopify_beta.client", None)
    module = importlib.import_module("tap_shopify_beta.client")
    yield module
    sys.modules.pop("tap_shopify_beta.client", None)


@pytest.fixture
def client_bulk(client):
    sys.modules.pop("tap_shopify_beta.client_bulk", None)
    module = importlib.import_module("tap_shopify_beta.client_bulk")
    yield module
    sys.modules.pop("tap_shopify_beta.client_bulk", None)


VARIANT_SCHEMA = {
    "properties": {
        "id": {"type": ["string", "null"]},
        "sku": {"type": ["string", "null"]},
        "requiresComponents": {"type": ["boolean", "null"]},
        "metafields": {
            "type": ["array", "null"],
            "items": {"properties": {"id": {}, "key": {}}},
        },
        "productVariantComponents": {
            "type": ["array", "null"],
            "items": {
                "properties": {
                    "id": {},
                    "quantity": {},
                    "productVariant": {"properties": {"id": {}, "sku": {}}},
                }
            },
        },
    }
}


def _variant_stream(client_module, selected=None):
    stream = client_module.shopifyStream.__new__(client_module.shopifyStream)
    stream.schema = VARIANT_SCHEMA
    stream.extra_paginated_fields = {"productVariantComponents": 25}
    stream.__dict__["selected_properties"] = selected or list(
        VARIANT_SCHEMA["properties"]
    )
    return stream


def test_paginated_fields_merges_stream_extras(client):
    stream = _variant_stream(client)

    assert stream.paginated_fields == {
        "metafields": 50,
        "refundLineItems": 50,
        "productVariantComponents": 25,
    }


def test_default_paginated_fields_unchanged_without_extras(client):
    stream = client.shopifyStream.__new__(client.shopifyStream)

    assert stream.paginated_fields == {"metafields": 50, "refundLineItems": 50}


def test_components_selected_as_connection(client):
    query = _variant_stream(client).gql_selected_fields

    assert "productVariantComponents(first: 25) {" in query
    components = query.split("productVariantComponents(first: 25) {", 1)[1]
    # A connection must be traversed through edges/node, not selected directly.
    assert components.lstrip().startswith("edges {")
    assert "node {" in components
    assert "quantity" in components
    assert "requiresComponents" in query
    assert "metafields(first: 50) {" in query


def test_unselected_components_cost_nothing(client):
    stream = _variant_stream(client, selected=["id", "sku"])

    query = stream.gql_selected_fields

    assert "productVariantComponents" not in query


def test_fetch_connection_returns_nodes_without_extra_request(client):
    stream = client.shopifyStream.__new__(client.shopifyStream)
    component = {
        "id": "gid://shopify/ProductVariantComponent/9",
        "quantity": 3,
        "productVariant": {"id": "gid://shopify/ProductVariant/2", "sku": "COMP-1"},
    }
    record = {
        "id": "gid://shopify/ProductVariant/1",
        "productVariantComponents": {
            "edges": [{"cursor": "c1", "node": component}],
            "pageInfo": {"hasNextPage": False},
        },
    }

    nodes = stream._fetch_paginated_connection(
        record, "productVariantComponents", page_size=25
    )

    assert nodes == [component]


def test_bulk_post_process_flattens_declared_connections(client_bulk):
    stream = client_bulk.shopifyBulkStream.__new__(client_bulk.shopifyBulkStream)
    stream.extra_paginated_fields = {"productVariantComponents": 25}
    component = {"id": "gid://shopify/ProductVariantComponent/9", "quantity": 2}
    row = {
        "id": "gid://shopify/ProductVariant/1",
        "productVariantComponents": {"edges": [{"node": component}]},
    }

    assert stream.post_process(row)["productVariantComponents"] == [component]


def test_bulk_post_process_leaves_rows_without_components(client_bulk):
    stream = client_bulk.shopifyBulkStream.__new__(client_bulk.shopifyBulkStream)
    stream.extra_paginated_fields = {"productVariantComponents": 25}
    row = {"id": "gid://shopify/ProductVariant/1", "sku": "PLAIN"}

    assert stream.post_process(row) == row
