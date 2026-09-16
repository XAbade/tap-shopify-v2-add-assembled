"""Stream type classes for tap-shopify-beta."""
import abc
from backports.cached_property import cached_property
import json
import sys
from typing import Dict, Iterable, Optional, List, Any, Set
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath

from hotglue_singer_sdk import typing as th

from tap_shopify_beta.client_bulk import shopifyBulkStream
from tap_shopify_beta.client_gql import shopifyGqlStream, GqlChildStream
from tap_shopify_beta.client_rest import shopifyRestStream
from tap_shopify_beta.shopify_dates import to_shopify_utc
from tap_shopify_beta.types.order_app import OrderAppType
from tap_shopify_beta.types.channel_information import ChannelInformationType
from tap_shopify_beta.types.customer_email_marketing_consent_state import CustomerEmailMarketingConsentStateType
from tap_shopify_beta.types.customer_visit import CustomerVisitType
from tap_shopify_beta.types.dispute import DisputeType
from tap_shopify_beta.types.image import ImageType
from tap_shopify_beta.types.last_order import LastOrderType
from tap_shopify_beta.types.line_item_node import LineItemNodeType
from tap_shopify_beta.types.count import CountType
from tap_shopify_beta.types.location import LocationType
from tap_shopify_beta.types.mergeable import MergeableType
from tap_shopify_beta.types.money_bag import MoneyBagType
from tap_shopify_beta.types.money_v2 import MoneyV2Type
from tap_shopify_beta.types.mailing_address import MailingAddressType
from tap_shopify_beta.types.sms_marketing_consent import SmsMarketingConsentType
from tap_shopify_beta.types.discount_allocations import DiscountAllocationsType
import copy
from hotglue_singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error,
)
from hotglue_singer_sdk.exceptions import InvalidStreamSortException
import requests

config_path = "config.json"
for i, arg in enumerate(sys.argv):
    if arg == "--config":
        if i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
        break

try:
    with open(config_path, "r") as jsonfile:
        data = json.load(jsonfile)
except FileNotFoundError:
    data = {}

stream_condition = data.get("bulk", False)
class DynamicStream(shopifyBulkStream if stream_condition else shopifyGqlStream):
    pass

class ProductsStream(DynamicStream):
    """Define product stream."""

    name = "products"
    primary_keys = ["id", "updatedAt"]
    query_name = "products"
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("description", th.StringType),
        th.Property("descriptionHtml", th.StringType),
        th.Property(
            "featuredImage",
            th.ObjectType(
                th.Property("id", th.StringType), th.Property("altText", th.StringType)
            ),
        ),
        th.Property("giftCardTemplateSuffix", th.StringType),
        th.Property("handle", th.StringType),
        th.Property("hasOnlyDefaultVariant", th.BooleanType),
        th.Property("hasOutOfStockVariants", th.BooleanType),
        th.Property("isGiftCard", th.BooleanType),
        th.Property("legacyResourceId", th.StringType),
        th.Property("mediaCount", CountType()),
        th.Property("onlineStorePreviewUrl", th.StringType),
        th.Property("onlineStoreUrl", th.StringType),
        th.Property(
            "options",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("position", th.IntegerType),
                    th.Property("values", th.ArrayType(th.StringType)),
                )
            ),
        ),
        th.Property(
            "priceRangeV2",
            th.ObjectType(
                th.Property(
                    "maxVariantPrice",
                    th.ObjectType(
                        th.Property("amount", th.StringType),
                        th.Property("currencyCode", th.StringType),
                    ),
                ),
                th.Property(
                    "minVariantPrice",
                    th.ObjectType(
                        th.Property("amount", th.StringType),
                        th.Property("currencyCode", th.StringType),
                    ),
                ),
            ),
        ),
        th.Property("productType", th.StringType),
        th.Property("publishedAt", th.DateTimeType),
        th.Property("requiresSellingPlan", th.BooleanType),
        th.Property("sellingPlanGroupCount", th.IntegerType),
        th.Property(
            "seo",
            th.ObjectType(
                th.Property("title", th.StringType),
                th.Property("description", th.StringType),
            ),
        ),
        th.Property("status", th.StringType),
        th.Property("templateSuffix", th.StringType),
        th.Property("title", th.StringType),
        th.Property("totalInventory", th.IntegerType),
        th.Property("totalVariants", th.IntegerType),
        th.Property("tracksInventory", th.BooleanType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("vendor", th.StringType),
        th.Property("metafields", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("key", th.StringType),
            th.Property("namespace", th.StringType),
            th.Property("value", th.StringType),
            th.Property("type", th.StringType),
        ))),
        th.Property("tags", th.ArrayType(th.StringType))
    ).to_dict()

    bulk_process_fields = {"Metafield": "metafields"}


class VariantsStream(DynamicStream):
    """Define variant stream."""

    name = "variants"
    primary_keys = ["id", "updatedAt"]
    query_name = "productVariants"
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("availableForSale", th.BooleanType),
        th.Property("barcode", th.StringType),
        th.Property("compareAtPrice", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("displayName", th.StringType),
        th.Property(
            "image",
            th.ObjectType(
                th.Property("id", th.StringType), th.Property("altText", th.StringType)
            ),
        ),
        th.Property("inventoryItem", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("measurement", th.ObjectType(
                th.Property("weight", th.ObjectType(
                    th.Property("unit", th.StringType),
                    th.Property("value", th.NumberType),
                )),
            )),
        )),
        th.Property("inventoryPolicy", th.StringType),
        th.Property("inventoryQuantity", th.IntegerType),
        th.Property("legacyResourceId", th.StringType),
        th.Property("position", th.IntegerType),
        th.Property("price", th.StringType),
        th.Property("product", th.ObjectType(th.Property("id", th.StringType))),
        th.Property(
            "selectedOptions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("value", th.StringType),
                )
            ),
        ),
        th.Property("sellingPlanGroupCount", th.IntegerType),
        th.Property("sku", th.StringType),
        th.Property("taxable", th.BooleanType),
        th.Property("title", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("metafields", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("key", th.StringType),
            th.Property("namespace", th.StringType),
            th.Property("value", th.StringType),
            th.Property("type", th.StringType),
        ))),
    ).to_dict()

    bulk_process_fields = {"Metafield": "metafields"}


class ComposedProductsStream(shopifyGqlStream):
    """Product variant components that make up Shopify bundles."""

    name = "composed_products"
    primary_keys = ["id"]
    query_name = "productVariants"
    json_path = "$.data.productVariants.edges[*].node"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("composedProductId", th.StringType),
        th.Property("partProductId", th.StringType),
        th.Property("partQuantity", th.IntegerType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()

    @cached_property
    def query(self) -> str:
        return """
            query tapShopify($first: Int, $after: String, $filter: String) {
                productVariants(
                    first: $first, after: $after, query: $filter
                ) {
                    edges {
                        cursor
                        node {
                            id
                            updatedAt
                            productVariantComponents(first: 30) {
                                edges {
                                    node {
                                        id
                                        quantity
                                        productVariant { id }
                                    }
                                }
                            }
                        }
                    }
                    pageInfo { hasNextPage }
                }
            }
        """

    def get_records(self, context):
        if self.config.get("sync_composed_products") is False:
            return
        yield from super().get_records(context)

    def get_url_params(self, context, next_page_token):
        params = {
            "first": self.page_size,
            "filter": "requires_components:true",
        }
        if next_page_token:
            params["after"] = next_page_token
        return params

    def get_next_page_token(self, response, previous_token):
        connection = (
            response.json().get("data", {}).get(self.query_name, {})
        )
        if not connection.get("pageInfo", {}).get("hasNextPage"):
            return None
        edges = connection.get("edges", [])
        cursor = edges[-1].get("cursor") if edges else None
        if not cursor or cursor == previous_token:
            raise RuntimeError(
                f"Non-progressing pagination for stream {self.name}"
            )
        return cursor

    def filter_response(self, response_json: dict) -> dict:
        connection = response_json.get("data", {}).get(self.query_name)
        if not connection:
            return response_json

        records = []
        for edge in connection.get("edges", []):
            parent = edge.get("node") or {}
            components = parent.get("productVariantComponents", {}).get(
                "edges", []
            )
            for component_edge in components:
                component = component_edge.get("node") or {}
                part = component.get("productVariant") or {}
                records.append({
                    "node": {
                        "id": component.get("id"),
                        "composedProductId": parent.get("id"),
                        "partProductId": part.get("id"),
                        "partQuantity": component.get("quantity"),
                        "updatedAt": parent.get("updatedAt"),
                    }
                })
        connection["edges"] = records
        return response_json


class OrdersStream(DynamicStream):
    """Define orders stream."""

    name = "orders"
    primary_keys = ["id", "updatedAt"]
    query_name = "orders"
    replication_key = "updatedAt"
    first_line_item = 25  # works as page_size for line_items
    _after_line_item = None
    last_replication_key = None
    sort_key = "UPDATED_AT"
    sort_key_type = "OrderSortKeys"
    child_context_keys = ["fulfillments", "refunds"]

    child_size = 140 # value based on the estimated query cost of child stream and the max allowed by the API (1000 per request)

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("app", OrderAppType()),
        th.Property("channelInformation", ChannelInformationType()),
        th.Property("billingAddress",MailingAddressType()),
        th.Property("billingAddressMatchesShippingAddress", th.BooleanType),
        th.Property("cancelledAt", th.DateTimeType),
        th.Property("cancelReason", th.StringType),
        th.Property("canMarkAsPaid", th.BooleanType),
        th.Property("canNotifyCustomer", th.BooleanType),
        th.Property("capturable", th.BooleanType),
        th.Property("cartDiscountAmountSet", MoneyBagType()),
        th.Property("clientIp", th.StringType),
        th.Property("closed", th.BooleanType),
        th.Property("closedAt", th.DateTimeType),
        th.Property("confirmed", th.BooleanType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("currencyCode", th.StringType),
        th.Property("currentCartDiscountAmountSet", MoneyBagType()),
        th.Property("currentSubtotalLineItemsQuantity", th.IntegerType),
        th.Property("currentSubtotalPriceSet", MoneyBagType()),
        th.Property(
            "currentTaxLines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("channelLiable", th.StringType),
                    th.Property("priceSet", MoneyBagType()),
                    th.Property("rate", th.NumberType),
                    th.Property("ratePercentage", th.NumberType),
                    th.Property("title", th.StringType),
                )
            ),
        ),
        th.Property("currentTotalDiscountsSet", MoneyBagType()),
        th.Property("currentTotalDutiesSet", MoneyBagType()),
        th.Property("currentTotalPriceSet", MoneyBagType()),
        th.Property("currentTotalTaxSet", MoneyBagType()),
        th.Property("currentTotalWeight", th.StringType),
        th.Property("customerId", th.StringType),
        th.Property("customerAcceptsMarketing", th.BooleanType),
        th.Property("customerLocale", th.StringType),
        th.Property("discountCode", th.StringType),
        th.Property("displayAddress", MailingAddressType()),
        th.Property("displayFinancialStatus", th.StringType),
        th.Property("displayFulfillmentStatus", th.StringType),
        th.Property("disputes",th.ArrayType(DisputeType())),
        th.Property("edited", th.BooleanType),
        th.Property("email", th.StringType),
        th.Property("estimatedTaxes", th.BooleanType),
        th.Property("fulfillable", th.BooleanType),
        th.Property("fulfillments", th.ArrayType(
            th.ObjectType(
                th.Property("id", th.StringType)
            )),
        ),
        th.Property("fullyPaid", th.BooleanType),
        th.Property("legacyResourceId", th.StringType),
        th.Property("hasTimelineComment", th.BooleanType),
        th.Property("merchantEditable", th.BooleanType),
        th.Property("name", th.StringType),
        th.Property("netPaymentSet", MoneyBagType()),
        th.Property("note", th.StringType),
        th.Property("originalTotalDutiesSet", MoneyBagType()),
        th.Property("originalTotalPriceSet", MoneyBagType()),
        th.Property("paymentGatewayNames", th.ArrayType(th.StringType)),
        th.Property("phone", th.StringType),
        th.Property("presentmentCurrencyCode", th.StringType),
        th.Property("processedAt", th.DateTimeType),
        th.Property("refundable", th.BooleanType),
        th.Property("refundDiscrepancySet", MoneyBagType()),
        # move to a new stream incrementally 
        th.Property(
            "refunds",
            th.ArrayType(th.ObjectType(
                th.Property("id", th.StringType),
            ),
        )),
        th.Property("registeredSourceUrl", th.StringType),
        th.Property("requiresShipping", th.BooleanType),
        th.Property("restockable", th.BooleanType),
        th.Property("riskLevel", th.StringType),
        th.Property(
            "risks",
            th.ArrayType(
                th.ObjectType(
                    th.Property("display", th.BooleanType),
                    th.Property("level", th.StringType),
                    th.Property("message", th.StringType),
                )
            ),
        ),
        th.Property("shippingAddress", MailingAddressType()),
        th.Property(
            "shippingLine",
            th.ObjectType(
                th.Property("carrierIdentifier", th.StringType),
                th.Property("code", th.StringType),
                th.Property("custom", th.BooleanType),
                th.Property("discountAllocations", th.ArrayType(DiscountAllocationsType())),
            ),
        ),
        th.Property("subtotalLineItemsQuantity", th.IntegerType),
        th.Property("subtotalPriceSet", MoneyBagType()),
        th.Property("taxesIncluded", th.BooleanType),
        th.Property(
            "taxLines",
            th.ArrayType(
                th.ObjectType(
                    th.Property("channelLiable", th.BooleanType),
                    th.Property("priceSet", MoneyBagType()),
                    th.Property("rate", th.NumberType),
                    th.Property("ratePercentage", th.NumberType),
                    th.Property("title", th.StringType),
                )
            ),
        ),
        th.Property("totalCapturableSet", MoneyBagType()),
        th.Property("totalDiscountsSet", MoneyBagType()),
        th.Property("totalOutstandingSet", MoneyBagType()),
        th.Property("totalPriceSet", MoneyBagType()),
        th.Property("totalReceivedSet", MoneyBagType()),
        th.Property("totalRefundedSet", MoneyBagType()),
        th.Property("totalRefundedShippingSet", MoneyBagType()),
        th.Property("totalShippingPriceSet", MoneyBagType()),
        th.Property("totalTaxSet", MoneyBagType()),
        th.Property("totalTipReceivedSet", MoneyBagType()),
        th.Property("totalWeight", th.StringType),
        th.Property(
            "transactions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("accountNumber", th.StringType),
                    th.Property("amountSet", MoneyBagType()),
                    th.Property("authorizationCode", th.StringType),
                    th.Property("authorizationExpiresAt", th.DateTimeType),
                    th.Property("createdAt", th.DateTimeType),
                    th.Property("errorCode", th.StringType),
                    th.Property(
                        "fees",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("amount", MoneyV2Type()),
                                th.Property("flatFee", MoneyV2Type()),
                                th.Property("flatFeeName", th.StringType),
                                th.Property("id", th.StringType),
                                th.Property("rate", th.StringType),
                                th.Property("rateName", th.StringType),
                                th.Property("taxAmount", MoneyV2Type()),
                                th.Property("type", th.StringType),
                            )
                        ),
                    ),
                    th.Property("formattedGateway", th.StringType),
                    th.Property("gateway", th.StringType),
                    th.Property("kind", th.StringType),
                    th.Property("manuallyCapturable", th.BooleanType),
                    th.Property("maximumRefundableV2", MoneyV2Type()),
                    th.Property("multiCapturable", th.BooleanType),
                    th.Property("order", th.ObjectType(
                        th.Property("id", th.StringType)
                    )),
                    th.Property("parentTransaction", th.ObjectType(
                        th.Property("id", th.StringType)
                    )),
                    th.Property("paymentIcon", ImageType()),
                    th.Property("paymentId", th.StringType),
                    th.Property("processedAt", th.DateTimeType),
                    th.Property("receiptJson", th.StringType),
                    th.Property("settlementCurrency", th.StringType),
                    th.Property("settlementCurrencyRate", th.StringType),
                    th.Property("status", th.StringType),
                    th.Property("test", th.BooleanType),
                    th.Property("totalUnsettledSet", MoneyBagType()),
                    # th.Property("user", th.ObjectType( ->> Needs read_users access scope
                    #     th.Property("id", th.StringType)
                    # )),
                )
            ),
        ),
        th.Property("unpaid", th.BooleanType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("sourceIdentifier", th.StringType),
        th.Property("sourceName", th.StringType),
        th.Property("retailLocation", LocationType()),
        th.Property("lineItems", th.ArrayType(LineItemNodeType())),
        th.Property("metafields", th.ArrayType(th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("key", th.StringType),
            th.Property("namespace", th.StringType),
            th.Property("value", th.StringType),
            th.Property("type", th.StringType),
        ))),
        th.Property("tags", th.ArrayType(th.StringType))
    ).to_dict()

    bulk_process_fields = {"LineItem": "lineItems", "Metafield": "metafields"}

    def has_next_page_line_items(self, record):
        return record.get("lineItems", {}).get("pageInfo", {}).get("hasNextPage", False)

    def parse_response(self, response):
        records = super().parse_response(response)
        # iterate through lines pages
        decorated_request = self.request_decorator(self._request)
        for record in records:
            if self.config.get("bulk", False):
                yield record
                continue

            # paginate through lineItems for non bulk requests
            context = {"order_id": record["id"].split("/")[-1]}
            line_items_edges = record.get("lineItems", {}).get("edges", [])
            has_next_page = self.has_next_page_line_items(record)
            order_node = record
            while has_next_page:
                self.logger.info(f"Fetching additional line items for order {context['order_id']}")
                self.after_line_item = "\"" + order_node.get("lineItems", {}).get("edges", [])[-1].get("cursor") + "\""
                prepared_request = self.prepare_request(
                    context, next_page_token=None
                )
                resp = decorated_request(prepared_request, context)
                orders = resp.json().get('data', {}).get('orders',{}).get('edges',[])
                if len(orders) > 1:
                    self.logger.warning(f"More than one order with same id. id={context['order_id']} and orders={orders}")
                order_node = orders[0].get('node')
                line_items_edges.extend(order_node.get("lineItems", {}).get("edges", []))
                has_next_page = self.has_next_page_line_items(order_node)
            record["lineItems"] = [edge.get("node") for edge in line_items_edges]

            is_customer_id_selected = 'customerId' in self.selected_properties
            customer = record.get('customer')
            if is_customer_id_selected and isinstance(customer, dict):
                record['customerId'] = customer.get('id')

            self.after_line_item = None
            if record.get(self.replication_key):
                self.last_replication_key = max(self.last_replication_key, record.get(self.replication_key)) if self.last_replication_key else record.get(self.replication_key)
            elif self.last_replication_key:
                record[self.replication_key] = self.last_replication_key
            else:
                raise Exception(f"No replication key in this record and no replication key could be set for it. id={record['id']}. record={record}")
            yield record

    @property
    def after_line_item(self):
        return self._after_line_item

    @after_line_item.setter
    def after_line_item(self, value):
        self._after_line_item = value
        # Clear the cached query when after_line_item changes
        self._clear_cache()

    def _clear_cache(self):
        # Clear the cache of the query
        if 'query' in self.__dict__:
            del self.__dict__['query']

    def get_url_params_line_items(self, context, next_page_token):
        """Return a dictionary of values to be used in URL parameterization."""
        params = {
            "first": 1,
            "filter": f"id:{context['order_id']}"
        }
        return params

    def prepare_request_payload(self, context, next_page_token):
        """Prepare the data payload for the GraphQL API request."""
        if context and "order_id" in context:
            params = self.get_url_params_line_items(context, next_page_token)
        else:
            params = self.get_url_params(context, next_page_token)
        query = self.query.lstrip()

        if 'customerId' in query:
            query = query.replace("customerId", "customer { id }")

        request_data = {
            "query": (" ".join([line.strip() for line in query.splitlines()])),
            "variables": params,
        }
        #self.logger.info(f"Attempting request with variables {params} and query: {request_data['query']}")
        return request_data
    
    def get_child_context(self, record, context) -> dict:
        refunds = [r["id"] for r in record.get("refunds", [])]
        fulfillments = [f["id"] for f in record.get("fulfillments", [])]
        return {"refunds": refunds, "fulfillments": fulfillments}
    
    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_context_bulk = {key: [] for key in self.child_context_keys}
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    # add id to child_context_bulk ids
                    for key, value in child_context.items():                        
                        child_context_bulk[key].extend(child_context[key]) if value else None
                
                if any(len(v) >= self.child_size for v in child_context_bulk.values()):
                    self._sync_children(child_context_bulk)
                    child_context_bulk = {key: [] for key in self.child_context_keys}

                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            # process remaining child context if len < 1000
            if any(v != [] for v in child_context_bulk.values()):
                self._sync_children(child_context_bulk)
            #----
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()
    
    def _sync_children(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            # sync child stream if it is selected and has ids to fetch in child_context
            if (child_stream.selected or child_stream.has_selected_descendents) and child_context.get(child_stream.context_key):
                child_stream.sync(context=child_context)


class FulfillmentsStream(GqlChildStream):
    """Define orders stream."""

    name = "orders_fulfillments"
    primary_keys = ["id"]
    query_name = "fulfillment"
    parent_stream_type = OrdersStream
    context_key = "fulfillments"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("order", th.ObjectType(
            th.Property("id", th.StringType)
        )),
        th.Property("inTransitAt", th.StringType),
        th.Property("legacyResourceId", th.StringType),
        th.Property("location", LocationType()),
        th.Property("name", th.StringType),
        th.Property("requiresShipping", th.BooleanType),
        th.Property("service", th.ObjectType(
            th.Property("id", th.StringType)
        )),
        th.Property("status", th.StringType),
        th.Property("totalQuantity", th.IntegerType),
        th.Property("trackingInfo", th.ArrayType(
            th.ObjectType(
                th.Property("company", th.StringType),
                th.Property("number", th.StringType),
                th.Property("url", th.StringType),
            ))
        ),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()
    

class RefundsStream(GqlChildStream):
    """Define orders stream."""

    name = "orders_refunds"
    primary_keys = ["id"]
    query_name = "refund"
    parent_stream_type = OrdersStream
    context_key = "refunds"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("order", th.ObjectType(
            th.Property("id", th.StringType)
        )),
        th.Property("createdAt", th.DateTimeType),
        th.Property(
            "duties",
            th.ArrayType(th.ObjectType(
                th.Property("amountSet", MoneyBagType()),
            )),
        ),
        th.Property("legacyResourceId", th.StringType),
        th.Property("note", th.StringType),
        th.Property(
            "refundLineItems",
            th.ArrayType(th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("lineItem", th.ObjectType(
                    th.Property("id", th.StringType),
                )),
                th.Property("quantity", th.IntegerType),
                th.Property("restockType", th.StringType),
                th.Property("priceSet", MoneyBagType()),
                th.Property("subtotalSet", MoneyBagType()),
            )),
        ),
        th.Property("totalRefundedSet", MoneyBagType()),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class ShopStream(shopifyGqlStream):
    """Define shop stream."""

    name = "shop"
    primary_keys = ["id"]
    query_name = "shop"
    replication_key = None
    is_list = False

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("contactEmail", th.StringType),
        th.Property(
            "countriesInShippingZones",
            th.ObjectType(
                th.Property("countryCodes", th.ArrayType(th.StringType)),
            ),
            th.Property("includeRestOfWorld", th.BooleanType),
        ),
        th.Property("currencyCode", th.StringType),
        th.Property(
            "currencyFormats",
            th.ObjectType(
                th.Property("moneyFormat", th.StringType),
                th.Property("moneyInEmailsFormat", th.StringType),
                th.Property("moneyWithCurrencyFormat", th.StringType),
                th.Property("moneyWithCurrencyInEmailsFormat", th.StringType),
            ),
        ),
        th.Property("customerAccounts", th.StringType),
        th.Property("description", th.StringType),
        th.Property(
            "domains",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("host", th.StringType),
                    th.Property("sslEnabled", th.BooleanType),
                    th.Property("url", th.StringType),
                )
            ),
        ),
        th.Property("email", th.StringType),
        th.Property("enabledPresentmentCurrencies", th.ArrayType(th.StringType)),
        th.Property(
            "features",
            th.ObjectType(
                th.Property("branding", th.StringType),
                th.Property("captcha", th.BooleanType),
                th.Property("captchaExternalDomains", th.BooleanType),
                th.Property("dynamicRemarketing", th.BooleanType),
                th.Property("giftCards", th.BooleanType),
                th.Property("harmonizedSystemCode", th.BooleanType),
                th.Property("internationalDomains", th.BooleanType),
                th.Property("internationalPriceOverrides", th.BooleanType),
                th.Property("internationalPriceRules", th.BooleanType),
                th.Property("reports", th.BooleanType),
                th.Property("sellsSubscriptions", th.BooleanType),
                th.Property("showMetrics", th.BooleanType),
            ),
        ),
        th.Property(
            "fulfillmentServices",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("callbackUrl", th.StringType),
                    th.Property("fulfillmentOrdersOptIn", th.BooleanType),
                    th.Property("handle", th.StringType),
                    th.Property("inventoryManagement", th.BooleanType),
                    th.Property("location", LocationType()),
                    th.Property("serviceName", th.StringType),
                    th.Property("type", th.StringType),
                )
            ),
        ),
        th.Property("ianaTimezone", th.StringType),
        th.Property(
            "limitedPendingOrderCount",
            th.ObjectType(
                th.Property("atMax", th.BooleanType),
                th.Property("count", th.IntegerType),
            ),
        ),
        th.Property("myshopifyDomain", th.StringType),
        th.Property("name", th.StringType),
        th.Property(
            "navigationSettings",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("url", th.StringType),
                )
            ),
        ),
        th.Property("orderNumberFormatPrefix", th.StringType),
        th.Property("orderNumberFormatSuffix", th.StringType),
        th.Property(
            "navigationSettings",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("title", th.StringType),
                    th.Property("url", th.StringType),
                )
            ),
        ),
        th.Property(
            "primaryDomain",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("host", th.StringType),
                th.Property("sslEnabled", th.BooleanType),
                th.Property("url", th.StringType),
            ),
        ),
        th.Property("taxesIncluded", th.BooleanType),
        th.Property("taxShipping", th.BooleanType),
        th.Property("timezoneAbbreviation", th.StringType),
        th.Property("timezoneOffset", th.StringType),
        th.Property("timezoneOffsetMinutes", th.IntegerType),
        th.Property("unitSystem", th.StringType),
        th.Property("url", th.StringType),
        th.Property("weightUnit", th.StringType),
    ).to_dict()


class InventoryItemsStream(DynamicStream):
    """Define Intentory Items stream."""

    name = "inventory_items"
    primary_keys = ["id"]
    query_name = "inventoryItems"
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("countryCodeOfOrigin", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("duplicateSkuCount", th.IntegerType),
        th.Property("harmonizedSystemCode", th.StringType),
        th.Property("inventoryHistoryUrl", th.StringType),
        th.Property("legacyResourceId", th.StringType),
        th.Property("locationsCount", CountType()),
        th.Property("provinceCodeOfOrigin", th.StringType),
        th.Property("requiresShipping", th.BooleanType),
        th.Property("sku", th.StringType),
        th.Property("tracked", th.BooleanType),
        th.Property(
            "trackedEditable",
            th.ObjectType(
                th.Property("locked", th.BooleanType),
                th.Property("reason", th.StringType),
            ),
        ),
        th.Property(
            "unitCost",
            th.ObjectType(
                th.Property("amount", th.StringType),
                th.Property("currencyCode", th.StringType),
            ),
        ),
        th.Property("updatedAt", th.DateTimeType),
        th.Property(
            "variant",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("availableForSale", th.BooleanType),
                th.Property("barcode", th.StringType),
                th.Property("compareAtPrice", th.StringType),
                th.Property("createdAt", th.DateTimeType),
                th.Property("defaultCursor", th.StringType),
                th.Property("displayName", th.StringType),
                th.Property("inventoryPolicy", th.StringType),
                th.Property(
                    "image",
                    th.ObjectType(
                        th.Property("id", th.StringType),
                        th.Property("altText", th.StringType),
                    ),
                ),
                th.Property("inventoryQuantity", th.IntegerType),
                th.Property("legacyResourceId", th.StringType),
                th.Property("position", th.IntegerType),
                th.Property("price", th.StringType),
                th.Property(
                    "selectedOptions",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name", th.StringType),
                            th.Property("value", th.StringType),
                        )
                    ),
                ),
                th.Property("sku", th.StringType),
                th.Property("taxCode", th.StringType),
                th.Property("taxable", th.BooleanType),
                th.Property("title", th.StringType),
                th.Property("updatedAt", th.DateTimeType),
            ),
        ),
    ).to_dict()


class CollectionsStream(DynamicStream):
    """Define collections stream."""

    name = "collections"
    primary_keys = ["id"]
    query_name = "collections"
    replication_key = "updatedAt"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("description", th.StringType),
        th.Property("descriptionHtml", th.StringType),
        th.Property("handle", th.StringType),
        th.Property(
            "image",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("altText", th.StringType)
            ),
        ),
        th.Property("legacyResourceId", th.StringType),
        th.Property("productsCount", CountType()),
        th.Property("sortOrder", th.StringType),
        th.Property("templateSuffix", th.StringType),
        th.Property("title", th.StringType),
        th.Property("updatedAt", th.DateTimeType),
    ).to_dict()


class CustomersStream(DynamicStream):
    """Define collections stream."""

    name = "customers"
    primary_keys = ["id"]
    query_name = "customers"
    replication_key = "updatedAt"
    sort_key = "UPDATED_AT"
    sort_key_type = "CustomerSortKeys"

    """
        Access denied for companyContactProfiles field.
        Required access: `read_customers` access scope or `read_companies` access scope.
        Also: The API client must be installed on a Shopify Plus store.
    """
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("lastName", th.StringType),
        th.Property("email", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("numberOfOrders", th.StringType),
        th.Property("amountSpent", MoneyV2Type()),
        # th.Property("companyContactProfiles", th.ArrayType(CompanyContactType())), # -> For some clients, we see "NO ACCESS" error,added complete error above.
        th.Property("multipassIdentifier", th.StringType),
        th.Property("note", th.StringType),
        th.Property("verifiedEmail", th.BooleanType),
        th.Property("validEmailAddress", th.BooleanType),
        th.Property("tags", th.CustomType({"type": ["array", "string"]})),
        th.Property("lifetimeDuration", th.StringType),
        th.Property("legacyResourceId", th.StringType),
        th.Property("taxExempt", th.BooleanType),
        th.Property("defaultAddress", MailingAddressType()),
        th.Property("lastOrder", LastOrderType()),
        th.Property("addresses", th.ArrayType(MailingAddressType())),
        th.Property("image", ImageType()),
        th.Property("canDelete", th.BooleanType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("emailMarketingConsent", CustomerEmailMarketingConsentStateType()),
        th.Property("smsMarketingConsent", SmsMarketingConsentType()),
        th.Property("mergeable", MergeableType()),
    ).to_dict()


class LocationsStream(shopifyRestStream):
    """Define collections stream."""

    path = "locations.json"
    name = "locations"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.locations.[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("city", th.StringType),
        th.Property("zip", th.StringType),
        th.Property("province", th.StringType),
        th.Property("country", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("country_code", th.StringType),
        th.Property("country_name", th.StringType),
        th.Property("province_code", th.StringType),
        th.Property("legacy", th.BooleanType),
        th.Property("active", th.BooleanType),
        th.Property("admin_graphql_api_id", th.StringType),
        th.Property("localized_country_name", th.StringType),
        th.Property("localized_province_name", th.StringType),
    ).to_dict()

    def _configured_location_ids(self) -> Set[str]:
        location_ids = self.config.get("location_ids") or []
        return {str(location_id) for location_id in location_ids if str(location_id)}

    def post_process(self, row: dict, context: Optional[dict] = None):
        """Filter locations before child inventory streams are traversed."""
        location_ids = self._configured_location_ids()
        if location_ids and str(row.get("id")) not in location_ids:
            return None
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "location_id": record["id"],
        }


class InventoryLevelRestStream(shopifyRestStream):
    """Define collections stream."""

    path = "inventory_levels.json"
    name = "inventory_level_rest"
    primary_keys = ["id"]
    records_jsonpath = "$.inventory_levels.[*]"

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        if next_page_token:
            return params  # Shopify REST: page_info replaces other filters
        if context is None:
            raise ValueError("Inventory levels require a location context")
        params["location_ids"] = context["location_id"]
        if self.config.get("inventory_item_ids"):
            item_ids = self.config.get("inventory_item_ids")
            if isinstance(item_ids, list):
                item_ids = ",".join(item_ids)
            params.update({"inventory_item_ids": item_ids})
        return params

    parent_stream_type = LocationsStream

    schema = th.PropertiesList(
        th.Property("admin_graphql_api_id", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "inventory_level_id": record["admin_graphql_api_id"],
        }


class InventoryLevelGqlStream(shopifyGqlStream):
    """Define collections stream."""

    name = "inventory_level_gql"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = InventoryLevelRestStream
    query_name = "inventoryLevel"
    is_list = False
    # Incoming and available fields are deprecated in inventory_level.
    additional_arguments = {
        "quantities": '(names: ["available", "incoming"])'
    }

    def single_object_params(self, context=None):
        if context is None:
            raise ValueError("Inventory levels require a parent context")
        return {"id": context["inventory_level_id"]}

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("quantities", th.ArrayType(
            th.ObjectType(
                th.Property("name", th.StringType),
                th.Property("quantity", th.IntegerType),
            )
        )),
        th.Property("item", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("sku", th.StringType),
        )),
        th.Property("location", th.ObjectType(
            th.Property("id", th.StringType),
            th.Property("name", th.StringType),
        )),
    ).to_dict()


class PriceRulesStream(shopifyRestStream):
    """Define collections stream."""

    name = "price_rules"
    primary_keys = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.price_rules.[*]"
    path = "price_rules.json"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("value_type", th.StringType),
        th.Property("value", th.StringType),
        th.Property("customer_selection", th.StringType),
        th.Property("target_type", th.StringType),
        th.Property("target_selection", th.StringType),
        th.Property("allocation_method", th.StringType),
        th.Property("allocation_limit", th.IntegerType),
        th.Property("once_per_customer", th.BooleanType),
        th.Property("usage_limit", th.IntegerType),
        th.Property("starts_at", th.DateTimeType),
        th.Property("ends_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("entitled_product_ids", th.ArrayType(th.IntegerType)),
        th.Property("entitled_variant_ids", th.ArrayType(th.IntegerType)),
        th.Property("entitled_collection_ids", th.ArrayType(th.IntegerType)),
        th.Property("entitled_country_ids", th.ArrayType(th.IntegerType)),
        th.Property("prerequisite_product_ids", th.ArrayType(th.IntegerType)),
        th.Property("prerequisite_variant_ids", th.ArrayType(th.IntegerType)),
        th.Property("prerequisite_collection_ids", th.ArrayType(th.IntegerType)),
        th.Property("prerequisite_saved_search_ids", th.ArrayType(th.IntegerType)),
        th.Property("customer_segment_prerequisite_ids", th.ArrayType(th.IntegerType)),
        th.Property("prerequisite_customer_ids", th.ArrayType(th.IntegerType)),
        th.Property("prerequisite_subtotal_range", th.StringType),
        th.Property("prerequisite_quantity_range", th.ObjectType(
            th.Property("greater_than_or_equal_to", th.IntegerType)
        )),
        th.Property("prerequisite_shipping_price_range", th.ObjectType(
            th.Property("less_than_or_equal_to", th.IntegerType)
        )),
        th.Property("prerequisite_to_entitlement_quantity_ratio", th.ObjectType(
            th.Property("prerequisite_quantity", th.StringType),
            th.Property("entitled_quantity", th.IntegerType),
        )),
        th.Property("prerequisite_to_entitlement_purchase", th.ObjectType(
            th.Property("prerequisite_amount", th.StringType),
        )),
        th.Property("prerequisite_subtotal_range", th.ObjectType(
            th.Property("greater_than_or_equal_to", th.StringType),
        )),
        th.Property("title", th.StringType),
        th.Property("_sdc_shop_myshopify_domain", th.StringType),
        th.Property("_sdc_shop_id", th.IntegerType),
        th.Property("_sdc_shop_name", th.StringType),
        th.Property("admin_graphql_api_id", th.StringType),
    ).to_dict()


class EventProductsStream(shopifyRestStream):
    """Define collections stream."""

    name = "event_products"
    primary_keys = ["id"]
    replication_key = "created_at"
    records_jsonpath = "$.events.[*]"
    path = "events.json"
    limit = 100

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("subject_id", th.IntegerType),
        th.Property("created_at", th.DateTimeType),
        th.Property("subject_type", th.StringType),
        th.Property("verb", th.StringType),
        th.Property("arguments", th.ArrayType(th.CustomType({"type": ["number", "string", "null", "object"]}))),
        th.Property("body", th.StringType),
        th.Property("message", th.StringType),
        th.Property("author", th.StringType),
        th.Property("description", th.StringType),
        th.Property("path", th.StringType),
    ).to_dict()


class MarketingEventsStream(shopifyRestStream):
    """Define collections stream."""

    name = "marketing_events"
    primary_keys = ["id"]
    path = "marketing_events.json"
    records_jsonpath= "$.marketing_events.[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("event_type", th.StringType),
        th.Property("remote_id", th.StringType),
        th.Property("started_at", th.DateTimeType),
        th.Property("ended_at", th.DateTimeType),
        th.Property("scheduled_to_end_at", th.DateTimeType),
        th.Property("budget", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("manage_url", th.StringType),
        th.Property("preview_url", th.StringType),
        th.Property("utm_campaign", th.StringType),
        th.Property("utm_source", th.StringType),
        th.Property("utm_medium", th.StringType),
        th.Property("budget_type", th.StringType),
        th.Property("description", th.StringType),
        th.Property("marketing_channel", th.StringType),
        th.Property("paid", th.BooleanType),
        th.Property("referring_domain", th.StringType),
        th.Property("breadcrumb_id", th.IntegerType),
        th.Property("marketing_activity_id", th.IntegerType),
        th.Property("admin_graphql_api_id", th.StringType),
        th.Property("marketed_resources", th.ArrayType(
            th.ObjectType(
                th.Property("type", th.StringType),
                th.Property("id", th.IntegerType),
            )
        )),
    ).to_dict()


class EventDestroyedProductsStream(EventProductsStream):
    """Define collections stream."""

    name = "event_destroyed_products"
    add_params = {"verb": "destroy"}


class CustomerVisitStream(shopifyGqlStream, metaclass=abc.ABCMeta):
    """Define base class for CustomerVisit stream"""

    primary_keys = ["id", "updatedAt"]
    query_name = "orders"
    replication_key = "updatedAt"
    is_timestamp_replication_key = True
    # JSONPath to compile over the result of filter_response().
    json_path = "$.edges[*].node"

    @property
    def page_size(self):
        return 100

    @property
    def schema(self):
        return th.PropertiesList(
            th.Property("id", th.StringType),
            th.Property("updatedAt", th.DateTimeType),
            th.Property("customerJourneySummary", th.ObjectType(
                th.Property(self.visit_type, CustomerVisitType())
            ))
        ).to_dict()

    def filter_response(self, response_json: dict) -> dict:
        order_edges = (
            ((response_json.get("data") or {}).get("orders") or {}).get(
                "edges", []
            )
        )
        return {
            "edges": [
                edge
                for edge in order_edges
                if edge.get("node", {}).get(
                    "customerJourneySummary", {}
                ).get(self.visit_type)
            ]
        }


class CustomerFirstVisitStream(CustomerVisitStream):
    """Define Customer Visit stream"""

    name = "customer_first_visit"

    @property
    def visit_type(self) -> str:
        return "firstVisit"


class CustomerLastVisitsStream(CustomerVisitStream):
    """Define Customer Visit stream"""

    name = "customer_last_visit"

    @property
    def visit_type(self) -> str:
        return "lastVisit"


class CustomerJourneySummaryStream(shopifyGqlStream):
    """Define base class for CustomerVisit stream"""

    name = "customer_journey_summary"
    primary_keys = ["id", "updatedAt"]
    query_name = "orders"
    replication_key = "updatedAt"
    is_timestamp_replication_key = True

    @property
    def page_size(self):
        return 100

    schema = th.PropertiesList(
            th.Property("id", th.StringType),
            th.Property("updatedAt", th.DateTimeType),
            th.Property("customerJourneySummary", th.ObjectType(
                th.Property("customerOrderIndex", th.IntegerType),
                th.Property("daysToConversion", th.IntegerType),
                th.Property("firstVisit", CustomerVisitType()),
                th.Property("lastVisit", CustomerVisitType()),
                th.Property("momentsCount", CountType()),
                th.Property("ready", th.BooleanType),
            ))
    ).to_dict()


class PayoutsStream(shopifyGqlStream):
    """Define base class for CustomerVisit stream"""

    name = "payouts"
    primary_keys = ["id", "issuedAt"]
    query_name = "payouts"
    replication_key = "issuedAt"
    json_path = "$.data.shopifyPaymentsAccount.payouts.edges[*].node"

    @property
    def page_size(self):
        return 100

    @cached_property
    def max_requests(self):
        return 1

    schema = th.PropertiesList(
            th.Property("id", th.StringType),
            th.Property("issuedAt", th.DateTimeType),
            th.Property("legacyResourceId", th.StringType),
            th.Property("net", MoneyV2Type()),
            th.Property("status", th.StringType),
            th.Property("summary", th.ObjectType(
                th.Property("adjustmentsFee", MoneyV2Type()),
                th.Property("adjustmentsGross", MoneyV2Type()),
                th.Property("chargesFee", MoneyV2Type()),
                th.Property("chargesGross", MoneyV2Type()),

                # TODO: specified in the docs but doesn't work
                # th.Property("advanceFees", MoneyV2Type()),
                # th.Property("advanceGross", MoneyV2Type()),
                # th.Property("refundsFee", MoneyV2Type()),
                # th.Property("refundsGross", MoneyV2Type()),
                th.Property("reservedFundsFee", MoneyV2Type()),
                th.Property("reservedFundsGross", MoneyV2Type()),
                th.Property("retriedPayoutsFee", MoneyV2Type()),
                th.Property("retriedPayoutsGross", MoneyV2Type()),
            )),
            th.Property("transactionType", th.StringType),
            
            # TODO: specified in the docs but doesn't work
            # th.Property("businessEntity", th.ObjectType(
            #     th.Property("id", th.StringType ),
            #     th.Property("companyName", th.StringType),
            #     th.Property("displayName", th.StringType),
            #     th.Property("primary", th.BooleanType),
            #     th.Property("address", MailingAddressType()),
            #     th.Property("shopifyPaymentsAccount", th.ObjectType(
            #         th.Property("id", th.StringType),
            #         th.Property("accountOpenerName", th.StringType),
            #         th.Property("activated", th.BooleanType),
            #         th.Property("balance", MoneyV2Type()),
            #         th.Property("defaultCurrency", th.StringType),
            #         th.Property("onboardable", th.BooleanType),
                    
            #     )),
            # )),
            th.Property("shopifyPaymentsAccountId", th.StringType),
    ).to_dict()
    
    @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        base_query = """
                query tapShopify($first: Int, $after: String, $filter: String) {
                    shopifyPaymentsAccount {
                        id
                        payouts(first: $first, after: $after, query: $filter) {
                            edges {
                                cursor
                                node {
                                    __selected_fields__
                                }
                            },
                            pageInfo {
                                hasNextPage
                            }
                        }
                    }
                }
            """
        gql_selected_fields = self.gql_selected_fields.replace("\nshopifyPaymentsAccountId", "")
        query = base_query.replace("__selected_fields__", gql_selected_fields)
        return query
    
    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        """Return token identifying next page or None if all records have been read."""
        response_json = response.json()
        has_next_json_path = f"$.data.shopifyPaymentsAccount.{self.query_name}.pageInfo.hasNextPage"
        has_next = next(extract_jsonpath(has_next_json_path, response_json))
        if has_next:
            cursor_json_path = f"$.data.shopifyPaymentsAccount.{self.query_name}.edges[-1].cursor"
            all_matches = extract_jsonpath(cursor_json_path, response_json)
            return next(all_matches, None)
        
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = dict()
        params["first"] = self.page_size
        if next_page_token:
            params["after"] = next_page_token
            
        if self.replication_key:
            start_date = self.start_date or self.get_starting_timestamp(context)
            
            if start_date:
                start_date = to_shopify_utc(start_date)
                params["filter"] = f"issued_at:>'{start_date}'"

        return params
    
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return a list of records."""
        response_json = response.json()
        
        errors = response_json.get("errors")
        if errors is not None:
            raise Exception(errors)

        account_id = response_json.get("data").get("shopifyPaymentsAccount").get("id")
        for record in extract_jsonpath(self.json_path, response_json):
            record["shopifyPaymentsAccountId"] = account_id
            yield record
