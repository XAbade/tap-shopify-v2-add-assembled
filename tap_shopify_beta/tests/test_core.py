"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import json

import requests
from hotglue_singer_sdk.testing import (  # type: ignore[import-not-found]
    get_standard_tap_tests,
)

from tap_shopify_beta.streams import (
    ComposedProductsStream,
    LocationsStream,
    OrdersStream,
)
from tap_shopify_beta.tap import TapshopifyBeta

SAMPLE_CONFIG = {
    "shop": "dummy-shop",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%d"
    ),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapshopifyBeta,
        config=SAMPLE_CONFIG
    )
    # Exclude the connection test: unit tests must not call Shopify.
    excluded_test_names = {"_test_stream_connections"}
    for test in tests:
        if test.__name__ in excluded_test_names:
            continue
        test()


def test_composed_products_stream_flattens_and_paginates_bundle_components():
    stream = ComposedProductsStream(tap=TapshopifyBeta(config=SAMPLE_CONFIG))

    assert "productVariantComponents(first: 30)" in str(stream.query)
    assert stream.get_url_params(None, "next") == {
        "first": 1,
        "filter": "requires_components:true",
        "after": "next",
    }

    payload = {
        "data": {
            "productVariants": {
                "edges": [{
                    "cursor": "cursor-1",
                    "node": {
                        "id": (
                            "gid://shopify/ProductVariant/bundle"
                        ),
                        "updatedAt": "2026-01-01T00:00:00Z",
                        "productVariantComponents": {
                            "edges": [{
                                "node": {
                                    "id": (
                                        "gid://shopify/"
                                        "ProductVariantComponent/relation"
                                    ),
                                    "quantity": 2,
                                    "productVariant": {
                                        "id": (
                                            "gid://shopify/"
                                            "ProductVariant/part"
                                        )
                                    },
                                }
                            }]
                        },
                    },
                }],
                "pageInfo": {"hasNextPage": True},
            }
        }
    }
    filtered = stream.filter_response(payload)
    assert filtered["data"]["productVariants"]["edges"] == [{
        "node": {
            "id": "gid://shopify/ProductVariantComponent/relation",
            "composedProductId": "gid://shopify/ProductVariant/bundle",
            "partProductId": "gid://shopify/ProductVariant/part",
            "partQuantity": 2,
            "updatedAt": "2026-01-01T00:00:00Z",
        }
    }]

    response = requests.Response()
    response._content = json.dumps({
        "data": {"productVariants": {
            "edges": [{"cursor": "cursor-1"}],
            "pageInfo": {"hasNextPage": True},
        }}
    }).encode()
    assert stream.get_next_page_token(response, None) == "cursor-1"

    try:
        stream.get_next_page_token(response, "cursor-1")
    except RuntimeError:
        pass
    else:
        assert False, "Repeated pagination cursor must fail"


def test_orders_schema_exposes_pos_source_and_retail_location():
    properties = OrdersStream.schema["properties"]

    assert properties["sourceIdentifier"]["type"] == ["string", "null"]
    assert properties["sourceName"]["type"] == ["string", "null"]
    assert properties["retailLocation"]["type"] == ["object", "null"]
    assert properties["retailLocation"]["properties"]["id"]["type"] == ["string", "null"]
    assert properties["retailLocation"]["properties"]["name"]["type"] == ["string", "null"]


def test_order_line_variant_schema_exposes_legacy_resource_id():
    line_items = OrdersStream.schema["properties"]["lineItems"]["items"]["properties"]
    variant = line_items["variant"]["properties"]

    assert variant["id"]["type"] == ["string", "null"]
    assert variant["legacyResourceId"]["type"] == ["string", "null"]

    stream = OrdersStream(tap=TapshopifyBeta(config=SAMPLE_CONFIG))
    query = stream.get_field_query("variant", variant)
    assert "id" in query
    assert "legacyResourceId" in query


def test_config_schema_exposes_optional_location_ids_string_array():
    location_ids = TapshopifyBeta.config_jsonschema["properties"]["location_ids"]

    assert location_ids["type"] == ["array", "null"]
    assert location_ids["items"]["type"] == ["string"]
    assert "location_ids" not in TapshopifyBeta.config_jsonschema.get("required", [])


def _locations_stream(location_ids=None):
    config = dict(SAMPLE_CONFIG)
    if location_ids is not None:
        config["location_ids"] = location_ids
    return LocationsStream(tap=TapshopifyBeta(config=config))


def test_locations_post_process_keeps_matching_string_location_id():
    stream = _locations_stream(["123"])
    record = {"id": 123, "name": "POS"}

    assert stream.post_process(record) == record


def test_locations_post_process_filters_non_matching_location_id():
    stream = _locations_stream(["123"])

    assert stream.post_process({"id": 456, "name": "Other"}) is None


def test_locations_post_process_preserves_all_locations_when_unset_or_empty():
    record = {"id": 456, "name": "Other"}

    stream = _locations_stream()
    assert stream.post_process(record) == record

    stream = _locations_stream([])
    assert stream.post_process(record) == record
