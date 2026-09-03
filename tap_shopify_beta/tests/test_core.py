"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from hotglue_singer_sdk.testing import get_standard_tap_tests

from tap_shopify_beta.streams import (
    LocationsStream,
    OrdersStream,
    ProductsStream,
    VariantsStream,
)
from tap_shopify_beta.tap import TapshopifyBeta

SAMPLE_CONFIG = {
    "shop": "dummy-shop",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapshopifyBeta,
        config=SAMPLE_CONFIG
    )
    # Exclude SDK connection test: unit suite must not perform live Shopify calls.
    excluded_test_names = {"_test_stream_connections"}
    for test in tests:
        if test.__name__ in excluded_test_names:
            continue
        test()


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


def test_variant_schema_exposes_bundle_components():
    properties = VariantsStream.schema["properties"]

    assert properties["requiresComponents"]["type"] == ["boolean", "null"]

    components = properties["productVariantComponents"]
    assert components["type"] == ["array", "null"]

    component = components["items"]["properties"]
    assert component["id"]["type"] == ["string", "null"]
    assert component["quantity"]["type"] == ["integer", "null"]
    assert component["productVariant"]["properties"]["id"]["type"] == ["string", "null"]
    assert component["productVariant"]["properties"]["sku"]["type"] == ["string", "null"]


def test_variants_stream_treats_components_as_a_connection():
    assert VariantsStream.extra_paginated_fields == {"productVariantComponents": 25}
    assert (
        VariantsStream.bulk_process_fields["ProductVariantComponent"]
        == "productVariantComponents"
    )
    # The default connections must survive the per-stream override.
    assert VariantsStream.default_paginated_fields["metafields"] == 50


def test_products_stream_sorts_by_replication_key():
    assert ProductsStream.sort_key == "UPDATED_AT"
    assert ProductsStream.sort_key_type == "ProductSortKeys"


def test_products_schema_exposes_bundle_parent_indicator():
    properties = ProductsStream.schema["properties"]

    assert properties["hasVariantsThatRequiresComponents"]["type"] == ["boolean", "null"]
