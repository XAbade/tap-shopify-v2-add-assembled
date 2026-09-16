"""shopify-beta tap class."""

from typing import List

from hotglue_singer_sdk import Stream, Tap  # type: ignore[import-not-found]
from hotglue_singer_sdk import typing as th  # type: ignore[import-not-found]

from tap_shopify_beta.streams import (
    CollectionsStream,
    ComposedProductsStream,
    CustomersStream,
    CustomerJourneySummaryStream,
    CustomerFirstVisitStream,
    CustomerLastVisitsStream,
    InventoryItemsStream,
    OrdersStream,
    ProductsStream,
    ShopStream,
    VariantsStream,
    LocationsStream,
    InventoryLevelRestStream,
    InventoryLevelGqlStream,
    PriceRulesStream,
    EventProductsStream,
    EventDestroyedProductsStream,
    MarketingEventsStream,
    FulfillmentsStream,
    RefundsStream,
    PayoutsStream
)

STREAM_TYPES = [
    ProductsStream,
    ComposedProductsStream,
    VariantsStream,
    ShopStream,
    OrdersStream,
    InventoryItemsStream,
    CollectionsStream,
    CustomersStream,
    CustomerJourneySummaryStream,
    CustomerFirstVisitStream,
    CustomerLastVisitsStream,
    LocationsStream,
    InventoryLevelRestStream,
    InventoryLevelGqlStream,
    PriceRulesStream,
    EventProductsStream,
    EventDestroyedProductsStream,
    MarketingEventsStream,
    FulfillmentsStream,
    RefundsStream,
    PayoutsStream
]


class TapshopifyBeta(Tap):
    """shopify-beta tap class."""

    name = "tap-shopify-beta"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=False,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "shop", th.StringType, required=True, description="Shopify string name"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync (inclusive)",
        ),
        th.Property(
            "sync_composed_products",
            th.BooleanType,
            default=True,
            description="Sync composed products; false skips fetching this stream",
        ),
        th.Property(
            "location_ids",
            th.ArrayType(th.StringType),
            required=False,
            description="Optional Shopify location IDs to sync inventory levels for",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapshopifyBeta.cli()
