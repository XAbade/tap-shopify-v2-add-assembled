"""Credential-free regression checks using real SDK pagination and state handling."""

from copy import deepcopy
import json
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from tap_shopify_beta.streams import InventoryLevelGqlStream
from tap_shopify_beta.tap import TapshopifyBeta

START = "2026-07-01T00:00:00Z"
NEW = "2026-07-03T00:00:00Z"
OLD = "2026-07-02T00:00:00Z"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("Regression tests must not access Shopify")

    monkeypatch.setattr(requests.Session, "send", blocked)


def make_stream(state=None, **config):
    tap = TapshopifyBeta(
        config={"shop": "fixture", "api_key": "fixture", "start_date": START, **config},
        state=state,
    )
    return tap, tap.streams["inventory_level_rest"]


def record(item, updated_at, location=1):
    return {
        "inventory_item_id": item,
        "location_id": location,
        "available": None if item == 2 else 5,
        "updated_at": updated_at,
        "admin_graphql_api_id": f"gid://shopify/InventoryLevel/{location}?inventory_item_id={item}",
    }


def mock_pages(monkeypatch, stream, pages):
    pages = iter(pages)
    params = []

    def request(prepared, context):
        params.append(parse_qs(urlparse(prepared.url).query))
        page = next(pages)
        if isinstance(page, Exception):
            raise page
        rows, cursor = page
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({"inventory_levels": rows}).encode()
        if cursor:
            response.headers["link"] = (
                f'<https://fixture.myshopify.com/admin/api/2026-07/'
                f'inventory_levels.json?page_info={cursor}>; rel="next"'
            )
        return response

    monkeypatch.setattr(stream, "_request", request)
    return params


def capture(monkeypatch, stream):
    records, children, states = [], [], []
    monkeypatch.setattr(stream, "_write_record_message", lambda row: records.append(deepcopy(row)))
    monkeypatch.setattr(stream, "_sync_children", lambda ctx: children.append(deepcopy(ctx)))
    monkeypatch.setattr(stream, "_write_state_message", lambda: states.append(deepcopy(stream._tap.state)))
    return records, children, states


def test_schema_and_child_chain():
    tap, stream = make_stream()
    assert stream.replication_method == "INCREMENTAL"
    assert stream.primary_keys == ["inventory_item_id", "location_id"]
    assert stream.state_partitioning_keys == ["location_id"]
    assert not stream.is_sorted
    properties = stream.schema["properties"]
    assert {"inventory_item_id", "location_id", "available", "updated_at", "admin_graphql_api_id"} <= properties.keys()
    assert "null" in properties["available"]["type"]
    assert properties["updated_at"]["format"] == "date-time"
    context = tap.streams["locations"].get_child_context({"id": 1}, None)
    assert context == {"location_id": 1}
    row = record(1, NEW)
    child_context = stream.get_child_context(row, context)
    child = tap.streams["inventory_level_gql"]
    assert isinstance(child, InventoryLevelGqlStream)
    assert child.parent_stream_type is type(stream)
    assert child.single_object_params(child_context) == {"id": row["admin_graphql_api_id"]}


def test_incremental_pagination_ties_out_of_order_and_location_isolation(monkeypatch):
    tap, stream = make_stream(inventory_item_ids=["1", "2", "3"])
    rows = [record(1, NEW), record(2, NEW), record(3, OLD)]
    params = mock_pages(monkeypatch, stream, [(rows[:1], "cursor"), (rows[1:], None)])
    emitted, children, _ = capture(monkeypatch, stream)
    stream.sync({"location_id": 1})
    assert params == [
        {"limit": ["250"], "updated_at_min": [START], "location_ids": ["1"], "inventory_item_ids": ["1,2,3"]},
        {"limit": ["250"], "page_info": ["cursor"]},
    ]
    assert emitted == rows
    assert children == [{"inventory_level_id": row["admin_graphql_api_id"]} for row in rows]
    assert stream.get_context_state({"location_id": 1})["replication_key_value"] == NEW

    params = mock_pages(monkeypatch, stream, [([record(1, OLD, location=2)], None)])
    stream.sync({"location_id": 2})
    assert params[0]["updated_at_min"] == [START]
    assert stream.get_context_state({"location_id": 2})["replication_key_value"] == OLD

    _, resumed = make_stream(state=deepcopy(tap.state))
    capture(monkeypatch, resumed)
    params = mock_pages(monkeypatch, resumed, [([record(2, NEW)], None)])
    resumed.sync({"location_id": 1})
    assert params[0]["updated_at_min"] == [NEW]  # Inclusive boundary replays ties.
    params = mock_pages(monkeypatch, resumed, [([], None)])
    resumed.sync({"location_id": 2})
    assert params[0]["updated_at_min"] == [OLD]
    assert resumed.get_context_state({"location_id": 2})["replication_key_value"] == OLD


@pytest.mark.parametrize("has_bookmark", [False, True])
def test_interrupted_location_restarts_at_committed_bookmark(monkeypatch, has_bookmark):
    tap, stream = make_stream()
    capture(monkeypatch, stream)
    if has_bookmark:
        mock_pages(monkeypatch, stream, [([record(1, OLD)], None)])
        stream.sync({"location_id": 1})
    _, _, states = capture(monkeypatch, stream)
    stream.STATE_MSG_FREQUENCY = 1
    mock_pages(monkeypatch, stream, [([record(1, NEW)], "next"), RuntimeError("interrupted")])
    with pytest.raises(RuntimeError, match="interrupted"):
        stream.sync({"location_id": 1})
    assert stream.get_context_state({"location_id": 1}).get("replication_key_value") == (OLD if has_bookmark else None)
    # Resume from an actually emitted interim state, not just successful final state.
    _, resumed = make_stream(state=states[-1])
    emitted, _, _ = capture(monkeypatch, resumed)
    params = mock_pages(monkeypatch, resumed, [([record(1, NEW), record(2, OLD)], None)])
    resumed.sync({"location_id": 1})
    assert params[0]["updated_at_min"] == [OLD if has_bookmark else START]
    assert len(emitted) == 2
    assert resumed.get_context_state({"location_id": 1})["replication_key_value"] == NEW
