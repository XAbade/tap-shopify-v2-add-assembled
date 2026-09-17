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


def make_stream(monkeypatch, state=None, location_ids=(1, 2, 3), **config):
    tap = TapshopifyBeta(
        config={"shop": "fixture", "api_key": "fixture", "start_date": START, **config},
        state=state,
    )
    mock_pages(monkeypatch, tap.streams["locations"], [
        ([{"id": location} for location in location_ids], None),
    ], "locations")
    return tap, tap.streams["inventory_level_rest"]


def record(item, updated_at, location=1):
    return {
        "inventory_item_id": item,
        "location_id": location,
        "available": None if item == 2 else 5,
        "updated_at": updated_at,
        "admin_graphql_api_id": f"gid://shopify/InventoryLevel/{location}?inventory_item_id={item}",
    }


def mock_pages(monkeypatch, stream, pages, key="inventory_levels"):
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
        response._content = json.dumps({key: rows}).encode()
        if cursor:
            response.headers["link"] = (
                f'<https://fixture.myshopify.com/admin/api/2026-07/'
                f'{key}.json?page_info={cursor}>; rel="next"'
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


def test_schema_and_child_chain(monkeypatch):
    tap, stream = make_stream(monkeypatch)
    assert stream.replication_method == "INCREMENTAL"
    assert stream.primary_keys == ["inventory_item_id", "location_id"]
    assert stream.state_partitioning_keys == []
    assert stream.partitions is None
    assert stream.parent_stream_type is None
    assert not stream.is_sorted
    properties = stream.schema["properties"]
    assert {"inventory_item_id", "location_id", "available", "updated_at", "admin_graphql_api_id"} <= properties.keys()
    assert "null" in properties["available"]["type"]
    assert properties["updated_at"]["format"] == "date-time"
    row = record(1, NEW)
    child_context = stream.get_child_context(row, None)
    child = tap.streams["inventory_level_gql"]
    assert isinstance(child, InventoryLevelGqlStream)
    assert child.parent_stream_type is type(stream)
    assert child.single_object_params(child_context) == {"id": row["admin_graphql_api_id"]}


def test_all_locations_pagination_ties_and_global_resume(monkeypatch):
    tap, stream = make_stream(monkeypatch, inventory_item_ids=["1", "2", "3"])
    # Inventory deliberately ignores the locations stream's configured ETL filter.
    tap.streams["locations"]._config["location_ids"] = ["1"]
    location_params = mock_pages(monkeypatch, tap.streams["locations"], [
        ([{"id": 1}], "locations-next"), ([{"id": 2}, {"id": 3}], None),
    ], "locations")
    rows = [record(1, NEW), record(2, NEW, 2), record(3, OLD, 3)]
    params = mock_pages(monkeypatch, stream, [(rows[:1], "cursor"), (rows[1:], None)])
    emitted, children, _ = capture(monkeypatch, stream)
    stream.sync()
    assert len(location_params) == 2
    assert params == [
        {"limit": ["250"], "location_ids": ["1,2,3"], "inventory_item_ids": ["1,2,3"]},
        {"limit": ["250"], "page_info": ["cursor"]},
    ]
    assert emitted == rows
    assert children == [{"inventory_level_id": row["admin_graphql_api_id"]} for row in rows]
    assert stream.stream_state == {
        "replication_key": "updated_at", "replication_key_value": NEW,
        "location_ids": ["1", "2", "3"],
    }

    _, resumed = make_stream(monkeypatch, state=deepcopy(tap.state), location_ids=(3, 1, 2))
    emitted, _, _ = capture(monkeypatch, resumed)
    params = mock_pages(monkeypatch, resumed, [([record(2, NEW, 2)], None)])
    resumed.sync()
    assert params[0]["updated_at_min"] == [NEW]  # Inclusive boundary replays ties.
    assert params[0]["location_ids"] == ["1,2,3"]
    assert len(emitted) == 1


def test_more_than_50_locations_share_one_bookmark(monkeypatch):
    _, stream = make_stream(monkeypatch, location_ids=range(1, 52))
    capture(monkeypatch, stream)
    params = mock_pages(monkeypatch, stream, [
        ([record(1, NEW)], "next"), ([record(2, OLD, 50)], None),
        ([record(3, OLD, 9)], None),
    ])
    stream.sync()
    location_ids = sorted(map(str, range(1, 52)))
    assert params[0]["location_ids"] == [",".join(location_ids[:50])]
    assert params[1] == {"limit": ["250"], "page_info": ["next"]}
    assert params[2]["location_ids"] == [location_ids[-1]]
    assert all("updated_at_min" not in p for p in params)
    assert stream.stream_state == {
        "replication_key": "updated_at", "replication_key_value": NEW,
        "location_ids": location_ids,
    }


@pytest.mark.parametrize("has_bookmark", [False, True])
@pytest.mark.parametrize("failure", ["page", "batch"])
def test_interruption_keeps_global_bookmark(monkeypatch, has_bookmark, failure):
    location_ids = sorted(map(str, range(1, 52)))
    state = {"bookmarks": {"inventory_level_rest": {
        "replication_key": "updated_at", "replication_key_value": OLD,
        "location_ids": location_ids,
    }}} if has_bookmark else None
    _, stream = make_stream(monkeypatch, state=state, location_ids=range(1, 52))
    _, _, states = capture(monkeypatch, stream)
    stream.STATE_MSG_FREQUENCY = 1
    mock_pages(monkeypatch, stream, [
        ([record(1, NEW)], "next" if failure == "page" else None),
        RuntimeError("interrupted"),
    ])
    with pytest.raises(RuntimeError, match="interrupted"):
        stream.sync()
    assert stream.stream_state.get("replication_key_value") == (OLD if has_bookmark else None)
    _, resumed = make_stream(monkeypatch, state=states[-1], location_ids=range(1, 52))
    emitted, _, _ = capture(monkeypatch, resumed)
    params = mock_pages(monkeypatch, resumed, [
        ([record(1, NEW)], None), ([record(2, OLD, 9)], None),
    ])
    resumed.sync()
    assert all(p.get("updated_at_min") == ([OLD] if has_bookmark else None) for p in params)
    assert len(emitted) == 2
    assert resumed.stream_state == {
        "replication_key": "updated_at", "replication_key_value": NEW,
        "location_ids": location_ids,
    }


def test_old_partition_state_replays_safely_once(monkeypatch):
    state = {"bookmarks": {"inventory_level_rest": {"partitions": [
        {"context": {"location_id": 1}},
        {"context": {"location_id": 2}, "replication_key": "updated_at", "replication_key_value": NEW},
    ]}}}
    _, stream = make_stream(monkeypatch, state=state)
    capture(monkeypatch, stream)
    params = mock_pages(monkeypatch, stream, [([record(1, OLD)], None)])
    stream.sync()
    assert len(params) == 1
    assert params[0]["location_ids"] == ["1,2,3"]
    assert "updated_at_min" not in params[0]
    assert stream.stream_state == {
        "replication_key": "updated_at", "replication_key_value": OLD,
        "location_ids": ["1", "2", "3"],
    }


@pytest.mark.parametrize("location_ids", [(), (1, 2, 3)])
def test_empty_results_preserve_global_bookmark(monkeypatch, location_ids):
    state = {"bookmarks": {"inventory_level_rest": {
        "replication_key": "updated_at", "replication_key_value": OLD,
    }}}
    _, stream = make_stream(monkeypatch, state=state, location_ids=location_ids)
    capture(monkeypatch, stream)
    params = mock_pages(monkeypatch, stream, [([], None)])
    stream.sync()
    assert len(params) == (1 if location_ids else 0)
    assert stream.stream_state == {
        "replication_key": "updated_at", "replication_key_value": OLD,
        "location_ids": list(map(str, location_ids)),
    }


@pytest.mark.parametrize("previous", [["1", "2"], ["1", "2", "3", "4"], None])
def test_location_changes_refresh_old_stock_and_retry_safely(monkeypatch, previous):
    bookmark = {"replication_key": "updated_at", "replication_key_value": NEW}
    if previous is not None:
        bookmark["location_ids"] = previous
    state = {"bookmarks": {"inventory_level_rest": bookmark}}
    _, stream = make_stream(monkeypatch, state=state, end_date=NEW)
    _, _, states = capture(monkeypatch, stream)
    stream.STATE_MSG_FREQUENCY = 1
    params = mock_pages(monkeypatch, stream, [
        ([record(1, OLD)], "next"), RuntimeError("interrupted"),
    ])
    with pytest.raises(RuntimeError, match="interrupted"):
        stream.sync()
    assert "updated_at_min" not in params[0]
    assert "updated_at_max" not in params[0]
    assert stream.stream_state.get("location_ids") == previous
    assert stream.stream_state["replication_key_value"] == NEW

    _, resumed = make_stream(monkeypatch, state=states[-1])
    emitted, _, _ = capture(monkeypatch, resumed)
    params = mock_pages(monkeypatch, resumed, [([record(1, OLD)], None)])
    resumed.sync()
    assert "updated_at_min" not in params[0]
    assert emitted == [record(1, OLD)]
    assert resumed.stream_state["location_ids"] == ["1", "2", "3"]
    assert "partitions" not in resumed.stream_state
