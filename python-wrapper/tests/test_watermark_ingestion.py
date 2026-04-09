"""Incremental / watermark options on path ingest — mirrors `tests/watermark_incremental.rs`."""

from __future__ import annotations

import pytest

import rust_data_processing as rdp

from tests.conftest import fixture_path


def _events_schema() -> list[dict[str, str]]:
    return [
        {"name": "id", "data_type": "int64"},
        {"name": "ts", "data_type": "int64"},
    ]


def test_watermark_csv_keeps_rows_strictly_above() -> None:
    opts = {
        "watermark_column": "ts",
        "watermark_exclusive_above": 100,
    }
    ds = rdp.ingest_from_path(fixture_path("watermark_events.csv"), _events_schema(), opts)
    assert ds.row_count() == 2
    ids = [row[0] for row in ds.to_rows()]
    assert ids == [2, 4]


def test_watermark_csv_empty_when_all_at_or_below_floor() -> None:
    opts = {
        "watermark_column": "ts",
        "watermark_exclusive_above": 200,
    }
    ds = rdp.ingest_from_path(fixture_path("watermark_events.csv"), _events_schema(), opts)
    assert ds.row_count() == 0


def test_watermark_json_matches_csv_semantics() -> None:
    opts = {
        "watermark_column": "ts",
        "watermark_exclusive_above": 100,
    }
    ds = rdp.ingest_from_path(fixture_path("watermark_events.json"), _events_schema(), opts)
    assert ds.row_count() == 2
    ids = [row[0] for row in ds.to_rows()]
    assert ids == [2, 4]


def test_watermark_rejects_column_without_floor() -> None:
    opts = {"watermark_column": "ts"}
    with pytest.raises(ValueError, match="watermark"):
        rdp.ingest_from_path(fixture_path("watermark_events.csv"), _events_schema(), opts)


def test_ingest_from_db_stub_accepts_watermark_options() -> None:
    """Options dict parses; stub still errors on disabled DB ingest."""
    with pytest.raises(ValueError, match="db_connectorx|db ingestion is disabled|invalid"):
        rdp.ingest_from_db(
            "not-a-valid-connectorx-url",
            "SELECT 1",
            _events_schema(),
            {"watermark_column": "ts", "watermark_exclusive_above": 100},
        )
