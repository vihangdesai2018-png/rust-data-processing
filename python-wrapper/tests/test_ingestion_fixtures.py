"""Ingestion parity with `tests/csv_ingestion.rs` and `tests/json_ingestion.rs` (fixture files)."""

from __future__ import annotations

import pytest

import rust_data_processing as rdp

from tests.conftest import fixture_path


def test_ingest_csv_people_happy_path() -> None:
    schema = [
        {"name": "id", "data_type": "int64"},
        {"name": "name", "data_type": "utf8"},
        {"name": "score", "data_type": "float64"},
        {"name": "active", "data_type": "bool"},
    ]
    ds = rdp.ingest_from_path(fixture_path("people.csv"), schema, {"format": "csv"})
    assert ds.row_count() == 2
    assert ds.to_rows()[0] == [1, "Ada", 98.5, True]


def test_ingest_json_people_nested_happy_path() -> None:
    schema = [
        {"name": "id", "data_type": "int64"},
        {"name": "user.name", "data_type": "utf8"},
        {"name": "score", "data_type": "float64"},
        {"name": "active", "data_type": "bool"},
    ]
    ds = rdp.ingest_from_path(fixture_path("people.json"), schema, {"format": "json"})
    assert ds.row_count() == 2
    rows = ds.to_rows()
    assert rows[0][0] == 1
    assert rows[0][1] == "Ada"
    assert rows[1][1] == "Grace"


def test_ingest_with_inferred_schema_round_trip() -> None:
    ds, schema = rdp.ingest_with_inferred_schema(
        fixture_path("people.csv"),
        {"format": "csv"},
    )
    assert ds.row_count() == 2
    names = [f["name"] for f in schema]
    assert "id" in names and "name" in names
