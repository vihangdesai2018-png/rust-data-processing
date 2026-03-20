"""Smoke tests: extension import and trivial APIs."""

from __future__ import annotations

import rust_data_processing as rdp


def test_version_and_extension_version() -> None:
    assert rdp.__version__
    assert rdp.extension_version()


def test_dataset_roundtrip_rows() -> None:
    schema = [
        {"name": "a", "data_type": "int64"},
        {"name": "b", "data_type": "utf8"},
    ]
    rows = [[1, "x"], [2, "y"]]
    ds = rdp.DataSet(schema, rows)
    assert ds.row_count() == 2
    assert ds.column_names() == ["a", "b"]
    assert ds.to_rows() == rows
