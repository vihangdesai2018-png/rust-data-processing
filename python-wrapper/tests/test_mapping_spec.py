"""Mirror `tests/mapping_spec.rs` using `transform_apply` (JSON TransformSpec)."""

from __future__ import annotations

import rust_data_processing as rdp


def test_mapping_spec_rename_cast_fill_select_applies() -> None:
    schema_in = [
        {"name": "id", "data_type": "int64"},
        {"name": "score", "data_type": "int64"},
        {"name": "name", "data_type": "utf8"},
    ]
    rows = [[1, 10, "Ada"], [2, None, "Grace"]]
    ds = rdp.DataSet(schema_in, rows)
    spec = {
        "output_schema": {
            "fields": [
                {"name": "id", "data_type": "Int64"},
                {"name": "score_i", "data_type": "Float64"},
            ]
        },
        "steps": [
            {"Rename": {"pairs": [["score", "score_i"]]}},
            {
                "Cast": {
                    "column": "score_i",
                    "to": "Float64",
                    "mode": "strict",
                },
            },
            {"FillNull": {"column": "score_i", "value": {"Float64": 0.0}}},
            {"Select": {"columns": ["id", "score_i"]}},
        ],
    }
    out = rdp.transform_apply(ds, spec)
    assert out.column_names() == ["id", "score_i"]
    assert out.to_rows() == [[1, 10.0], [2, 0.0]]


def test_mapping_spec_drop_and_with_literal_work() -> None:
    schema_in = [
        {"name": "id", "data_type": "int64"},
        {"name": "score", "data_type": "int64"},
        {"name": "name", "data_type": "utf8"},
    ]
    rows = [[1, 10, "Ada"], [2, None, "Grace"]]
    ds = rdp.DataSet(schema_in, rows)
    spec = {
        "output_schema": {
            "fields": [
                {"name": "id", "data_type": "Int64"},
                {"name": "score", "data_type": "Int64"},
                {"name": "tag", "data_type": "Utf8"},
            ]
        },
        "steps": [
            {"WithLiteral": {"name": "tag", "value": {"Utf8": "v1"}}},
            {"Drop": {"columns": ["name"]}},
            {"Select": {"columns": ["id", "score", "tag"]}},
        ],
    }
    out = rdp.transform_apply(ds, spec)
    assert out.column_names() == ["id", "score", "tag"]
    assert out.to_rows()[0][2] == "v1"
