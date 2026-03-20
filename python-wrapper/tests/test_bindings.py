"""Coverage for processing, SQL, and high-level JSON helpers."""

from __future__ import annotations

import pytest

import rust_data_processing as rdp


def test_processing_reduce_and_filter() -> None:
    schema = [
        {"name": "id", "data_type": "int64"},
        {"name": "active", "data_type": "bool"},
        {"name": "score", "data_type": "float64"},
    ]
    rows = [
        [1, True, 10.0],
        [2, False, 20.0],
        [3, True, None],
    ]
    ds = rdp.DataSet(schema, rows)
    s = rdp.processing_reduce(ds, "score", "sum")
    assert s == 30.0
    kept = rdp.processing_filter(ds, lambda r: r[1] == True)  # noqa: E712
    assert kept.row_count() == 2


def test_transform_apply_dict() -> None:
    schema_in = [
        {"name": "id", "data_type": "int64"},
        {"name": "score", "data_type": "int64"},
    ]
    rows = [[1, 10], [2, None]]
    ds = rdp.DataSet(schema_in, rows)
    spec = {
        "output_schema": {
            "fields": [
                {"name": "id", "data_type": "Int64"},
                {"name": "score_f", "data_type": "Float64"},
            ]
        },
        "steps": [
            {"Rename": {"pairs": [["score", "score_f"]]}},
            {
                "Cast": {
                    "column": "score_f",
                    "to": "Float64",
                    "mode": "lossy",
                }
            },
            {"FillNull": {"column": "score_f", "value": {"Float64": 0.0}}},
        ],
    }
    out = rdp.transform_apply(ds, spec)
    assert out.column_names() == ["id", "score_f"]
    assert out.to_rows() == [[1, 10.0], [2, 0.0]]


def test_profile_validate_outliers_helpers() -> None:
    schema = [{"name": "x", "data_type": "float64"}]
    rows = [[1.0], [None], [3.0]]
    ds = rdp.DataSet(schema, rows)
    prof = rdp.profile_dataset(ds, {"head_rows": 2, "quantiles": [0.5]})
    assert prof["row_count"] == 2
    assert "columns" in prof

    vschema = [{"name": "email", "data_type": "utf8"}]
    vrows = [[None]]
    vds = rdp.DataSet(vschema, vrows)
    rep = rdp.validate_dataset(
        vds,
        {
            "checks": [
                {"kind": "not_null", "column": "email", "severity": "error"},
            ]
        },
    )
    assert rep["summary"]["failed_checks"] >= 1

    oschema = [{"name": "x", "data_type": "float64"}]
    orows = [[1.0]] * 4 + [[1000.0]]
    ods = rdp.DataSet(oschema, orows)
    ore = rdp.detect_outliers(ods, "x", {"kind": "iqr", "k": 1.5})
    assert ore["outlier_count"] >= 1


def test_dataframe_pipeline_collect() -> None:
    schema = [
        {"name": "g", "data_type": "utf8"},
        {"name": "v", "data_type": "int64"},
    ]
    rows = [["a", 1], ["a", 2], ["b", 3]]
    ds = rdp.DataSet(schema, rows)
    lf = rdp.DataFrame.from_dataset(ds)
    out = lf.group_by(
        ["g"],
        [{"type": "sum", "column": "v", "alias": "s"}],
    ).collect()
    assert out.row_count() == 2


def test_execution_engine_reduce_metrics() -> None:
    schema = [{"name": "n", "data_type": "int64"}]
    rows = [[1], [2], [3]]
    ds = rdp.DataSet(schema, rows)
    eng = rdp.ExecutionEngine({"chunk_size": 2})
    r = eng.reduce(ds, "n", "sum")
    assert r == 6
    snap = eng.metrics_snapshot()
    assert "rows_processed" in snap


def test_execution_filter_map_parallel_and_events() -> None:
    kinds: list[str] = []

    def on_ev(e: dict) -> None:
        kinds.append(e["kind"])

    eng = rdp.ExecutionEngine({"chunk_size": 3, "num_threads": 2}, on_execution_event=on_ev)
    schema = [{"name": "i", "data_type": "int64"}]
    rows = [[j] for j in range(10)]
    ds = rdp.DataSet(schema, rows)
    filtered = eng.filter_parallel(ds, lambda r: r[0] % 2 == 0)
    assert filtered.row_count() == 5
    mapped = eng.map_parallel(ds, lambda r: [r[0] * 10])
    assert mapped.to_rows() == [[j * 10] for j in range(10)]
    assert "chunk_started" in kinds


def test_ingestion_observer_missing_file() -> None:
    failures: list[tuple[str, str]] = []

    def on_failure(_ctx, severity: str, message: str) -> None:
        failures.append((severity, message))

    schema = [{"name": "x", "data_type": "int64"}]
    with pytest.raises((OSError, ValueError)):
        rdp.ingest_from_path(
            "__missing_rdp_file__.csv",
            schema,
            {
                "format": "csv",
                "observer": {"on_failure": on_failure},
                "alert_at_or_above": "critical",
            },
        )
    assert failures


def test_ingest_from_db_stub_without_connectorx_feature() -> None:
    with pytest.raises(ValueError, match="db_connectorx|db ingestion is disabled|invalid"):
        rdp.ingest_from_db_infer("not-a-valid-connectorx-url", "SELECT 1")


def test_cdc_boundary_types() -> None:
    from rust_data_processing.cdc import (
        CdcEvent,
        CdcOp,
        RowImage,
        SourceMeta,
        TableRef,
    )

    ev = CdcEvent(
        meta=SourceMeta(source="db", checkpoint=None),
        table=TableRef.with_schema("public", "users"),
        op=CdcOp.INSERT,
        after=RowImage.new([("id", 1), ("name", "ada")]),
    )
    assert ev.op == CdcOp.INSERT
    assert ev.table.schema == "public"
