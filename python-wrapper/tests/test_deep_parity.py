"""Mirror `tests/deep_tests.rs` through the Python wrapper (`@pytest.mark.deep`)."""

from __future__ import annotations

import pytest

import rust_data_processing as rdp

from tests.conftest import fixture_path
from tests.helpers import feature_wise_parity, reduce_parity

pytestmark = pytest.mark.deep

SEATTLE_SCHEMA = [
    {"name": "date", "data_type": "utf8"},
    {"name": "precipitation", "data_type": "float64"},
    {"name": "temp_max", "data_type": "float64"},
    {"name": "temp_min", "data_type": "float64"},
    {"name": "wind", "data_type": "float64"},
    {"name": "weather", "data_type": "utf8"},
]

JOB_RUNS_SCHEMA = [
    {"name": "job_id", "data_type": "int64"},
    {"name": "creator_user_name", "data_type": "utf8"},
    {"name": "created_time", "data_type": "int64"},
    {"name": "settings.name", "data_type": "utf8"},
    {"name": "settings.tags.team", "data_type": "utf8"},
    {"name": "settings.tags.env", "data_type": "utf8"},
    {"name": "cluster.num_workers", "data_type": "int64"},
    {"name": "metrics.duration_ms", "data_type": "float64"},
    {"name": "metrics.success", "data_type": "bool"},
    {"name": "metrics.bytes_written", "data_type": "int64"},
]


def _seattle() -> rdp.DataSet:
    return rdp.ingest_from_path(fixture_path("deep/seattle-weather.csv"), SEATTLE_SCHEMA, {})


def test_deep_csv_seattle_weather_ingests_and_casts() -> None:
    ds = _seattle()
    assert ds.row_count() > 1000
    rows = ds.to_rows()
    assert rows[0][0] == "2012-01-01"
    assert rows[0][5] == "drizzle"
    assert isinstance(rows[1][1], float)


def test_deep_reduce_new_ops_parity_in_memory_vs_polars_on_seattle_weather() -> None:
    ds = _seattle()
    for col in ["precipitation", "temp_max", "temp_min", "wind"]:
        reduce_parity(ds, col, "mean")
        reduce_parity(ds, col, "variance_population")
        reduce_parity(ds, col, "variance_sample")
        reduce_parity(ds, col, "stddev_population")
        reduce_parity(ds, col, "stddev_sample")
        reduce_parity(ds, col, "sum_squares")
        reduce_parity(ds, col, "l2_norm")
        reduce_parity(ds, col, "sum")
        reduce_parity(ds, col, "min")
        reduce_parity(ds, col, "max")
    reduce_parity(ds, "weather", "count_distinct_non_null")
    reduce_parity(ds, "temp_max", "count")
    mean = rdp.processing_reduce(ds, "temp_max", "mean")
    mn = rdp.processing_reduce(ds, "temp_max", "min")
    mx = rdp.processing_reduce(ds, "temp_max", "max")
    assert mn <= mean <= mx
    v = rdp.processing_reduce(ds, "temp_max", "variance_population")
    assert float(v) >= 0.0


def test_deep_feature_wise_mean_std_parity_on_seattle_numeric_columns() -> None:
    ds = _seattle()
    cols = ["precipitation", "temp_max", "temp_min", "wind"]
    feature_wise_parity(ds, cols, "sample")
    feature_wise_parity(ds, cols, "population")


def test_deep_group_by_mean_max_count_distinct_on_seattle_weather() -> None:
    ds = _seattle()
    lf = rdp.DataFrame.from_dataset(ds)
    out = lf.group_by(
        ["weather"],
        [
            {"type": "mean", "column": "temp_max", "alias": "mu_tmax"},
            {"type": "max", "column": "temp_min", "alias": "max_tmin"},
            {"type": "count_rows", "alias": "n_rows"},
            {"type": "count_distinct_non_null", "column": "date", "alias": "n_dates"},
            {"type": "std_dev", "column": "wind", "alias": "sd_wind", "kind": "sample"},
        ],
    ).collect()
    assert out.row_count() >= 5
    assert len(out.column_names()) == 6
    idx = {n: i for i, n in enumerate(out.column_names())}
    total = 0
    for r in out.to_rows():
        v = r[idx["n_rows"]]
        if v is not None:
            total += int(v)
    assert total == ds.row_count()


def test_deep_arg_extrema_and_topk_weather_on_seattle() -> None:
    ds = _seattle()
    i_max, v_max = rdp.processing_arg_max_row(ds, "temp_max")
    i_min, v_min = rdp.processing_arg_min_row(ds, "temp_max")
    assert i_max is not None and i_min is not None
    assert float(v_max) >= float(v_min)
    assert i_max < ds.row_count()
    assert i_min < ds.row_count()
    top = rdp.processing_top_k_by_frequency(ds, "weather", 5)
    assert len(top) == 5
    for a, b in zip(top, top[1:]):
        assert a[1] >= b[1]


def _job_runs() -> rdp.DataSet:
    return rdp.ingest_from_path(fixture_path("deep/job_runs_sample.json"), JOB_RUNS_SCHEMA, {})


def test_deep_reduce_new_ops_parity_on_job_runs_json_fixture() -> None:
    ds = _job_runs()
    assert ds.row_count() == 3
    numeric = [
        "job_id",
        "created_time",
        "cluster.num_workers",
        "metrics.bytes_written",
        "metrics.duration_ms",
    ]
    for col in numeric:
        reduce_parity(ds, col, "mean")
        reduce_parity(ds, col, "sum")
        reduce_parity(ds, col, "min")
        reduce_parity(ds, col, "max")
        reduce_parity(ds, col, "sum_squares")
        reduce_parity(ds, col, "l2_norm")
        reduce_parity(ds, col, "variance_population")
        reduce_parity(ds, col, "variance_sample")
        reduce_parity(ds, col, "stddev_population")
        reduce_parity(ds, col, "stddev_sample")
    for col in [
        "metrics.success",
        "creator_user_name",
        "settings.tags.team",
        "settings.tags.env",
        "settings.name",
    ]:
        reduce_parity(ds, col, "count_distinct_non_null")
    reduce_parity(ds, "job_id", "count")
    assert rdp.processing_reduce(ds, "metrics.bytes_written", "count_distinct_non_null") == 2
    assert rdp.processing_reduce(ds, "cluster.num_workers", "count_distinct_non_null") == 2
    feature_wise_parity(ds, ["job_id", "metrics.duration_ms"], "sample")


def test_deep_json_nested_job_runs_extracts_dot_paths_and_handles_nulls() -> None:
    ds = _job_runs()
    assert ds.row_count() == 3
    rows = ds.to_rows()
    assert rows[0][0] == 12001
    assert rows[0][3] == "daily_ingest_events"
    assert rows[1][5] == "prod"
    assert rows[2][6] is None
    assert rows[2][9] is None
    assert rows[2][8] is False


def test_deep_transform_spec_and_sql_work_on_real_fixture() -> None:
    ds = _seattle()
    spec = {
        "output_schema": {
            "fields": [
                {"name": "date", "data_type": "Utf8"},
                {"name": "wx", "data_type": "Utf8"},
                {"name": "temp_max_x2", "data_type": "Float64"},
            ]
        },
        "steps": [
            {"Rename": {"pairs": [["weather", "wx"]]}},
            {
                "DeriveMulF64": {
                    "name": "temp_max_x2",
                    "source": "temp_max",
                    "factor": 2.0,
                },
            },
            {"Select": {"columns": ["date", "wx", "temp_max_x2"]}},
        ],
    }
    mapped = rdp.transform_apply(ds, spec)
    assert mapped.row_count() == ds.row_count()
    assert isinstance(mapped.to_rows()[0][2], float)
    out = rdp.sql_query_dataset(
        mapped,
        "SELECT date, wx FROM df WHERE wx IS NOT NULL ORDER BY date ASC LIMIT 5",
    )
    assert out.column_names() == ["date", "wx"]
    assert out.row_count() == 5


def test_deep_profiling_head_sampling_is_deterministic() -> None:
    ds = _seattle()
    rep = rdp.profile_dataset(ds, {"sampling": {"head": 100}, "quantiles": [0.5]})
    assert rep["row_count"] == 100
    assert len(rep["columns"]) == len(SEATTLE_SCHEMA)
    date_col = next(c for c in rep["columns"] if c["name"] == "date")
    assert date_col["data_type"] == "utf8"


def test_deep_validation_and_outliers_smoke_on_real_fixture() -> None:
    ds = _seattle()
    rep = rdp.validate_dataset(
        ds,
        {
            "checks": [
                {"kind": "not_null", "column": "date", "severity": "error"},
                {
                    "kind": "regex_match",
                    "column": "date",
                    "pattern": r"^\d{4}-\d{2}-\d{2}$",
                    "severity": "warn",
                    "strict": True,
                },
                {
                    "kind": "range_f64",
                    "column": "wind",
                    "min": 0.0,
                    "max": 100.0,
                    "severity": "warn",
                },
            ],
        },
    )
    assert rep["summary"]["total_checks"] == 3
    out = rdp.detect_outliers(
        ds,
        "temp_max",
        {"kind": "iqr", "k": 1.5},
        {"sampling": {"head": 200}, "max_examples": 5},
    )
    assert out["row_count"] == 200


def _pa_to_rdp_dtype(pa: object, typ: object) -> str | None:
    import pyarrow.types as types

    if types.is_integer(typ):
        return "int64"
    if types.is_floating(typ):
        return "float64"
    if types.is_boolean(typ):
        return "bool"
    if types.is_string(typ) or types.is_large_string(typ):
        return "utf8"
    return None


def test_deep_parquet_apache_fixture_ingests_supported_columns() -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    import pyarrow.compute as pc

    path = fixture_path("deep/rle-dict-snappy-checksum.parquet")
    tbl = pq.read_table(path)
    fields: list[dict[str, str]] = []
    seen: set[str] = set()
    for name in tbl.column_names:
        arr = tbl.column(name)
        rdp_dt = _pa_to_rdp_dtype(pa, arr.type)
        if rdp_dt is not None and name not in seen:
            fields.append({"name": name, "data_type": rdp_dt})
            seen.add(name)
        if len(fields) >= 6:
            break
    assert fields
    ds = rdp.ingest_from_path(path, fields, {"format": "parquet"})
    assert ds.row_count() == tbl.num_rows
    casted = []
    for f in fields:
        col = tbl.column(f["name"])
        tgt = {
            "int64": pa.int64(),  # type: ignore[attr-defined]
            "float64": pa.float64(),  # type: ignore[attr-defined]
            "bool": pa.bool_(),  # type: ignore[attr-defined]
            "utf8": pa.large_string(),  # type: ignore[attr-defined]
        }[f["data_type"]]
        casted.append(pc.cast(col, tgt))
    n = min(10, tbl.num_rows)
    py_rows = ds.to_rows()
    for row_idx in range(n):
        for col_idx, f in enumerate(fields):
            raw = casted[col_idx][row_idx].as_py()
            got = py_rows[row_idx][col_idx]
            if raw is None:
                assert got is None, (row_idx, f["name"])
            elif f["data_type"] == "float64":
                assert isinstance(got, float)
                assert abs(float(raw) - got) <= max(1e-6, abs(got) * 1e-9)
            else:
                assert got == raw, (row_idx, f["name"], got, raw)
    for f in fields:
        dt = f["data_type"]
        name = f["name"]
        if dt in ("int64", "float64"):
            reduce_parity(ds, name, "mean")
            reduce_parity(ds, name, "sum_squares")
            reduce_parity(ds, name, "l2_norm")
            reduce_parity(ds, name, "variance_sample")
        elif dt == "utf8":
            reduce_parity(ds, name, "count_distinct_non_null")
        else:
            reduce_parity(ds, name, "count_distinct_non_null")
    numeric = [f["name"] for f in fields if f["data_type"] in ("int64", "float64")]
    if len(numeric) >= 2:
        feature_wise_parity(ds, numeric, "sample")
