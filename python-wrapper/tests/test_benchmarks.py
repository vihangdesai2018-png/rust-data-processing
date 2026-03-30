"""Python-side timing (pytest-benchmark) over `rust_data_processing` — not Criterion, same APIs."""

from __future__ import annotations

import pytest

import rust_data_processing as rdp

pytestmark = pytest.mark.benchmark


def _wide_dataset(n: int = 8000) -> rdp.DataSet:
    schema = [
        {"name": "id", "data_type": "int64"},
        {"name": "active", "data_type": "bool"},
        {"name": "score", "data_type": "float64"},
        {"name": "aux", "data_type": "float64"},
        {"name": "grp", "data_type": "utf8"},
    ]
    rows = []
    for i in range(n):
        rows.append(
            [
                i,
                (i % 3) != 0,
                float(i) * 0.1,
                float(i) * 0.03 + 1.0,
                f"g{i % 8}",
            ],
        )
    return rdp.DataSet(schema, rows)


def test_bench_processing_filter_map_reduce(benchmark) -> None:
    ds = _wide_dataset()

    def run() -> None:
        active_idx, id_idx, score_idx = 1, 0, 2
        filt = rdp.processing_filter(
            ds,
            lambda r: r[active_idx] is True and int(r[id_idx]) % 2 == 0,
        )
        mapped = rdp.processing_map(
            filt,
            lambda r: [
                r[0],
                r[1],
                (float(r[score_idx]) * 1.1) if r[score_idx] is not None else None,
                r[3],
                r[4],
            ],
        )
        rdp.processing_reduce(mapped, "score", "sum")

    benchmark(run)


def test_bench_dataframe_group_by_collect(benchmark) -> None:
    ds = _wide_dataset()
    lf = rdp.DataFrame.from_dataset(ds)

    def run() -> None:
        lf.group_by(
            ["grp"],
            [
                {"type": "mean", "column": "score", "alias": "m"},
                {"type": "sum", "column": "aux", "alias": "s"},
            ],
        ).collect()

    benchmark(run)


def test_bench_execution_engine_filter_parallel(benchmark) -> None:
    ds = _wide_dataset(4000)
    eng = rdp.ExecutionEngine({"chunk_size": 512, "num_threads": 2})

    def run() -> None:
        eng.filter_parallel(ds, lambda r: int(r[0]) % 2 == 0)

    benchmark(run)
