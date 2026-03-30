"""Parity helpers — only `rust_data_processing` APIs (no direct Rust/cargo invocations)."""

from __future__ import annotations

import math
from typing import Any, Sequence

import rust_data_processing as rdp


def values_close_mem_polars(mem: Any, pol: Any, eps_abs: float = 1e-6) -> None:
    """Match `deep_tests::assert_value_close_mem_polars` for scalars from reduce."""
    if mem is None and pol is None:
        return
    if type(mem) is int and type(pol) is int:
        assert mem == pol
        return
    if isinstance(mem, bool) or isinstance(pol, bool):
        assert mem == pol, f"mem={mem!r} pol={pol!r}"
        return
    if isinstance(mem, (int, float)) and isinstance(pol, (int, float)):
        a, b = float(mem), float(pol)
        diff = abs(a - b)
        if diff <= eps_abs:
            return
        scale = max(abs(a), abs(b), 1.0)
        rel_tol = max(scale * 1e-9, 8.0 * sys_float_eps())
        assert diff <= rel_tol, f"mem={mem!r} pol={pol!r} diff={diff} rel_tol={rel_tol}"
        return
    assert mem == pol, f"mem={mem!r} pol={pol!r}"


def sys_float_eps() -> float:
    return float(math.ulp(1.0))  # ~2.22e-16


def reduce_parity(ds: rdp.DataSet, column: str, op: str) -> None:
    mem = rdp.processing_reduce(ds, column, op)
    lf = rdp.DataFrame.from_dataset(ds)
    pol = lf.reduce(column, op)
    assert pol is not None, f"polars reduce returned None for {column!r} {op!r}"
    eps = max(1e-6, sys_float_eps())
    values_close_mem_polars(mem, pol, eps)


def feature_wise_parity(ds: rdp.DataSet, columns: Sequence[str], std_kind: str | None) -> None:
    mem = rdp.processing_feature_wise_mean_std(ds, list(columns), std_kind)
    lf = rdp.DataFrame.from_dataset(ds)
    pol = lf.feature_wise_mean_std(list(columns), std_kind)
    by_name_m = {d["column"]: d for d in mem}
    by_name_p = {d["column"]: d for d in pol}
    assert set(by_name_m) == set(by_name_p)
    eps = max(1e-6, sys_float_eps())
    for name in by_name_m:
        values_close_mem_polars(by_name_m[name]["mean"], by_name_p[name]["mean"], eps)
        values_close_mem_polars(by_name_m[name]["std_dev"], by_name_p[name]["std_dev"], eps)


def people_dataset() -> rdp.DataSet:
    """Mirror `tests/sql.rs` `people_dataset`."""
    schema = [
        {"name": "id", "data_type": "int64"},
        {"name": "active", "data_type": "bool"},
        {"name": "score", "data_type": "float64"},
        {"name": "name", "data_type": "utf8"},
        {"name": "grp", "data_type": "utf8"},
    ]
    rows = [
        [1, True, 10.0, "Ada", "A"],
        [2, False, 20.0, "Grace", "A"],
        [3, True, 3.0, "Linus", "B"],
        [4, True, None, "Ken", "B"],
    ]
    return rdp.DataSet(schema, rows)
