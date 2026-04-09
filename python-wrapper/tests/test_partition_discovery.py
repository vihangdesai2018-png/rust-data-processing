"""Hive-style partition discovery — mirrors `tests/partition_discovery.rs`."""

from __future__ import annotations

import os

import pytest

import rust_data_processing as rdp

from tests.conftest import fixture_path


def _norm(p: str) -> str:
    return p.replace("\\", "/")


def test_discover_hive_two_partitions_plus_root_file() -> None:
    root = fixture_path("hive_partitioned")
    files = rdp.discover_hive_partitioned_files(root, None)
    assert len(files) == 3
    by_tail = {_norm(f["path"]).split("hive_partitioned/")[-1]: f for f in files}

    assert by_tail["at_root.csv"]["segments"] == []

    us = by_tail["dt=2024-01-01/region=us/events.csv"]
    assert us["segments"] == [
        {"key": "dt", "value": "2024-01-01"},
        {"key": "region", "value": "us"},
    ]
    eu = by_tail["dt=2024-01-01/region=eu/events.csv"]
    assert eu["segments"][1]["value"] == "eu"


def test_discover_hive_with_glob_pattern() -> None:
    root = fixture_path("hive_partitioned")
    files = rdp.discover_hive_partitioned_files(root, "**/events.csv")
    assert len(files) == 2
    assert all(_norm(f["path"]).endswith("events.csv") for f in files)


def test_discover_skips_non_hive_directories() -> None:
    root = fixture_path("hive_partitioned_skip")
    files = rdp.discover_hive_partitioned_files(root, None)
    assert files == []


def test_discover_rejects_non_directory_root() -> None:
    f = fixture_path("hive_partitioned", "at_root.csv")
    with pytest.raises(ValueError, match="directory|hive"):
        rdp.discover_hive_partitioned_files(f, None)


def test_paths_from_glob_finds_fixture_csvs() -> None:
    base = fixture_path("hive_partitioned")
    pat = _norm(os.path.join(base, "**", "*.csv"))
    paths = rdp.paths_from_glob(pat)
    assert len(paths) >= 3


def test_paths_from_explicit_list_order_and_dedup() -> None:
    root = fixture_path("hive_partitioned")
    a = os.path.join(root, "at_root.csv")
    b = os.path.join(root, "dt=2024-01-01", "region=us", "events.csv")
    paths = rdp.paths_from_explicit_list([a, b, a])
    assert len(paths) == 2
    assert os.path.samefile(paths[0], a)
    assert os.path.samefile(paths[1], b)


def test_paths_from_explicit_list_errors_on_missing() -> None:
    missing = os.path.join(fixture_path("hive_partitioned"), "nope.csv")
    with pytest.raises(ValueError, match="not an existing file|file"):
        rdp.paths_from_explicit_list([missing])


def test_parse_partition_segment() -> None:
    d = rdp.parse_partition_segment("dt=2024-01-01")
    assert d == {"key": "dt", "value": "2024-01-01"}
    assert rdp.parse_partition_segment("nodash") is None
