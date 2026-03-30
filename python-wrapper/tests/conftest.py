"""Pytest configuration: repo-root fixture paths (same files as Rust `tests/fixtures/`)."""

from __future__ import annotations

from pathlib import Path

import pytest

# python-wrapper/tests/conftest.py → repo root is parents[2]
REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES = REPO_ROOT / "tests" / "fixtures"


def fixture_path(*parts: str) -> str:
    return str(FIXTURES.joinpath(*parts))


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "deep: uses tests/fixtures/deep (larger / slower)")
    config.addinivalue_line("markers", "benchmark: pytest-benchmark timing tests")
