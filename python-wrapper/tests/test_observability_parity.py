"""Mirror `tests/ingestion_observability.rs` via Python observer hooks."""

from __future__ import annotations

import pytest

import rust_data_processing as rdp

from tests.conftest import fixture_path


def test_observer_receives_failure_and_alert_on_critical_io_error() -> None:
    failures: list[str] = []
    alerts: list[str] = []

    def on_failure(_ctx: object, severity: str, _message: str) -> None:
        failures.append(severity)

    def on_alert(_ctx: object, severity: str, _message: str) -> None:
        alerts.append(severity)

    schema = [{"name": "id", "data_type": "int64"}]
    with pytest.raises((OSError, ValueError)):
        rdp.ingest_from_path(
            fixture_path("does_not_exist.csv"),
            schema,
            {
                "format": "csv",
                "observer": {"on_failure": on_failure, "on_alert": on_alert},
                "alert_at_or_above": "critical",
            },
        )
    assert failures == ["critical"]
    assert alerts == ["critical"]


def test_observer_receives_failure_without_alert_for_non_critical_error() -> None:
    failures: list[str] = []
    alerts: list[str] = []

    def on_failure(_ctx: object, severity: str, _message: str) -> None:
        failures.append(severity)

    def on_alert(_ctx: object, severity: str, _message: str) -> None:
        alerts.append(severity)

    schema = [{"name": "definitely_missing", "data_type": "utf8"}]
    with pytest.raises(ValueError):
        rdp.ingest_from_path(
            fixture_path("people.csv"),
            schema,
            {
                "format": "csv",
                "observer": {"on_failure": on_failure, "on_alert": on_alert},
                "alert_at_or_above": "critical",
            },
        )
    assert failures == ["error"]
    assert alerts == []
