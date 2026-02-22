use std::sync::{Arc, Mutex};

use rust_data_processing::ingestion::{
    ingest_from_path, IngestionFormat, IngestionObserver, IngestionOptions, IngestionSeverity,
};
use rust_data_processing::types::{DataType, Field, Schema};

#[derive(Default)]
struct RecordingObserver {
    failures: Mutex<Vec<IngestionSeverity>>,
    alerts: Mutex<Vec<IngestionSeverity>>,
}

impl IngestionObserver for RecordingObserver {
    fn on_failure(
        &self,
        _ctx: &rust_data_processing::ingestion::IngestionContext,
        severity: IngestionSeverity,
        _error: &rust_data_processing::IngestionError,
    ) {
        self.failures.lock().unwrap().push(severity);
    }

    fn on_alert(
        &self,
        _ctx: &rust_data_processing::ingestion::IngestionContext,
        severity: IngestionSeverity,
        _error: &rust_data_processing::IngestionError,
    ) {
        self.alerts.lock().unwrap().push(severity);
    }
}

fn schema_id_only() -> Schema {
    Schema::new(vec![Field::new("id", DataType::Int64)])
}

fn schema_missing_col() -> Schema {
    Schema::new(vec![Field::new("definitely_missing", DataType::Utf8)])
}

#[test]
fn observer_receives_failure_and_alert_on_critical_io_error() {
    let obs = Arc::new(RecordingObserver::default());
    let opts = IngestionOptions {
        format: Some(IngestionFormat::Csv),
        observer: Some(obs.clone()),
        alert_at_or_above: IngestionSeverity::Critical,
        ..Default::default()
    };

    // Missing file -> Io error -> Critical
    let _ = ingest_from_path("tests/fixtures/does_not_exist.csv", &schema_id_only(), &opts).unwrap_err();

    let failures = obs.failures.lock().unwrap().clone();
    let alerts = obs.alerts.lock().unwrap().clone();
    assert_eq!(failures, vec![IngestionSeverity::Critical]);
    assert_eq!(alerts, vec![IngestionSeverity::Critical]);
}

#[test]
fn observer_receives_failure_without_alert_for_non_critical_error() {
    let obs = Arc::new(RecordingObserver::default());
    let opts = IngestionOptions {
        format: Some(IngestionFormat::Csv),
        observer: Some(obs.clone()),
        alert_at_or_above: IngestionSeverity::Critical,
        ..Default::default()
    };

    // Schema mismatch -> Error severity (not Critical) -> should not alert
    let _ = ingest_from_path("tests/fixtures/people.csv", &schema_missing_col(), &opts).unwrap_err();

    let failures = obs.failures.lock().unwrap().clone();
    assert_eq!(failures, vec![IngestionSeverity::Error]);
    assert!(obs.alerts.lock().unwrap().is_empty());
}

