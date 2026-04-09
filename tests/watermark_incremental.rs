//! Incremental / high-water filtering on file ingest (`P2-E1-S2a`, `P2-E1-S2c`) and option plumbing for DB (`P2-E1-S2b`).

use std::path::PathBuf;

use rust_data_processing::ingestion::{IngestionOptions, ingest_from_path};
use rust_data_processing::types::{DataType, Field, Schema, Value};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn events_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("ts", DataType::Int64),
    ])
}

#[test]
fn watermark_file_happy_path_keeps_rows_strictly_above() {
    let path = fixture("watermark_events.csv");
    let schema = events_schema();
    let opts = IngestionOptions {
        watermark_column: Some("ts".to_string()),
        watermark_exclusive_above: Some(Value::Int64(100)),
        ..Default::default()
    };
    let ds = ingest_from_path(&path, &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);
    let ids: Vec<i64> = ds
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int64(i) => i,
            _ => panic!("expected id int"),
        })
        .collect();
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn watermark_file_empty_when_all_at_or_below_high_water() {
    let path = fixture("watermark_events.csv");
    let schema = events_schema();
    let opts = IngestionOptions {
        watermark_column: Some("ts".to_string()),
        watermark_exclusive_above: Some(Value::Int64(200)),
        ..Default::default()
    };
    let ds = ingest_from_path(&path, &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 0);
}

#[test]
fn watermark_json_matches_csv_semantics() {
    let path = fixture("watermark_events.json");
    let schema = events_schema();
    let opts = IngestionOptions {
        watermark_column: Some("ts".to_string()),
        watermark_exclusive_above: Some(Value::Int64(100)),
        ..Default::default()
    };
    let ds = ingest_from_path(&path, &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);
    let ids: Vec<i64> = ds
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Int64(i) => i,
            _ => panic!("expected id int"),
        })
        .collect();
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn watermark_rejects_column_without_value() {
    let path = fixture("watermark_events.csv");
    let schema = events_schema();
    let opts = IngestionOptions {
        watermark_column: Some("ts".to_string()),
        watermark_exclusive_above: None,
        ..Default::default()
    };
    let err = ingest_from_path(&path, &schema, &opts).unwrap_err();
    assert!(err.to_string().contains("watermark_column"));
}

#[test]
fn db_stub_accepts_options_without_panic() {
    use rust_data_processing::ingestion::{ingest_from_db, ingest_from_db_infer};
    let schema = events_schema();
    let opts = IngestionOptions::default();
    let e = ingest_from_db("not a url", "SELECT 1", &schema, &opts).unwrap_err();
    assert!(e.to_string().contains("db ingestion is disabled") || e.to_string().contains("invalid"));

    let e2 = ingest_from_db_infer("not a url", "SELECT 1", &opts).unwrap_err();
    assert!(e2.to_string().contains("db ingestion is disabled") || e2.to_string().contains("invalid"));
}
