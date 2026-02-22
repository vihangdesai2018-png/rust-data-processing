use rust_data_processing::ingestion::json::{ingest_json_from_path, ingest_json_from_str};
use rust_data_processing::types::{DataType, Field, Schema, Value};

fn people_schema_nested() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("user.name", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("active", DataType::Bool),
    ])
}

#[test]
fn ingest_json_array_from_path_happy_path() {
    let schema = people_schema_nested();
    let ds = ingest_json_from_path("tests/fixtures/people.json", &schema).unwrap();

    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[0][0], Value::Int64(1));
    assert_eq!(ds.rows[0][1], Value::Utf8("Ada".to_string()));
    assert_eq!(ds.rows[1][1], Value::Utf8("Grace".to_string()));
}

#[test]
fn ingest_json_ndjson_happy_path() {
    let schema = people_schema_nested();
    let input = r#"
{"id":1,"user":{"name":"Ada"},"score":98.5,"active":true}
{"id":2,"user":{"name":"Grace"},"score":87.25,"active":false}
"#;
    let ds = ingest_json_from_str(input, &schema).unwrap();
    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[0][1], Value::Utf8("Ada".to_string()));
}

#[test]
fn ingest_json_errors_on_missing_field() {
    let schema = people_schema_nested();
    let input = r#"[{"id":1,"user":{"name":"Ada"},"score":98.5}]"#;
    let err = ingest_json_from_str(input, &schema).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("schema mismatch"));
    assert!(msg.contains("missing required field 'active'"));
}

#[test]
fn ingest_json_errors_on_type_mismatch() {
    let schema = people_schema_nested();
    let input = r#"[{"id":"nope","user":{"name":"Ada"},"score":98.5,"active":true}]"#;
    let err = ingest_json_from_str(input, &schema).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("failed to parse value"));
    assert!(msg.contains("column 'id'"));
}
