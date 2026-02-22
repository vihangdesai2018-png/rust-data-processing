use rust_data_processing::ingestion::csv::{ingest_csv_from_path, ingest_csv_from_reader};
use rust_data_processing::types::{DataType, Field, Schema, Value};

fn people_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("active", DataType::Bool),
    ])
}

#[test]
fn ingest_csv_from_path_happy_path() {
    let schema = people_schema();
    let ds = ingest_csv_from_path("tests/fixtures/people.csv", &schema).unwrap();

    assert_eq!(ds.row_count(), 2);
    assert_eq!(
        ds.rows[0],
        vec![
            Value::Int64(1),
            Value::Utf8("Ada".to_string()),
            Value::Float64(98.5),
            Value::Bool(true),
        ]
    );
}

#[test]
fn ingest_csv_allows_reordered_columns() {
    let schema = people_schema();
    let input = "name,id,active,score\nAda,1,true,98.5\n";
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(input.as_bytes());

    let ds = ingest_csv_from_reader(&mut rdr, &schema).unwrap();
    assert_eq!(ds.row_count(), 1);
    assert_eq!(ds.rows[0][0], Value::Int64(1));
    assert_eq!(ds.rows[0][1], Value::Utf8("Ada".to_string()));
}

#[test]
fn ingest_csv_errors_on_missing_required_column() {
    let schema = people_schema();
    let input = "id,name,score\n1,Ada,98.5\n";
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(input.as_bytes());

    let err = ingest_csv_from_reader(&mut rdr, &schema).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("schema mismatch"));
    assert!(msg.contains("missing required column 'active'"));
}

#[test]
fn ingest_csv_errors_on_type_parse() {
    let schema = people_schema();
    let input = "id,name,score,active\nnot_an_int,Ada,98.5,true\n";
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(input.as_bytes());

    let err = ingest_csv_from_reader(&mut rdr, &schema).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("failed to parse value"));
    assert!(msg.contains("column 'id'"));
}
