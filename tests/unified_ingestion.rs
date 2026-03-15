use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use rust_data_processing::ingestion::{ingest_from_path, IngestionFormat, IngestionOptions};
#[cfg(feature = "excel_test_writer")]
use rust_data_processing::ingestion::ExcelSheetSelection;
use rust_data_processing::types::{DataType, Field, Schema, Value};

fn tmp_file(ext: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("rust-data-processing-unified-{nanos}.{ext}"))
}

fn people_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("active", DataType::Bool),
    ])
}

fn people_schema_json_nested() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("user.name", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("active", DataType::Bool),
    ])
}

fn write_people_parquet(path: &PathBuf) {
    let schema_str = r#"
    message schema {
      REQUIRED INT64 id;
      REQUIRED BINARY name (UTF8);
      REQUIRED DOUBLE score;
      REQUIRED BOOLEAN active;
    }
    "#;

    let schema = Arc::new(parse_message_type(schema_str).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = File::create(path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    let mut rg = writer.next_row_group().unwrap();
    let mut col_idx: usize = 0;
    while let Some(mut col) = rg.next_column().unwrap() {
        match col.untyped() {
            ColumnWriter::Int64ColumnWriter(w) => {
                w.write_batch(&[1_i64, 2_i64], None, None).unwrap();
            }
            ColumnWriter::ByteArrayColumnWriter(w) => {
                assert_eq!(col_idx, 1);
                let v1 = ByteArray::from("Ada");
                let v2 = ByteArray::from("Grace");
                w.write_batch(&[v1, v2], None, None).unwrap();
            }
            ColumnWriter::DoubleColumnWriter(w) => {
                w.write_batch(&[98.5_f64, 87.25_f64], None, None).unwrap();
            }
            ColumnWriter::BoolColumnWriter(w) => {
                w.write_batch(&[true, false], None, None).unwrap();
            }
            _ => panic!("unexpected column writer in test"),
        }
        col.close().unwrap();
        col_idx += 1;
    }
    rg.close().unwrap();
    writer.close().unwrap();
}

#[test]
fn unified_ingest_csv_auto_by_extension() {
    let schema = people_schema();
    let opts = IngestionOptions::default();
    let ds = ingest_from_path("tests/fixtures/people.csv", &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[0][0], Value::Int64(1));
}

#[test]
fn unified_ingest_csv_explicit_format() {
    let schema = people_schema();
    let opts = IngestionOptions {
        format: Some(IngestionFormat::Csv),
        ..Default::default()
    };
    let ds = ingest_from_path("tests/fixtures/people.csv", &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);
}

#[test]
fn unified_ingest_json_explicit_format_errors_with_flat_schema() {
    let schema = people_schema();
    let opts = IngestionOptions {
        format: Some(IngestionFormat::Json),
        ..Default::default()
    };
    // This JSON has nested "user.name" in earlier tests; keep this schema flat and assert error.
    let err = ingest_from_path("tests/fixtures/people.json", &schema, &opts).unwrap_err();
    assert!(err.to_string().contains("missing required field 'name'"));
}

#[test]
fn unified_ingest_json_auto_by_extension_happy_path_nested_schema() {
    let schema = people_schema_json_nested();
    let opts = IngestionOptions::default(); // inferred from .json
    let ds = ingest_from_path("tests/fixtures/people.json", &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[0][1], Value::Utf8("Ada".to_string()));
}

#[test]
fn unified_ingest_json_explicit_format_happy_path_nested_schema() {
    let schema = people_schema_json_nested();
    let opts = IngestionOptions {
        format: Some(IngestionFormat::Json),
        ..Default::default()
    };
    let ds = ingest_from_path("tests/fixtures/people.json", &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);
}

#[test]
fn unified_ingest_parquet_explicit_format() {
    let schema = people_schema();
    let path = tmp_file("parquet");
    write_people_parquet(&path);

    let opts = IngestionOptions {
        format: Some(IngestionFormat::Parquet),
        ..Default::default()
    };
    let ds = ingest_from_path(&path, &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[1][1], Value::Utf8("Grace".to_string()));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn unified_ingest_parquet_auto_by_extension() {
    let schema = people_schema();
    let path = tmp_file("parquet");
    write_people_parquet(&path);

    let ds = ingest_from_path(&path, &schema, &IngestionOptions::default()).unwrap();
    assert_eq!(ds.row_count(), 2);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unified_ingest_switch_formats_same_schema() {
    let schema = people_schema();

    // CSV
    let ds_csv = ingest_from_path("tests/fixtures/people.csv", &schema, &IngestionOptions::default()).unwrap();
    assert_eq!(ds_csv.row_count(), 2);

    // Parquet
    let path = tmp_file("parquet");
    write_people_parquet(&path);
    let opts_parquet = IngestionOptions {
        format: Some(IngestionFormat::Parquet),
        ..Default::default()
    };
    let ds_parquet = ingest_from_path(&path, &schema, &opts_parquet).unwrap();
    assert_eq!(ds_parquet.row_count(), 2);

    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "excel_test_writer")]
#[test]
fn unified_ingest_excel_all_sheets_explicit_format() {
    use rust_xlsxwriter::Workbook;

    let schema = people_schema();
    let path = tmp_file("xlsx");

    let mut wb = Workbook::new();
    let ws1 = wb.add_worksheet();
    ws1.set_name("Sheet1").unwrap();
    ws1.write_string(0, 0, "id").unwrap();
    ws1.write_string(0, 1, "name").unwrap();
    ws1.write_string(0, 2, "score").unwrap();
    ws1.write_string(0, 3, "active").unwrap();
    ws1.write_number(1, 0, 1).unwrap();
    ws1.write_string(1, 1, "Ada").unwrap();
    ws1.write_number(1, 2, 98.5).unwrap();
    ws1.write_boolean(1, 3, true).unwrap();

    let ws2 = wb.add_worksheet();
    ws2.set_name("Second").unwrap();
    ws2.write_string(0, 0, "id").unwrap();
    ws2.write_string(0, 1, "name").unwrap();
    ws2.write_string(0, 2, "score").unwrap();
    ws2.write_string(0, 3, "active").unwrap();
    ws2.write_number(1, 0, 2).unwrap();
    ws2.write_string(1, 1, "Grace").unwrap();
    ws2.write_number(1, 2, 87.25).unwrap();
    ws2.write_boolean(1, 3, false).unwrap();

    wb.save(&path).unwrap();

    let opts = IngestionOptions {
        format: Some(IngestionFormat::Excel),
        excel_sheet_selection: ExcelSheetSelection::AllSheets,
        ..Default::default()
    };
    let ds = ingest_from_path(&path, &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);

    let _ = std::fs::remove_file(&path);
}

#[cfg(feature = "excel_test_writer")]
#[test]
fn unified_ingest_excel_all_sheets_auto_by_extension() {
    use rust_xlsxwriter::Workbook;

    let schema = people_schema();
    let path = tmp_file("xlsx");

    let mut wb = Workbook::new();
    let ws1 = wb.add_worksheet();
    ws1.set_name("Sheet1").unwrap();
    ws1.write_string(0, 0, "id").unwrap();
    ws1.write_string(0, 1, "name").unwrap();
    ws1.write_string(0, 2, "score").unwrap();
    ws1.write_string(0, 3, "active").unwrap();
    ws1.write_number(1, 0, 1).unwrap();
    ws1.write_string(1, 1, "Ada").unwrap();
    ws1.write_number(1, 2, 98.5).unwrap();
    ws1.write_boolean(1, 3, true).unwrap();

    let ws2 = wb.add_worksheet();
    ws2.set_name("Second").unwrap();
    ws2.write_string(0, 0, "id").unwrap();
    ws2.write_string(0, 1, "name").unwrap();
    ws2.write_string(0, 2, "score").unwrap();
    ws2.write_string(0, 3, "active").unwrap();
    ws2.write_number(1, 0, 2).unwrap();
    ws2.write_string(1, 1, "Grace").unwrap();
    ws2.write_number(1, 2, 87.25).unwrap();
    ws2.write_boolean(1, 3, false).unwrap();

    wb.save(&path).unwrap();

    let opts = IngestionOptions {
        // auto inferred from .xlsx
        excel_sheet_selection: ExcelSheetSelection::AllSheets,
        ..Default::default()
    };
    let ds = ingest_from_path(&path, &schema, &opts).unwrap();
    assert_eq!(ds.row_count(), 2);

    let _ = std::fs::remove_file(&path);
}
