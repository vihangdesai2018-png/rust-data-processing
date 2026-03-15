#![cfg(feature = "excel_test_writer")]

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rust_data_processing::ingestion::excel::{ingest_excel_from_path, ingest_excel_workbook_from_path};
use rust_data_processing::types::{DataType, Field, Schema, Value};

fn tmp_file(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("rust-data-processing-{name}-{nanos}.xlsx"))
}

fn people_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("active", DataType::Bool),
    ])
}

fn write_people_xlsx(path: &PathBuf, include_active: bool, id_as_string: bool) {
    use rust_xlsxwriter::Workbook;

    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    ws.set_name("Sheet1").unwrap();

    // header
    ws.write_string(0, 0, "id").unwrap();
    ws.write_string(0, 1, "name").unwrap();
    ws.write_string(0, 2, "score").unwrap();
    if include_active {
        ws.write_string(0, 3, "active").unwrap();
    }

    // row 1
    if id_as_string {
        ws.write_string(1, 0, "1").unwrap();
    } else {
        ws.write_number(1, 0, 1).unwrap();
    }
    ws.write_string(1, 1, "Ada").unwrap();
    ws.write_number(1, 2, 98.5).unwrap();
    if include_active {
        ws.write_boolean(1, 3, true).unwrap();
    }

    // row 2
    if id_as_string {
        ws.write_string(2, 0, "2").unwrap();
    } else {
        ws.write_number(2, 0, 2).unwrap();
    }
    ws.write_string(2, 1, "Grace").unwrap();
    ws.write_number(2, 2, 87.25).unwrap();
    if include_active {
        ws.write_boolean(2, 3, false).unwrap();
    }

    wb.save(path).unwrap();
}

fn write_people_multi_sheet_xlsx(path: &PathBuf) {
    use rust_xlsxwriter::Workbook;

    let mut wb = Workbook::new();

    // Sheet1: 2 rows
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
    ws1.write_number(2, 0, 2).unwrap();
    ws1.write_string(2, 1, "Grace").unwrap();
    ws1.write_number(2, 2, 87.25).unwrap();
    ws1.write_boolean(2, 3, false).unwrap();

    // Sheet2: 1 row
    let ws2 = wb.add_worksheet();
    ws2.set_name("Second").unwrap();
    ws2.write_string(0, 0, "id").unwrap();
    ws2.write_string(0, 1, "name").unwrap();
    ws2.write_string(0, 2, "score").unwrap();
    ws2.write_string(0, 3, "active").unwrap();
    ws2.write_number(1, 0, 3).unwrap();
    ws2.write_string(1, 1, "Linus").unwrap();
    ws2.write_number(1, 2, 77.0).unwrap();
    ws2.write_boolean(1, 3, true).unwrap();

    wb.save(path).unwrap();
}

#[test]
fn ingest_excel_happy_path() {
    let schema = people_schema();
    let path = tmp_file("people");
    write_people_xlsx(&path, true, false);

    let ds = ingest_excel_from_path(&path, None, &schema).unwrap();
    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[0][0], Value::Int64(1));
    assert_eq!(ds.rows[0][1], Value::Utf8("Ada".to_string()));
    assert_eq!(ds.rows[1][3], Value::Bool(false));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn ingest_excel_errors_on_missing_required_column() {
    let schema = people_schema();
    let path = tmp_file("missing");
    write_people_xlsx(&path, false, false);

    let err = ingest_excel_from_path(&path, None, &schema).unwrap_err();
    assert!(err.to_string().contains("missing required column 'active'"));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ingest_excel_allows_string_numbers() {
    let schema = people_schema();
    let path = tmp_file("string-nums");
    write_people_xlsx(&path, true, true);

    let ds = ingest_excel_from_path(&path, None, &schema).unwrap();
    assert_eq!(ds.rows[0][0], Value::Int64(1));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ingest_excel_multi_tab_all_sheets_concatenates_rows() {
    let schema = people_schema();
    let path = tmp_file("multi");
    write_people_multi_sheet_xlsx(&path);

    let ds = ingest_excel_workbook_from_path(&path, None, &schema).unwrap();
    assert_eq!(ds.row_count(), 3);
    assert_eq!(ds.rows[2][0], Value::Int64(3));
    assert_eq!(ds.rows[2][1], Value::Utf8("Linus".to_string()));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn ingest_excel_multi_tab_selected_sheet_only() {
    let schema = people_schema();
    let path = tmp_file("multi-selected");
    write_people_multi_sheet_xlsx(&path);

    let sheets = vec!["Second"]; // select only the second sheet
    let ds = ingest_excel_workbook_from_path(&path, Some(&sheets), &schema).unwrap();
    assert_eq!(ds.row_count(), 1);
    assert_eq!(ds.rows[0][0], Value::Int64(3));

    let _ = std::fs::remove_file(&path);
}

