use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use rust_data_processing::ingestion::parquet::ingest_parquet_from_path;
use rust_data_processing::types::{DataType, Field, Schema, Value};

fn tmp_file(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("rust-data-processing-{name}-{nanos}.parquet"))
}

fn people_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("active", DataType::Bool),
    ])
}

fn write_people_parquet(path: &PathBuf, include_active: bool, id_as_binary: bool) {
    let schema_str = if include_active {
        if id_as_binary {
            r#"
            message schema {
              REQUIRED BINARY id (UTF8);
              REQUIRED BINARY name (UTF8);
              REQUIRED DOUBLE score;
              REQUIRED BOOLEAN active;
            }
            "#
        } else {
            r#"
            message schema {
              REQUIRED INT64 id;
              REQUIRED BINARY name (UTF8);
              REQUIRED DOUBLE score;
              REQUIRED BOOLEAN active;
            }
            "#
        }
    } else if id_as_binary {
        r#"
        message schema {
          REQUIRED BINARY id (UTF8);
          REQUIRED BINARY name (UTF8);
          REQUIRED DOUBLE score;
        }
        "#
    } else {
        r#"
        message schema {
          REQUIRED INT64 id;
          REQUIRED BINARY name (UTF8);
          REQUIRED DOUBLE score;
        }
        "#
    };

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
                // Used for both `id` (if id_as_binary) and `name`.
                // We determine which by column order:
                // - if id_as_binary: col0=id, col1=name
                // - else: the only byte array column is name
                let is_id = id_as_binary && col_idx == 0;
                if is_id {
                    let id1 = ByteArray::from("1");
                    let id2 = ByteArray::from("2");
                    w.write_batch(&[id1, id2], None, None).unwrap();
                } else {
                    let v1 = ByteArray::from("Ada");
                    let v2 = ByteArray::from("Grace");
                    w.write_batch(&[v1, v2], None, None).unwrap();
                }
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
fn ingest_parquet_happy_path() {
    let schema = people_schema();
    let path = tmp_file("people");
    write_people_parquet(&path, true, false);

    let ds = ingest_parquet_from_path(&path, &schema).unwrap();
    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[0][0], Value::Int64(1));
    assert_eq!(ds.rows[0][1], Value::Utf8("Ada".to_string()));
    assert_eq!(ds.rows[1][3], Value::Bool(false));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn ingest_parquet_errors_on_missing_required_column() {
    let schema = people_schema();
    let path = tmp_file("missing");
    write_people_parquet(&path, false, false);

    let err = ingest_parquet_from_path(&path, &schema).unwrap_err();
    assert!(err.to_string().contains("missing required column 'active'"));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn ingest_parquet_errors_on_type_mismatch() {
    let schema = people_schema();
    let path = tmp_file("type-mismatch");
    write_people_parquet(&path, true, true);

    let err = ingest_parquet_from_path(&path, &schema).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("failed to parse value"));
    assert!(msg.contains("column 'id'"));
    let _ = std::fs::remove_file(&path);
}

#[test]
#[ignore]
fn parquet_perf_smoke_test() {
    use std::time::Instant;

    // Generates a moderately sized parquet file and measures ingestion time.
    // This is ignored by default; run with: `cargo test -- --ignored`
    let schema = people_schema();
    let path = tmp_file("perf");

    // Write 100k rows by repeating the same 2-row batch 50k times.
    let schema_str = r#"
    message schema {
      REQUIRED INT64 id;
      REQUIRED BINARY name (UTF8);
      REQUIRED DOUBLE score;
      REQUIRED BOOLEAN active;
    }
    "#;
    let schema_pq = Arc::new(parse_message_type(schema_str).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema_pq, props).unwrap();
    let mut rg = writer.next_row_group().unwrap();

    // Write columns in order with 100k values each.
    let n: usize = 100_000;
    let ids: Vec<i64> = (0..n as i64).collect();
    let scores: Vec<f64> = (0..n).map(|i| (i % 100) as f64).collect();
    let actives: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();
    let names: Vec<ByteArray> = (0..n)
        .map(|i| ByteArray::from(if i % 2 == 0 { "Ada" } else { "Grace" }))
        .collect();

    let mut col_idx: usize = 0;
    while let Some(mut col) = rg.next_column().unwrap() {
        match col.untyped() {
            ColumnWriter::Int64ColumnWriter(w) => {
                w.write_batch(&ids, None, None).unwrap();
            }
            ColumnWriter::ByteArrayColumnWriter(w) => {
                // second column is name
                assert_eq!(col_idx, 1);
                w.write_batch(&names, None, None).unwrap();
            }
            ColumnWriter::DoubleColumnWriter(w) => {
                w.write_batch(&scores, None, None).unwrap();
            }
            ColumnWriter::BoolColumnWriter(w) => {
                w.write_batch(&actives, None, None).unwrap();
            }
            _ => panic!("unexpected column writer in perf test"),
        }
        col.close().unwrap();
        col_idx += 1;
    }
    rg.close().unwrap();
    writer.close().unwrap();

    let start = Instant::now();
    let ds = ingest_parquet_from_path(&path, &schema).unwrap();
    let elapsed = start.elapsed();
    eprintln!("ingested {} rows in {:?}", ds.row_count(), elapsed);

    let _ = std::fs::remove_file(&path);
}
