//! Parquet ingestion implementation.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use parquet::file::reader::{ChunkReader, FileReader};
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::Field;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Schema, Value};

/// Ingest a Parquet file into an in-memory `DataSet`.
///
/// Notes:
/// - Validates that all schema fields exist as Parquet leaf columns (by column path string)
/// - Uses the Parquet record API (`RowIter`) for a first implementation
pub fn ingest_parquet_from_path(path: impl AsRef<Path>, schema: &Schema) -> IngestionResult<DataSet> {
    let reader = SerializedFileReader::try_from(path.as_ref())?;

    let available_columns = parquet_leaf_column_paths(&reader);
    for field in &schema.fields {
        if !available_columns.contains(field.name.as_str()) {
            return Err(IngestionError::SchemaMismatch {
                message: format!("missing required column '{}'", field.name),
            });
        }
    }

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for (idx0, row_res) in reader.into_iter().enumerate() {
        let row_num = idx0 + 1;
        let row = row_res?;

        // Build a name->Field map for lookup.
        let mut map: HashMap<&str, &Field> = HashMap::new();
        for (name, field) in row.get_column_iter() {
            map.insert(name.as_str(), field);
        }

        let mut out_row: Vec<Value> = Vec::with_capacity(schema.fields.len());
        for f in &schema.fields {
            let v = map.get(f.name.as_str()).ok_or_else(|| IngestionError::SchemaMismatch {
                message: format!("row {row_num} missing required column '{}'", f.name),
            })?;
            out_row.push(convert_parquet_field(row_num, &f.name, &f.data_type, v)?);
        }
        rows.push(out_row);
    }

    Ok(DataSet::new(schema.clone(), rows))
}

fn parquet_leaf_column_paths<R: ChunkReader + 'static>(
    reader: &SerializedFileReader<R>,
) -> HashSet<String> {
    let mut set = HashSet::new();
    let cols = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns();
    for c in cols {
        set.insert(c.path().string());
    }
    set
}

fn convert_parquet_field(
    row: usize,
    column: &str,
    data_type: &DataType,
    f: &Field,
) -> IngestionResult<Value> {
    match f {
        Field::Null => return Ok(Value::Null),
        _ => {}
    }

    match data_type {
        DataType::Utf8 => match f {
            Field::Str(s) => Ok(Value::Utf8(s.clone())),
            _ => Err(IngestionError::ParseError {
                row,
                column: column.to_string(),
                raw: f.to_string(),
                message: "expected string".to_string(),
            }),
        },
        DataType::Bool => match f {
            Field::Bool(b) => Ok(Value::Bool(*b)),
            _ => Err(IngestionError::ParseError {
                row,
                column: column.to_string(),
                raw: f.to_string(),
                message: "expected bool".to_string(),
            }),
        },
        DataType::Int64 => match f {
            Field::Byte(v) => Ok(Value::Int64(i64::from(*v))),
            Field::Short(v) => Ok(Value::Int64(i64::from(*v))),
            Field::Int(v) => Ok(Value::Int64(i64::from(*v))),
            Field::Long(v) => Ok(Value::Int64(*v)),
            Field::UByte(v) => Ok(Value::Int64(i64::from(*v))),
            Field::UShort(v) => Ok(Value::Int64(i64::from(*v))),
            Field::UInt(v) => Ok(Value::Int64(i64::from(*v))),
            Field::ULong(v) => i64::try_from(*v)
                .map(Value::Int64)
                .map_err(|_| IngestionError::ParseError {
                    row,
                    column: column.to_string(),
                    raw: f.to_string(),
                    message: "u64 out of range for i64".to_string(),
                }),
            _ => Err(IngestionError::ParseError {
                row,
                column: column.to_string(),
                raw: f.to_string(),
                message: "expected integer".to_string(),
            }),
        },
        DataType::Float64 => match f {
            Field::Float(v) => Ok(Value::Float64(f64::from(*v))),
            Field::Double(v) => Ok(Value::Float64(*v)),
            _ => Err(IngestionError::ParseError {
                row,
                column: column.to_string(),
                raw: f.to_string(),
                message: "expected number".to_string(),
            }),
        },
    }
}
