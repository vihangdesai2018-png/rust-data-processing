#![cfg(feature = "excel")]

use std::path::Path;

use calamine::{open_workbook_auto, Data, Reader};

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Schema, Value};

/// Ingest an Excel document (`.xlsx`, `.xls`, `.ods`, etc.) into an in-memory `DataSet`.
///
/// Behavior:
/// - Picks `sheet_name` if provided; otherwise uses the first sheet in the workbook
/// - Detects the first non-empty row as the header row
/// - Validates that all schema fields exist as headers
/// - Reads remaining rows and converts cells into typed `Value`s
pub fn ingest_excel_from_path(
    path: impl AsRef<Path>,
    sheet_name: Option<&str>,
    schema: &Schema,
) -> IngestionResult<DataSet> {
    let sheets: Option<Vec<&str>> = sheet_name.map(|s| vec![s]);
    ingest_excel_workbook_from_path(path, sheets.as_deref(), schema)
}

/// Ingest multiple sheets from an Excel workbook and concatenate all rows into one `DataSet`.
///
/// - If `sheet_names` is `None`, ingests **all sheets** in workbook order.
/// - If `sheet_names` is `Some(&[...])`, ingests only those sheets (in the provided order).
///
/// Assumption for 1.1.3.1: all tabs share the same header schema.
pub fn ingest_excel_workbook_from_path(
    path: impl AsRef<Path>,
    sheet_names: Option<&[&str]>,
    schema: &Schema,
) -> IngestionResult<DataSet> {
    let mut workbook = open_workbook_auto(path)?;

    let sheets: Vec<String> = match sheet_names {
        Some(names) => names.iter().map(|s| s.to_string()).collect(),
        None => workbook.sheet_names().to_vec(),
    };
    if sheets.is_empty() {
        return Err(IngestionError::SchemaMismatch {
            message: "workbook has no sheets".to_string(),
        });
    }

    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    for sheet in sheets {
        let range = workbook.worksheet_range(&sheet)?;
        let mut sheet_rows = ingest_sheet_range(&sheet, &range, schema)?;
        all_rows.append(&mut sheet_rows);
    }

    Ok(DataSet::new(schema.clone(), all_rows))
}

fn ingest_sheet_range(
    sheet: &str,
    range: &calamine::Range<Data>,
    schema: &Schema,
) -> IngestionResult<Vec<Vec<Value>>> {
    let (header_row_idx, col_idxs, header_cells) = build_header_projection(range, schema)
        .map_err(|e| wrap_schema_err_with_sheet(sheet, e))?;

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for (idx0, row) in range.rows().enumerate() {
        if idx0 <= header_row_idx {
            continue;
        }

        // Report 1-based row number (Excel-like).
        let user_row = idx0 + 1;

        let mut out_row: Vec<Value> = Vec::with_capacity(schema.fields.len());
        for (field, &col_idx) in schema.fields.iter().zip(col_idxs.iter()) {
            let cell = row.get(col_idx).unwrap_or(&Data::Empty);
            let col_label = format!("{sheet}:{name}", name = field.name);
            out_row.push(convert_cell(user_row, &col_label, &field.data_type, cell)?);
        }
        rows.push(out_row);
    }

    // Use header_cells only to avoid unused warning in some feature builds.
    let _ = header_cells;
    Ok(rows)
}

fn wrap_schema_err_with_sheet(sheet: &str, err: IngestionError) -> IngestionError {
    match err {
        IngestionError::SchemaMismatch { message } => IngestionError::SchemaMismatch {
            message: format!("sheet '{sheet}': {message}"),
        },
        other => other,
    }
}

fn build_header_projection(
    range: &calamine::Range<Data>,
    schema: &Schema,
) -> IngestionResult<(usize, Vec<usize>, Vec<String>)> {
    let mut header_row_idx: Option<usize> = None;
    let mut header_cells: Option<Vec<String>> = None;

    for (idx0, row) in range.rows().enumerate() {
        let non_empty = row.iter().any(|c| !matches!(c, Data::Empty));
        if non_empty {
            header_row_idx = Some(idx0);
            header_cells = Some(row.iter().map(cell_to_header_string).collect());
            break;
        }
    }

    let header_row_idx = header_row_idx.ok_or_else(|| IngestionError::SchemaMismatch {
        message: "sheet has no non-empty rows (no header row found)".to_string(),
    })?;
    let header_cells = header_cells.unwrap_or_default();

    // Build a projection of schema field -> column index by searching header_cells.
    let mut col_idxs: Vec<usize> = Vec::with_capacity(schema.fields.len());
    for f in &schema.fields {
        match header_cells.iter().position(|h| h.trim() == f.name) {
            Some(idx) => col_idxs.push(idx),
            None => {
                return Err(IngestionError::SchemaMismatch {
                    message: format!(
                        "missing required column '{}'. headers={:?}",
                        f.name, header_cells
                    ),
                });
            }
        }
    }

    Ok((header_row_idx, col_idxs, header_cells))
}

fn cell_to_header_string(c: &Data) -> String {
    match c {
        Data::String(s) => s.clone(),
        Data::Int(i) => i.to_string(),
        Data::Float(f) => {
            if f.fract() == 0.0 {
                (*f as i64).to_string()
            } else {
                f.to_string()
            }
        }
        Data::Bool(b) => b.to_string(),
        Data::DateTime(f) => f.to_string(),
        Data::DateTimeIso(s) => s.clone(),
        Data::DurationIso(s) => s.clone(),
        Data::Error(e) => format!("{e:?}"),
        Data::Empty => "".to_string(),
    }
}

fn convert_cell(row: usize, column: &str, data_type: &DataType, c: &Data) -> IngestionResult<Value> {
    if matches!(c, Data::Empty) {
        return Ok(Value::Null);
    }

    match data_type {
        DataType::Utf8 => Ok(Value::Utf8(cell_to_string(c))),
        DataType::Bool => parse_bool_cell(row, column, c).map(Value::Bool),
        DataType::Int64 => parse_i64_cell(row, column, c).map(Value::Int64),
        DataType::Float64 => parse_f64_cell(row, column, c).map(Value::Float64),
    }
}

fn cell_to_string(c: &Data) -> String {
    match c {
        Data::String(s) => s.clone(),
        _ => c.to_string(),
    }
}

fn parse_bool_cell(row: usize, column: &str, c: &Data) -> IngestionResult<bool> {
    match c {
        Data::Bool(b) => Ok(*b),
        Data::Int(i) => Ok(*i != 0),
        Data::Float(f) => Ok(*f != 0.0),
        Data::String(s) => parse_bool_str(s).map_err(|message| IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: s.clone(),
            message,
        }),
        _ => Err(IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: c.to_string(),
            message: "expected bool".to_string(),
        }),
    }
}

fn parse_bool_str(s: &str) -> Result<bool, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        _ => Err("expected bool (true/false/1/0/yes/no)".to_string()),
    }
}

fn parse_i64_cell(row: usize, column: &str, c: &Data) -> IngestionResult<i64> {
    match c {
        Data::Int(i) => Ok(*i),
        Data::Float(f) => {
            if f.fract() == 0.0 {
                Ok(*f as i64)
            } else {
                Err(IngestionError::ParseError {
                    row,
                    column: column.to_string(),
                    raw: c.to_string(),
                    message: "expected integer (got non-integer float)".to_string(),
                })
            }
        }
        Data::String(s) => s.trim().parse::<i64>().map_err(|e| IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: s.clone(),
            message: e.to_string(),
        }),
        _ => Err(IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: c.to_string(),
            message: "expected integer".to_string(),
        }),
    }
}

fn parse_f64_cell(row: usize, column: &str, c: &Data) -> IngestionResult<f64> {
    match c {
        Data::Float(f) => Ok(*f),
        Data::Int(i) => Ok(*i as f64),
        Data::String(s) => s.trim().parse::<f64>().map_err(|e| IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: s.clone(),
            message: e.to_string(),
        }),
        _ => Err(IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: c.to_string(),
            message: "expected number".to_string(),
        }),
    }
}

