//! Optional SQL support (feature-gated).
//!
//! Phase 1 note: this module is intentionally minimal and does not commit to a specific engine
//! backend yet (e.g. DataFusion). The rest of the crate stays Polars-first and DataFrame-centric.

use crate::error::{IngestionError, IngestionResult};
use crate::pipeline::DataFrame;

/// Execute a SQL query against a [`DataFrame`].
///
/// This is a Phase 1 placeholder behind the `sql` feature flag.
pub fn query(_df: &DataFrame, _sql: &str) -> IngestionResult<DataFrame> {
    Err(IngestionError::SchemaMismatch {
        message: "sql support is not implemented yet (feature-gated placeholder)".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::query;
    use crate::pipeline::DataFrame;
    use crate::types::{DataSet, DataType, Field, Schema, Value};

    #[test]
    fn sql_query_placeholder_returns_error() {
        let ds = DataSet::new(
            Schema::new(vec![Field::new("id", DataType::Int64)]),
            vec![vec![Value::Int64(1)]],
        );
        let df = DataFrame::from_dataset(&ds).unwrap();
        let res = query(&df, "select * from df");
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(err.to_string().contains("sql support is not implemented yet"));
    }
}

