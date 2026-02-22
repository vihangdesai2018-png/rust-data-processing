//! Row filtering for [`crate::types::DataSet`].

use crate::types::{DataSet, Value};

/// Returns a new [`DataSet`] containing only rows for which `predicate` returns `true`.
///
/// This is a convenience wrapper around [`DataSet::filter_rows`].
pub fn filter<F>(dataset: &DataSet, predicate: F) -> DataSet
where
    F: FnMut(&[Value]) -> bool,
{
    dataset.filter_rows(predicate)
}

#[cfg(test)]
mod tests {
    use super::filter;
    use crate::types::{DataSet, DataType, Field, Schema, Value};

    fn sample_dataset() -> DataSet {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("active", DataType::Bool),
            Field::new("name", DataType::Utf8),
        ]);

        let rows = vec![
            vec![Value::Int64(1), Value::Bool(true), Value::Utf8("a".to_string())],
            vec![Value::Int64(2), Value::Bool(false), Value::Utf8("b".to_string())],
            vec![Value::Int64(3), Value::Bool(true), Value::Utf8("c".to_string())],
        ];

        DataSet::new(schema, rows)
    }

    #[test]
    fn schema_index_of_works() {
        let ds = sample_dataset();
        assert_eq!(ds.schema.index_of("id"), Some(0));
        assert_eq!(ds.schema.index_of("active"), Some(1));
        assert_eq!(ds.schema.index_of("name"), Some(2));
        assert_eq!(ds.schema.index_of("missing"), None);
    }

    #[test]
    fn filter_rows_by_numeric_predicate() {
        let ds = sample_dataset();
        let id_idx = ds.schema.index_of("id").unwrap();

        let out = ds.filter_rows(|row| matches!(row.get(id_idx), Some(Value::Int64(v)) if *v > 1));

        assert_eq!(out.schema, ds.schema);
        assert_eq!(out.row_count(), 2);
        assert_eq!(
            out.rows,
            vec![
                vec![Value::Int64(2), Value::Bool(false), Value::Utf8("b".to_string())],
                vec![Value::Int64(3), Value::Bool(true), Value::Utf8("c".to_string())],
            ]
        );
        // Original unchanged
        assert_eq!(ds.row_count(), 3);
    }

    #[test]
    fn filter_rows_by_bool_predicate() {
        let ds = sample_dataset();
        let active_idx = ds.schema.index_of("active").unwrap();

        let out = filter(&ds, |row| matches!(row.get(active_idx), Some(Value::Bool(true))));

        assert_eq!(out.row_count(), 2);
        assert_eq!(
            out.rows,
            vec![
                vec![Value::Int64(1), Value::Bool(true), Value::Utf8("a".to_string())],
                vec![Value::Int64(3), Value::Bool(true), Value::Utf8("c".to_string())],
            ]
        );
    }

    #[test]
    fn filter_rows_can_return_empty_dataset() {
        let ds = sample_dataset();
        let out = ds.filter_rows(|_| false);
        assert_eq!(out.schema, ds.schema);
        assert!(out.rows.is_empty());
    }
}

