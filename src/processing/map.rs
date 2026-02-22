//! Row mapping for [`crate::types::DataSet`].

use crate::types::{DataSet, Value};

/// Returns a new [`DataSet`] by applying `mapper` to every row.
///
/// This is a convenience wrapper around [`DataSet::map_rows`].
///
/// # Panics
///
/// Panics if `mapper` returns rows with a different length than the schema field count.
pub fn map<F>(dataset: &DataSet, mapper: F) -> DataSet
where
    F: FnMut(&[Value]) -> Vec<Value>,
{
    dataset.map_rows(mapper)
}

#[cfg(test)]
mod tests {
    use super::map;
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
    fn map_rows_transforms_values_and_preserves_schema() {
        let ds = sample_dataset();
        let out = map(&ds, |row| {
            let id = match &row[0] {
                Value::Int64(v) => Value::Int64(v + 10),
                other => other.clone(),
            };
            let active = match &row[1] {
                Value::Bool(v) => Value::Bool(!v),
                other => other.clone(),
            };
            let name = match &row[2] {
                Value::Utf8(s) => Value::Utf8(s.to_uppercase()),
                other => other.clone(),
            };
            vec![id, active, name]
        });

        assert_eq!(out.schema, ds.schema);
        assert_eq!(out.row_count(), 3);
        assert_eq!(
            out.rows,
            vec![
                vec![
                    Value::Int64(11),
                    Value::Bool(false),
                    Value::Utf8("A".to_string())
                ],
                vec![
                    Value::Int64(12),
                    Value::Bool(true),
                    Value::Utf8("B".to_string())
                ],
                vec![
                    Value::Int64(13),
                    Value::Bool(false),
                    Value::Utf8("C".to_string())
                ],
            ]
        );

        // Original unchanged
        assert_eq!(ds.rows[0][0], Value::Int64(1));
        assert_eq!(ds.rows[0][1], Value::Bool(true));
        assert_eq!(ds.rows[0][2], Value::Utf8("a".to_string()));
    }

    #[test]
    #[should_panic(expected = "mapped row length")]
    fn map_rows_panics_if_mapper_returns_wrong_arity() {
        let ds = sample_dataset();
        let _ = ds.map_rows(|_row| vec![Value::Int64(1)]);
    }
}

