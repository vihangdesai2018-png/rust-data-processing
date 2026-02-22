//! Reduction operations for [`crate::types::DataSet`].

use crate::types::{DataSet, DataType, Value};

/// Built-in reduction operations over a single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReduceOp {
    /// Count all rows (including nulls).
    Count,
    /// Sum numeric values, ignoring nulls.
    Sum,
    /// Minimum numeric value, ignoring nulls.
    Min,
    /// Maximum numeric value, ignoring nulls.
    Max,
}

/// Reduce a column using a built-in [`ReduceOp`].
///
/// - Returns `None` if `column` does not exist in the schema.
/// - For `Sum`/`Min`/`Max`, returns `Some(Value::Null)` if there are no non-null values.
/// - For `Count`, always returns `Some(Value::Int64(row_count))`.
pub fn reduce(dataset: &DataSet, column: &str, op: ReduceOp) -> Option<Value> {
    let idx = dataset.schema.index_of(column)?;

    match op {
        ReduceOp::Count => Some(Value::Int64(dataset.row_count() as i64)),
        ReduceOp::Sum | ReduceOp::Min | ReduceOp::Max => match dataset.schema.fields.get(idx) {
            Some(field) => reduce_numeric_typed(dataset, idx, field.data_type.clone(), op),
            None => None,
        },
    }
}

fn reduce_numeric_typed(
    dataset: &DataSet,
    idx: usize,
    data_type: DataType,
    op: ReduceOp,
) -> Option<Value> {
    match data_type {
        DataType::Int64 => {
            let mut acc: Option<i64> = None;
            for row in &dataset.rows {
                match row.get(idx) {
                    Some(Value::Null) | None => {}
                    Some(Value::Int64(v)) => {
                        acc = Some(match (op, acc) {
                            (ReduceOp::Sum, Some(a)) => a + v,
                            (ReduceOp::Sum, None) => *v,
                            (ReduceOp::Min, Some(a)) => a.min(*v),
                            (ReduceOp::Min, None) => *v,
                            (ReduceOp::Max, Some(a)) => a.max(*v),
                            (ReduceOp::Max, None) => *v,
                            _ => unreachable!("non-numeric op handled earlier"),
                        });
                    }
                    Some(_) => {}
                }
            }
            Some(acc.map(Value::Int64).unwrap_or(Value::Null))
        }
        DataType::Float64 => {
            let mut acc: Option<f64> = None;
            for row in &dataset.rows {
                match row.get(idx) {
                    Some(Value::Null) | None => {}
                    Some(Value::Float64(v)) => {
                        acc = Some(match (op, acc) {
                            (ReduceOp::Sum, Some(a)) => a + v,
                            (ReduceOp::Sum, None) => *v,
                            (ReduceOp::Min, Some(a)) => a.min(*v),
                            (ReduceOp::Min, None) => *v,
                            (ReduceOp::Max, Some(a)) => a.max(*v),
                            (ReduceOp::Max, None) => *v,
                            _ => unreachable!("non-numeric op handled earlier"),
                        });
                    }
                    Some(_) => {}
                }
            }
            Some(acc.map(Value::Float64).unwrap_or(Value::Null))
        }
        _ => Some(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::{reduce, ReduceOp};
    use crate::types::{DataSet, DataType, Field, Schema, Value};

    fn numeric_dataset_with_nulls() -> DataSet {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("score", DataType::Float64),
        ]);

        let rows = vec![
            vec![Value::Int64(1), Value::Float64(10.0)],
            vec![Value::Int64(2), Value::Null],
            vec![Value::Int64(3), Value::Float64(5.5)],
        ];

        DataSet::new(schema, rows)
    }

    #[test]
    fn reduce_count_counts_rows() {
        let ds = numeric_dataset_with_nulls();
        assert_eq!(reduce(&ds, "score", ReduceOp::Count), Some(Value::Int64(3)));
        assert_eq!(reduce(&ds, "id", ReduceOp::Count), Some(Value::Int64(3)));
    }

    #[test]
    fn reduce_sum_ignores_nulls_and_preserves_type() {
        let ds = numeric_dataset_with_nulls();
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Sum),
            Some(Value::Float64(15.5))
        );
        assert_eq!(reduce(&ds, "id", ReduceOp::Sum), Some(Value::Int64(6)));
    }

    #[test]
    fn reduce_min_max_ignore_nulls() {
        let ds = numeric_dataset_with_nulls();
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Min),
            Some(Value::Float64(5.5))
        );
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Max),
            Some(Value::Float64(10.0))
        );
        assert_eq!(reduce(&ds, "id", ReduceOp::Min), Some(Value::Int64(1)));
        assert_eq!(reduce(&ds, "id", ReduceOp::Max), Some(Value::Int64(3)));
    }

    #[test]
    fn reduce_returns_none_for_missing_column() {
        let ds = numeric_dataset_with_nulls();
        assert_eq!(reduce(&ds, "missing", ReduceOp::Count), None);
        assert_eq!(reduce(&ds, "missing", ReduceOp::Sum), None);
    }

    #[test]
    fn reduce_numeric_returns_null_if_all_values_null() {
        let schema = Schema::new(vec![Field::new("score", DataType::Float64)]);
        let ds = DataSet::new(schema, vec![vec![Value::Null], vec![Value::Null]]);
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Sum),
            Some(Value::Null)
        );
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Min),
            Some(Value::Null)
        );
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Max),
            Some(Value::Null)
        );
    }
}

