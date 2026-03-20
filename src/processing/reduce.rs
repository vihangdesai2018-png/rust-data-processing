//! Reduction operations for [`crate::types::DataSet`].

use std::collections::HashSet;

use crate::types::{DataSet, DataType, Value};

/// Population vs sample variance / standard deviation (`ddof` 0 vs 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarianceKind {
    /// Divide by `n` (when `n > 0`).
    Population,
    /// Divide by `n - 1` (when `n >= 2`); otherwise [`None`] / null.
    Sample,
}

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
    /// Arithmetic mean of numeric values as [`Value::Float64`], ignoring nulls.
    Mean,
    /// Variance (Welford); null if no values, or sample with fewer than two values.
    Variance(VarianceKind),
    /// Standard deviation from variance; same null rules as [`ReduceOp::Variance`].
    StdDev(VarianceKind),
    /// \(\sum x^2\) over non-null numeric values as [`Value::Float64`].
    SumSquares,
    /// \(\sqrt{\sum x^2}\) over non-null numeric values as [`Value::Float64`].
    L2Norm,
    /// Count of distinct non-null values (returns [`Value::Int64`]).
    CountDistinctNonNull,
}

/// Reduce a column using a built-in [`ReduceOp`].
///
/// - Returns `None` if `column` does not exist in the schema.
/// - For `Count`, always returns `Some(Value::Int64(row_count))`.
/// - For numeric aggregates other than `Count` / `CountDistinctNonNull`, returns
///   `Some(Value::Null)` if there are no non-null numeric values, or if the column type is not
///   numeric (for those ops). `CountDistinctNonNull` supports [`DataType::Bool`] and
///   [`DataType::Utf8`] as well as numeric types.
pub fn reduce(dataset: &DataSet, column: &str, op: ReduceOp) -> Option<Value> {
    let idx = dataset.schema.index_of(column)?;

    match op {
        ReduceOp::Count => Some(Value::Int64(dataset.row_count() as i64)),
        ReduceOp::CountDistinctNonNull => {
            let field = dataset.schema.fields.get(idx)?;
            reduce_count_distinct_non_null(dataset, idx, &field.data_type)
        }
        ReduceOp::Sum | ReduceOp::Min | ReduceOp::Max => match dataset.schema.fields.get(idx) {
            Some(field) => reduce_numeric_typed(dataset, idx, field.data_type.clone(), op),
            None => None,
        },
        ReduceOp::Mean
        | ReduceOp::Variance(_)
        | ReduceOp::StdDev(_)
        | ReduceOp::SumSquares
        | ReduceOp::L2Norm => match dataset.schema.fields.get(idx) {
            Some(field) => reduce_numeric_float_stats(dataset, idx, field.data_type.clone(), op),
            None => None,
        },
    }
}

#[derive(Default)]
pub(crate) struct Welford {
    n: u64,
    mean: f64,
    m2: f64,
}

impl Welford {
    pub(crate) fn observe(&mut self, x: f64) {
        self.n += 1;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    pub(crate) fn mean(&self) -> Option<f64> {
        (self.n > 0).then_some(self.mean)
    }

    pub(crate) fn variance(&self, kind: VarianceKind) -> Option<f64> {
        if self.n == 0 {
            return None;
        }
        match kind {
            VarianceKind::Population => Some(self.m2 / self.n as f64),
            VarianceKind::Sample => {
                if self.n < 2 {
                    None
                } else {
                    Some(self.m2 / (self.n - 1) as f64)
                }
            }
        }
    }

    pub(crate) fn observation_count(&self) -> u64 {
        self.n
    }
}

fn reduce_numeric_float_stats(
    dataset: &DataSet,
    idx: usize,
    data_type: DataType,
    op: ReduceOp,
) -> Option<Value> {
    match data_type {
        dt @ (DataType::Int64 | DataType::Float64) => {
            let is_int = matches!(dt, DataType::Int64);
            let mut w = Welford::default();
            let mut sum_squares = 0.0_f64;
            let mut any = false;

            for row in &dataset.rows {
                let x = match row.get(idx) {
                    Some(Value::Null) | None => None,
                    Some(Value::Int64(v)) if is_int => Some(*v as f64),
                    Some(Value::Float64(v)) if !is_int => Some(*v),
                    Some(_) => None,
                };
                if let Some(x) = x {
                    any = true;
                    w.observe(x);
                    sum_squares += x * x;
                }
            }

            if !any {
                return Some(Value::Null);
            }

            let out = match op {
                ReduceOp::Mean => Value::Float64(w.mean().expect("n > 0")),
                ReduceOp::Variance(kind) => match w.variance(kind) {
                    Some(v) => Value::Float64(v),
                    None => Value::Null,
                },
                ReduceOp::StdDev(kind) => match w.variance(kind) {
                    Some(v) => Value::Float64(v.sqrt()),
                    None => Value::Null,
                },
                ReduceOp::SumSquares => Value::Float64(sum_squares),
                ReduceOp::L2Norm => Value::Float64(sum_squares.sqrt()),
                _ => unreachable!("caller only dispatches float stats ops"),
            };
            Some(out)
        }
        _ => Some(Value::Null),
    }
}

fn reduce_count_distinct_non_null(
    dataset: &DataSet,
    idx: usize,
    data_type: &DataType,
) -> Option<Value> {
    let n = match data_type {
        DataType::Int64 => {
            let mut set = HashSet::new();
            for row in &dataset.rows {
                if let Some(Value::Int64(v)) = row.get(idx) {
                    set.insert(*v);
                }
            }
            set.len() as i64
        }
        DataType::Float64 => {
            let mut set = HashSet::new();
            for row in &dataset.rows {
                if let Some(Value::Float64(v)) = row.get(idx) {
                    set.insert(v.to_bits());
                }
            }
            set.len() as i64
        }
        DataType::Bool => {
            let mut set = HashSet::new();
            for row in &dataset.rows {
                if let Some(Value::Bool(v)) = row.get(idx) {
                    set.insert(*v);
                }
            }
            set.len() as i64
        }
        DataType::Utf8 => {
            let mut set = HashSet::new();
            for row in &dataset.rows {
                if let Some(Value::Utf8(s)) = row.get(idx) {
                    set.insert(s.clone());
                }
            }
            set.len() as i64
        }
    };
    Some(Value::Int64(n))
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
    use super::{reduce, ReduceOp, VarianceKind};
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
        assert_eq!(reduce(&ds, "score", ReduceOp::Mean), Some(Value::Null));
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Variance(VarianceKind::Population)),
            Some(Value::Null)
        );
        assert_eq!(
            reduce(&ds, "score", ReduceOp::StdDev(VarianceKind::Sample)),
            Some(Value::Null)
        );
    }

    #[test]
    fn reduce_mean_float_and_int() {
        let ds = numeric_dataset_with_nulls();
        assert_eq!(
            reduce(&ds, "score", ReduceOp::Mean),
            Some(Value::Float64(7.75))
        );
        assert_eq!(reduce(&ds, "id", ReduceOp::Mean), Some(Value::Float64(2.0)));
    }

    #[test]
    fn reduce_variance_std_known_values() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Float64(1.0)],
                vec![Value::Float64(2.0)],
                vec![Value::Float64(3.0)],
            ],
        );
        let pop = 2.0 / 3.0;
        assert_eq!(
            reduce(&ds, "x", ReduceOp::Variance(VarianceKind::Population)),
            Some(Value::Float64(pop))
        );
        assert_eq!(
            reduce(&ds, "x", ReduceOp::Variance(VarianceKind::Sample)),
            Some(Value::Float64(1.0))
        );
        let std_pop = reduce(&ds, "x", ReduceOp::StdDev(VarianceKind::Population)).unwrap();
        match std_pop {
            Value::Float64(v) => assert!((v - pop.sqrt()).abs() < 1e-12),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn reduce_sample_variance_single_value_is_null() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let ds = DataSet::new(schema, vec![vec![Value::Float64(42.0)]]);
        assert_eq!(
            reduce(&ds, "x", ReduceOp::Variance(VarianceKind::Sample)),
            Some(Value::Null)
        );
    }

    #[test]
    fn reduce_population_variance_single_value_is_zero() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let ds = DataSet::new(schema, vec![vec![Value::Float64(42.0)]]);
        assert_eq!(
            reduce(&ds, "x", ReduceOp::Variance(VarianceKind::Population)),
            Some(Value::Float64(0.0))
        );
        let std0 = reduce(&ds, "x", ReduceOp::StdDev(VarianceKind::Population)).unwrap();
        match std0 {
            Value::Float64(v) => assert_eq!(v, 0.0),
            other => panic!("expected Float64, got {other:?}"),
        }
    }

    #[test]
    fn reduce_int64_mean_sum_squares_and_distinct() {
        let schema = Schema::new(vec![Field::new("k", DataType::Int64)]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Int64(2)],
                vec![Value::Int64(3)],
                vec![Value::Null],
            ],
        );
        assert_eq!(reduce(&ds, "k", ReduceOp::Mean), Some(Value::Float64(2.5)));
        assert_eq!(
            reduce(&ds, "k", ReduceOp::SumSquares),
            Some(Value::Float64(13.0))
        );
        assert_eq!(
            reduce(&ds, "k", ReduceOp::L2Norm),
            Some(Value::Float64(13.0_f64.sqrt()))
        );
        assert_eq!(
            reduce(&ds, "k", ReduceOp::CountDistinctNonNull),
            Some(Value::Int64(2))
        );
    }

    #[test]
    fn reduce_sum_squares_and_l2() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Float64(3.0)],
                vec![Value::Float64(4.0)],
                vec![Value::Null],
            ],
        );
        assert_eq!(
            reduce(&ds, "x", ReduceOp::SumSquares),
            Some(Value::Float64(25.0))
        );
        assert_eq!(
            reduce(&ds, "x", ReduceOp::L2Norm),
            Some(Value::Float64(5.0))
        );
    }

    #[test]
    fn reduce_count_distinct_non_null() {
        let schema = Schema::new(vec![
            Field::new("f", DataType::Float64),
            Field::new("s", DataType::Utf8),
        ]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Float64(1.0), Value::Utf8("a".to_string())],
                vec![Value::Float64(1.0), Value::Utf8("b".to_string())],
                vec![Value::Null, Value::Null],
            ],
        );
        assert_eq!(
            reduce(&ds, "f", ReduceOp::CountDistinctNonNull),
            Some(Value::Int64(1))
        );
        assert_eq!(
            reduce(&ds, "s", ReduceOp::CountDistinctNonNull),
            Some(Value::Int64(2))
        );
    }

    #[test]
    fn reduce_new_ops_return_none_for_missing_column() {
        let ds = numeric_dataset_with_nulls();
        assert_eq!(reduce(&ds, "nope", ReduceOp::Mean), None);
        assert_eq!(
            reduce(&ds, "nope", ReduceOp::Variance(VarianceKind::Sample)),
            None
        );
        assert_eq!(
            reduce(&ds, "nope", ReduceOp::CountDistinctNonNull),
            None
        );
    }

    #[test]
    fn reduce_sum_squares_and_l2_all_null() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let ds = DataSet::new(schema, vec![vec![Value::Null]]);
        assert_eq!(reduce(&ds, "x", ReduceOp::SumSquares), Some(Value::Null));
        assert_eq!(reduce(&ds, "x", ReduceOp::L2Norm), Some(Value::Null));
    }

    #[test]
    fn reduce_count_distinct_bool_and_empty_rows() {
        let schema = Schema::new(vec![Field::new("b", DataType::Bool)]);
        let ds = DataSet::new(schema.clone(), vec![]);
        assert_eq!(
            reduce(&ds, "b", ReduceOp::CountDistinctNonNull),
            Some(Value::Int64(0))
        );

        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Bool(true)],
                vec![Value::Bool(false)],
                vec![Value::Bool(true)],
                vec![Value::Null],
            ],
        );
        assert_eq!(
            reduce(&ds, "b", ReduceOp::CountDistinctNonNull),
            Some(Value::Int64(2))
        );
    }

    #[test]
    fn reduce_mean_variance_null_for_non_numeric_column() {
        let schema = Schema::new(vec![Field::new("label", DataType::Utf8)]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Utf8("a".to_string())],
                vec![Value::Utf8("b".to_string())],
            ],
        );
        assert_eq!(reduce(&ds, "label", ReduceOp::Mean), Some(Value::Null));
        assert_eq!(
            reduce(&ds, "label", ReduceOp::Variance(VarianceKind::Population)),
            Some(Value::Null)
        );
        assert_eq!(
            reduce(&ds, "label", ReduceOp::SumSquares),
            Some(Value::Null)
        );
    }

    #[test]
    fn reduce_std_dev_sample_matches_sqrt_of_sample_variance() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Float64(0.0)],
                vec![Value::Float64(4.0)],
                vec![Value::Float64(8.0)],
            ],
        );
        let var_s = match reduce(&ds, "x", ReduceOp::Variance(VarianceKind::Sample)).unwrap() {
            Value::Float64(v) => v,
            other => panic!("expected Float64, got {other:?}"),
        };
        let std_s = match reduce(&ds, "x", ReduceOp::StdDev(VarianceKind::Sample)).unwrap() {
            Value::Float64(v) => v,
            other => panic!("expected Float64, got {other:?}"),
        };
        assert!((std_s - var_s.sqrt()).abs() < 1e-12);
    }

    #[test]
    fn reduce_l2_squared_matches_sum_squares_for_non_nulls() {
        let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
        let ds = DataSet::new(
            schema,
            vec![vec![Value::Float64(2.0)], vec![Value::Float64(3.0)]],
        );
        let ss = match reduce(&ds, "x", ReduceOp::SumSquares).unwrap() {
            Value::Float64(v) => v,
            other => panic!("expected Float64, got {other:?}"),
        };
        let l2 = match reduce(&ds, "x", ReduceOp::L2Norm).unwrap() {
            Value::Float64(v) => v,
            other => panic!("expected Float64, got {other:?}"),
        };
        assert!((l2 * l2 - ss).abs() < 1e-12);
    }
}
