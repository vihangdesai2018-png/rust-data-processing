//! Multi-column and row-index reductions over a [`DataSet`].
//!
//! Aggregate semantics (nulls, all-null groups, casting) are documented in
//! `docs/REDUCE_AGG_SEMANTICS.md` at the repository root.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::types::{DataSet, DataType, Value};

use super::reduce::{VarianceKind, Welford};

/// Per-column mean and standard deviation (square root of variance under `std_kind`).
#[derive(Debug, Clone, PartialEq)]
pub struct FeatureMeanStd {
    pub mean: Value,
    pub std_dev: Value,
}

/// One pass over all rows: compute mean and std dev for each listed **numeric** column (`Int64` /
/// `Float64`). Nulls are ignored. If a column has no non-null values, both fields are
/// [`Value::Null`]. Sample std dev is undefined for fewer than two values → [`Value::Null`].
///
/// Returns [`None`] if any name is missing from the schema or is not numeric.
pub fn feature_wise_mean_std(
    dataset: &DataSet,
    columns: &[&str],
    std_kind: VarianceKind,
) -> Option<Vec<(String, FeatureMeanStd)>> {
    let mut meta: Vec<(String, usize, DataType)> = Vec::with_capacity(columns.len());
    for &name in columns {
        let idx = dataset.schema.index_of(name)?;
        let dt = dataset.schema.fields.get(idx)?.data_type.clone();
        if !matches!(dt, DataType::Int64 | DataType::Float64) {
            return None;
        }
        meta.push((name.to_string(), idx, dt));
    }

    let mut w: Vec<Welford> = (0..meta.len()).map(|_| Welford::default()).collect();
    for row in &dataset.rows {
        for (i, (_, idx, dt)) in meta.iter().enumerate() {
            let x = match (row.get(*idx), dt) {
                (Some(Value::Int64(v)), DataType::Int64) => Some(*v as f64),
                (Some(Value::Float64(v)), DataType::Float64) => Some(*v),
                _ => None,
            };
            if let Some(x) = x {
                w[i].observe(x);
            }
        }
    }

    let mut out = Vec::with_capacity(meta.len());
    for ((name, _, _), wf) in meta.into_iter().zip(w) {
        let mean = wf.mean().map(Value::Float64).unwrap_or(Value::Null);
        let std_dev = wf
            .variance(std_kind)
            .map(|v| Value::Float64(v.sqrt()))
            .unwrap_or(Value::Null);
        let (mean, std_dev) = if wf.observation_count() == 0 {
            (Value::Null, Value::Null)
        } else {
            (mean, std_dev)
        };
        out.push((name, FeatureMeanStd { mean, std_dev }));
    }
    Some(out)
}

fn cmp_non_null_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Int64(x), Value::Int64(y)) => Some(x.cmp(y)),
        (Value::Float64(x), Value::Float64(y)) => Some(x.total_cmp(y)),
        (Value::Utf8(x), Value::Utf8(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Returns [`None`] if `column` is not in the schema. Otherwise `Some(None)` if there is no
/// non-null comparable value, or `Some(Some((row_index, value)))` for the **first** row
/// attaining the maximum (stable tie-break).
pub fn arg_max_row(dataset: &DataSet, column: &str) -> Option<Option<(usize, Value)>> {
    let idx = dataset.schema.index_of(column)?;
    let mut best: Option<(usize, Value)> = None;
    for (r, row) in dataset.rows.iter().enumerate() {
        let Some(cell) = row.get(idx) else {
            continue;
        };
        if matches!(cell, Value::Null) {
            continue;
        }
        match &best {
            None => best = Some((r, cell.clone())),
            Some((_, bv)) => {
                if cmp_non_null_values(cell, bv) == Some(Ordering::Greater) {
                    best = Some((r, cell.clone()));
                }
            }
        }
    }
    Some(best)
}

/// Same as [`arg_max_row`] for the minimum.
pub fn arg_min_row(dataset: &DataSet, column: &str) -> Option<Option<(usize, Value)>> {
    let idx = dataset.schema.index_of(column)?;
    let mut best: Option<(usize, Value)> = None;
    for (r, row) in dataset.rows.iter().enumerate() {
        let Some(cell) = row.get(idx) else {
            continue;
        };
        if matches!(cell, Value::Null) {
            continue;
        }
        match &best {
            None => best = Some((r, cell.clone())),
            Some((_, bv)) => {
                if cmp_non_null_values(cell, bv) == Some(Ordering::Less) {
                    best = Some((r, cell.clone()));
                }
            }
        }
    }
    Some(best)
}

fn freq_bucket_key(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Int64(x) => Some(format!("i:{x}")),
        Value::Float64(x) => Some(format!("f:{}", x.to_bits())),
        Value::Bool(b) => Some(format!("b:{b}")),
        Value::Utf8(s) => Some(format!("s:{s}")),
    }
}

fn value_sort_key(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Int64(x) => format!("i:{x:020}"),
        Value::Float64(x) => format!("f:{:020}", x.to_bits()),
        Value::Bool(b) => format!("b:{b}"),
        Value::Utf8(s) => format!("s:{s}"),
    }
}

/// Non-null value frequencies; returns the top `k` pairs by count (desc), breaking ties by
/// `value_sort_key` ascending. `k == 0` yields an empty vector.
///
/// Returns [`None`] if the column is not in the schema.
pub fn top_k_by_frequency(dataset: &DataSet, column: &str, k: usize) -> Option<Vec<(Value, i64)>> {
    let idx = dataset.schema.index_of(column)?;
    let mut buckets: HashMap<String, (Value, i64)> = HashMap::new();
    for row in &dataset.rows {
        let Some(cell) = row.get(idx) else {
            continue;
        };
        let Some(key) = freq_bucket_key(cell) else {
            continue;
        };
        buckets
            .entry(key)
            .and_modify(|(_, c)| *c += 1)
            .or_insert_with(|| (cell.clone(), 1));
    }
    let mut v: Vec<(Value, i64)> = buckets.into_values().collect();
    v.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| value_sort_key(&a.0).cmp(&value_sort_key(&b.0)))
    });
    v.truncate(k);
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::{arg_max_row, arg_min_row, feature_wise_mean_std, top_k_by_frequency};
    use crate::processing::VarianceKind;
    use crate::types::{DataSet, DataType, Field, Schema, Value};

    #[test]
    fn feature_wise_mean_std_two_columns_one_pass() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Float64),
        ]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Int64(10), Value::Float64(1.0)],
                vec![Value::Int64(20), Value::Null],
                vec![Value::Null, Value::Float64(3.0)],
            ],
        );
        let got = feature_wise_mean_std(&ds, &["a", "b"], VarianceKind::Sample).unwrap();
        assert_eq!(got[0].0, "a");
        assert_eq!(got[0].1.mean, Value::Float64(15.0));
        let std_a = match &got[0].1.std_dev {
            Value::Float64(x) => *x,
            o => panic!("{o:?}"),
        };
        assert!((std_a - 50.0_f64.sqrt()).abs() < 1e-9);
        assert_eq!(got[1].0, "b");
        assert_eq!(got[1].1.mean, Value::Float64(2.0));
        let std_b = match &got[1].1.std_dev {
            Value::Float64(x) => *x,
            o => panic!("{o:?}"),
        };
        assert!((std_b - 2.0_f64.sqrt()).abs() < 1e-9);
    }

    #[test]
    fn feature_wise_returns_none_for_unknown_or_non_numeric_column() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64),
            Field::new("t", DataType::Utf8),
        ]);
        let ds = DataSet::new(
            schema,
            vec![vec![Value::Int64(1), Value::Utf8("x".to_string())]],
        );
        assert!(feature_wise_mean_std(&ds, &["missing"], VarianceKind::Sample).is_none());
        assert!(feature_wise_mean_std(&ds, &["a", "t"], VarianceKind::Sample).is_none());
    }

    #[test]
    fn arg_max_min_first_on_ties() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int64)]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Int64(1)],
                vec![Value::Int64(3)],
                vec![Value::Int64(3)],
                vec![Value::Null],
            ],
        );
        assert_eq!(arg_max_row(&ds, "x"), Some(Some((1, Value::Int64(3)))));
        assert_eq!(arg_min_row(&ds, "x"), Some(Some((0, Value::Int64(1)))));
    }

    #[test]
    fn top_k_frequency_ordering() {
        let schema = Schema::new(vec![Field::new("label", DataType::Utf8)]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Utf8("a".to_string())],
                vec![Value::Utf8("b".to_string())],
                vec![Value::Utf8("a".to_string())],
                vec![Value::Utf8("c".to_string())],
                vec![Value::Null],
            ],
        );
        let top = top_k_by_frequency(&ds, "label", 2).unwrap();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0], (Value::Utf8("a".to_string()), 2));
        assert_eq!(top[1].1, 1);
    }
}
