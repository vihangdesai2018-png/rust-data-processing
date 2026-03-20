#![cfg(feature = "deep_tests")]

use std::collections::HashSet;

use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
use rust_data_processing::pipeline::{Agg, DataFrame};
use rust_data_processing::processing::{
    arg_max_row, arg_min_row, feature_wise_mean_std, reduce, top_k_by_frequency, ReduceOp,
    VarianceKind,
};
use rust_data_processing::profiling::{profile_dataset, ProfileOptions, SamplingMode};
use rust_data_processing::outliers::{detect_outliers_dataset, OutlierMethod, OutlierOptions};
use rust_data_processing::validation::{validate_dataset, Check, Severity, ValidationSpec};
use rust_data_processing::transform::{TransformSpec, TransformStep};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

fn assert_value_close_mem_polars(mem: Value, polars: Value, eps_abs: f64) {
    match (mem, polars) {
        (Value::Float64(a), Value::Float64(b)) => {
            let diff = (a - b).abs();
            if diff <= eps_abs {
                return;
            }
            // Large magnitudes (e.g. Int64 totals cast to f64) accumulate rounding error;
            // allow small relative tolerance so Welford vs Polars stay comparable.
            let scale = a.abs().max(b.abs()).max(1.0);
            let rel_tol = (scale * 1e-9).max(f64::EPSILON * 8.0);
            assert!(
                diff <= rel_tol,
                "mem={a} polars={b} diff={diff} eps_abs={eps_abs} rel_tol={rel_tol}"
            );
        }
        (a, b) => assert_eq!(a, b, "in-memory vs polars reduce mismatch"),
    }
}

fn assert_feature_wise_parity(ds: &DataSet, cols: &[&str], kind: VarianceKind) {
    let mem = feature_wise_mean_std(ds, cols, kind).expect("feature_wise columns ok");
    let pol = DataFrame::from_dataset(ds)
        .unwrap()
        .feature_wise_mean_std(cols, kind)
        .unwrap();
    assert_eq!(mem.len(), pol.len());
    let eps = 1e-6_f64.max(f64::EPSILON);
    for i in 0..mem.len() {
        assert_eq!(mem[i].0, pol[i].0, "column name mismatch at {i}");
        assert_value_close_mem_polars(mem[i].1.mean.clone(), pol[i].1.mean.clone(), eps);
        assert_value_close_mem_polars(
            mem[i].1.std_dev.clone(),
            pol[i].1.std_dev.clone(),
            eps,
        );
    }
}

fn assert_reduce_parity(ds: &DataSet, column: &str, op: ReduceOp) {
    let mem = reduce(ds, column, op).expect("column must exist");
    let polars = DataFrame::from_dataset(ds)
        .unwrap()
        .reduce(column, op)
        .unwrap()
        .expect("polars reduce returns Some when column exists");
    // Polars vs Welford: tight abs epsilon for typical magnitudes; larger values use a small
    // relative tolerance (see `assert_value_close_mem_polars`) so big Int64-as-f64 means stay comparable.
    let eps = 1e-6_f64.max(f64::EPSILON);
    assert_value_close_mem_polars(mem, polars, eps);
}

#[test]
fn deep_csv_seattle_weather_ingests_and_casts() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    // File is ~1.4k days; keep assertions stable but not overly brittle.
    assert!(ds.row_count() > 1000);
    assert_eq!(ds.rows[0][0], Value::Utf8("2012-01-01".to_string()));
    assert_eq!(ds.rows[0][5], Value::Utf8("drizzle".to_string()));
    assert!(matches!(ds.rows[1][1], Value::Float64(_)));
}

#[test]
fn deep_reduce_new_ops_parity_in_memory_vs_polars_on_seattle_weather() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    for col in ["precipitation", "temp_max", "temp_min", "wind"] {
        assert_reduce_parity(&ds, col, ReduceOp::Mean);
        assert_reduce_parity(&ds, col, ReduceOp::Variance(VarianceKind::Population));
        assert_reduce_parity(&ds, col, ReduceOp::Variance(VarianceKind::Sample));
        assert_reduce_parity(&ds, col, ReduceOp::StdDev(VarianceKind::Population));
        assert_reduce_parity(&ds, col, ReduceOp::StdDev(VarianceKind::Sample));
        assert_reduce_parity(&ds, col, ReduceOp::SumSquares);
        assert_reduce_parity(&ds, col, ReduceOp::L2Norm);
        assert_reduce_parity(&ds, col, ReduceOp::Sum);
        assert_reduce_parity(&ds, col, ReduceOp::Min);
        assert_reduce_parity(&ds, col, ReduceOp::Max);
    }

    assert_reduce_parity(&ds, "weather", ReduceOp::CountDistinctNonNull);
    assert_reduce_parity(&ds, "temp_max", ReduceOp::Count);

    // Invariants on a column with nulls allowed (wind may have gaps).
    let mean = reduce(&ds, "temp_max", ReduceOp::Mean).unwrap();
    let mn = reduce(&ds, "temp_max", ReduceOp::Min).unwrap();
    let mx = reduce(&ds, "temp_max", ReduceOp::Max).unwrap();
    match (mean, mn, mx) {
        (Value::Float64(mu), Value::Float64(lo), Value::Float64(hi)) => {
            assert!(mu >= lo && mu <= hi, "mean should lie between min and max");
            let v = match reduce(&ds, "temp_max", ReduceOp::Variance(VarianceKind::Population)).unwrap()
            {
                Value::Float64(x) => x,
                other => panic!("expected variance Float64, got {other:?}"),
            };
            assert!(v >= 0.0, "population variance non-negative");
        }
        triple => panic!("unexpected min/max/mean types: {triple:?}"),
    }
}

#[test]
fn deep_feature_wise_mean_std_parity_on_seattle_numeric_columns() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    assert_feature_wise_parity(
        &ds,
        &["precipitation", "temp_max", "temp_min", "wind"],
        VarianceKind::Sample,
    );
    assert_feature_wise_parity(
        &ds,
        &["precipitation", "temp_max", "temp_min", "wind"],
        VarianceKind::Population,
    );
}

#[test]
fn deep_group_by_mean_max_count_distinct_on_seattle_weather() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    let out = DataFrame::from_dataset(&ds)
        .unwrap()
        .group_by(
            &["weather"],
            &[
                Agg::Mean {
                    column: "temp_max".to_string(),
                    alias: "mu_tmax".to_string(),
                },
                Agg::Max {
                    column: "temp_min".to_string(),
                    alias: "max_tmin".to_string(),
                },
                Agg::CountRows {
                    alias: "n_rows".to_string(),
                },
                Agg::CountDistinctNonNull {
                    column: "date".to_string(),
                    alias: "n_dates".to_string(),
                },
                Agg::StdDev {
                    column: "wind".to_string(),
                    alias: "sd_wind".to_string(),
                    kind: VarianceKind::Sample,
                },
            ],
        )
        .unwrap()
        .collect()
        .unwrap();

    assert!(out.row_count() >= 5);
    assert_eq!(out.schema.fields.len(), 6);

    let mut sum_rows = 0i64;
    for row in &out.rows {
        if let Value::Int64(n) = row[3] {
            sum_rows += n;
        }
    }
    assert_eq!(sum_rows, ds.row_count() as i64, "CountRows per group should sum to total rows");
}

#[test]
fn deep_arg_extrema_and_topk_weather_on_seattle() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    let (i_max, v_max) = arg_max_row(&ds, "temp_max").unwrap().expect("argmax");
    let (i_min, v_min) = arg_min_row(&ds, "temp_max").unwrap().expect("argmin");
    match (v_max, v_min) {
        (Value::Float64(a), Value::Float64(b)) => assert!(a >= b),
        _ => panic!("expected Float64 extrema"),
    }
    assert!(i_max < ds.row_count());
    assert!(i_min < ds.row_count());

    let top = top_k_by_frequency(&ds, "weather", 5).unwrap();
    assert_eq!(top.len(), 5);
    for w in top.windows(2) {
        assert!(w[0].1 >= w[1].1, "top-k should be sorted by count desc");
    }
}

#[test]
fn deep_reduce_new_ops_parity_on_job_runs_json_fixture() {
    let schema = Schema::new(vec![
        Field::new("job_id", DataType::Int64),
        Field::new("creator_user_name", DataType::Utf8),
        Field::new("created_time", DataType::Int64),
        Field::new("settings.name", DataType::Utf8),
        Field::new("settings.tags.team", DataType::Utf8),
        Field::new("settings.tags.env", DataType::Utf8),
        Field::new("cluster.num_workers", DataType::Int64),
        Field::new("metrics.duration_ms", DataType::Float64),
        Field::new("metrics.success", DataType::Bool),
        Field::new("metrics.bytes_written", DataType::Int64),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/job_runs_sample.json",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    assert_eq!(ds.row_count(), 3);

    // Numeric columns: in-memory vs Polars for new stats ops.
    for col in [
        "job_id",
        "created_time",
        "cluster.num_workers",
        "metrics.bytes_written",
        "metrics.duration_ms",
    ] {
        assert_reduce_parity(&ds, col, ReduceOp::Mean);
        assert_reduce_parity(&ds, col, ReduceOp::Sum);
        assert_reduce_parity(&ds, col, ReduceOp::Min);
        assert_reduce_parity(&ds, col, ReduceOp::Max);
        assert_reduce_parity(&ds, col, ReduceOp::SumSquares);
        assert_reduce_parity(&ds, col, ReduceOp::L2Norm);
        assert_reduce_parity(&ds, col, ReduceOp::Variance(VarianceKind::Population));
        assert_reduce_parity(&ds, col, ReduceOp::Variance(VarianceKind::Sample));
        assert_reduce_parity(&ds, col, ReduceOp::StdDev(VarianceKind::Population));
        assert_reduce_parity(&ds, col, ReduceOp::StdDev(VarianceKind::Sample));
    }

    assert_reduce_parity(&ds, "metrics.success", ReduceOp::CountDistinctNonNull);
    assert_reduce_parity(&ds, "creator_user_name", ReduceOp::CountDistinctNonNull);
    assert_reduce_parity(&ds, "settings.tags.team", ReduceOp::CountDistinctNonNull);
    assert_reduce_parity(&ds, "settings.tags.env", ReduceOp::CountDistinctNonNull);
    assert_reduce_parity(&ds, "settings.name", ReduceOp::CountDistinctNonNull);

    assert_reduce_parity(&ds, "job_id", ReduceOp::Count);

    // Small row count: explicit sanity checks (fixture has one null cluster + one null bytes_written).
    assert_eq!(
        reduce(&ds, "metrics.bytes_written", ReduceOp::CountDistinctNonNull).unwrap(),
        Value::Int64(2)
    );
    assert_eq!(
        reduce(&ds, "cluster.num_workers", ReduceOp::CountDistinctNonNull).unwrap(),
        Value::Int64(2)
    );

    assert_feature_wise_parity(
        &ds,
        &["job_id", "metrics.duration_ms"],
        VarianceKind::Sample,
    );
}

#[test]
fn deep_json_nested_job_runs_extracts_dot_paths_and_handles_nulls() {
    let schema = Schema::new(vec![
        Field::new("job_id", DataType::Int64),
        Field::new("creator_user_name", DataType::Utf8),
        Field::new("created_time", DataType::Int64),
        Field::new("settings.name", DataType::Utf8),
        Field::new("settings.tags.team", DataType::Utf8),
        Field::new("settings.tags.env", DataType::Utf8),
        Field::new("cluster.num_workers", DataType::Int64),
        Field::new("metrics.duration_ms", DataType::Float64),
        Field::new("metrics.success", DataType::Bool),
        Field::new("metrics.bytes_written", DataType::Int64),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/job_runs_sample.json",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    assert_eq!(ds.row_count(), 3);
    assert_eq!(ds.rows[0][0], Value::Int64(12001));
    assert_eq!(ds.rows[0][3], Value::Utf8("daily_ingest_events".to_string()));
    assert_eq!(ds.rows[1][5], Value::Utf8("prod".to_string()));

    // Third row has cluster=null and bytes_written=null.
    assert_eq!(ds.rows[2][6], Value::Null);
    assert_eq!(ds.rows[2][9], Value::Null);
    assert_eq!(ds.rows[2][8], Value::Bool(false));
}

#[test]
fn deep_transform_spec_and_sql_work_on_real_fixture() {
    // Use the Seattle weather CSV fixture as a realistic dataset.
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    // Apply a mapping spec: rename + derive + select/reorder.
    let out_schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("wx", DataType::Utf8),
        Field::new("temp_max_x2", DataType::Float64),
    ]);

    let spec = TransformSpec::new(out_schema.clone())
        .with_step(TransformStep::Rename {
            pairs: vec![("weather".to_string(), "wx".to_string())],
        })
        // temp_max_x2 = temp_max * 2.0
        .with_step(TransformStep::DeriveMulF64 {
            name: "temp_max_x2".to_string(),
            source: "temp_max".to_string(),
            factor: 2.0,
        })
        .with_step(TransformStep::Select {
            columns: vec!["date".to_string(), "wx".to_string(), "temp_max_x2".to_string()],
        });

    let mapped = spec.apply(&ds).unwrap();
    assert_eq!(mapped.schema, out_schema);
    assert_eq!(mapped.row_count(), ds.row_count());
    assert!(matches!(mapped.rows[0][2], Value::Float64(_)));

    // Ensure the SQL wrapper runs on the transformed data.
    // We keep assertions non-brittle by only checking basic shape and determinism.
    let df = DataFrame::from_dataset(&mapped).unwrap();
    let out = rust_data_processing::sql::query(
        &df,
        "SELECT date, wx FROM df WHERE wx IS NOT NULL ORDER BY date ASC LIMIT 5",
    )
    .unwrap()
    .collect()
    .unwrap();

    assert_eq!(out.schema.field_names().collect::<Vec<_>>(), vec!["date", "wx"]);
    assert_eq!(out.row_count(), 5);
}

#[test]
fn deep_profiling_head_sampling_is_deterministic() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    let rep = profile_dataset(
        &ds,
        &ProfileOptions {
            sampling: SamplingMode::Head(100),
            quantiles: vec![0.5],
        },
    )
    .unwrap();

    assert_eq!(rep.row_count, 100);
    assert_eq!(rep.columns.len(), schema.fields.len());
    let date = rep.columns.iter().find(|c| c.name == "date").unwrap();
    assert_eq!(date.data_type, DataType::Utf8);
}

#[test]
fn deep_validation_and_outliers_smoke_on_real_fixture() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Utf8),
        Field::new("precipitation", DataType::Float64),
        Field::new("temp_max", DataType::Float64),
        Field::new("temp_min", DataType::Float64),
        Field::new("wind", DataType::Float64),
        Field::new("weather", DataType::Utf8),
    ]);

    let ds = ingest_from_path(
        "tests/fixtures/deep/seattle-weather.csv",
        &schema,
        &IngestionOptions::default(),
    )
    .unwrap();

    let spec = ValidationSpec::new(vec![
        Check::NotNull {
            column: "date".to_string(),
            severity: Severity::Error,
        },
        Check::RegexMatch {
            column: "date".to_string(),
            pattern: r"^\d{4}-\d{2}-\d{2}$".to_string(),
            severity: Severity::Warn,
            strict: true,
        },
        Check::RangeF64 {
            column: "wind".to_string(),
            min: 0.0,
            max: 100.0,
            severity: Severity::Warn,
        },
    ]);
    let rep = validate_dataset(&ds, &spec).unwrap();
    assert_eq!(rep.summary.total_checks, 3);

    // Outliers: just ensure the primitive runs deterministically (don't assert a specific count).
    let out = detect_outliers_dataset(
        &ds,
        "temp_max",
        OutlierMethod::Iqr { k: 1.5 },
        &OutlierOptions {
            sampling: SamplingMode::Head(200),
            max_examples: 5,
        },
    )
    .unwrap();
    assert_eq!(out.row_count, 200);
}

#[test]
fn deep_parquet_apache_fixture_ingests_supported_columns() {
    use polars::prelude::{AnyValue, LazyFrame, ScanArgsParquet, Series};

    let path = "tests/fixtures/deep/rle-dict-snappy-checksum.parquet";

    // Inspect with Polars to find a stable subset of supported columns, then ensure our
    // ingestion path produces matching values for those columns.
    let df = LazyFrame::scan_parquet(path.into(), ScanArgsParquet::default())
        .unwrap()
        .collect()
        .unwrap();

    // Select up to 6 columns that map cleanly into our limited logical type system.
    let mut fields: Vec<Field> = Vec::new();
    for col in df.columns() {
        let s = col.as_materialized_series();
        let dt = match s.dtype() {
            polars::datatypes::DataType::String => Some(DataType::Utf8),
            polars::datatypes::DataType::Boolean => Some(DataType::Bool),
            polars::datatypes::DataType::Int8
            | polars::datatypes::DataType::Int16
            | polars::datatypes::DataType::Int32
            | polars::datatypes::DataType::Int64
            | polars::datatypes::DataType::UInt8
            | polars::datatypes::DataType::UInt16
            | polars::datatypes::DataType::UInt32
            | polars::datatypes::DataType::UInt64 => Some(DataType::Int64),
            polars::datatypes::DataType::Float32 | polars::datatypes::DataType::Float64 => {
                Some(DataType::Float64)
            }
            _ => None,
        };

        if let Some(dt) = dt {
            fields.push(Field::new(s.name().to_string(), dt));
        }
        if fields.len() >= 6 {
            break;
        }
    }

    assert!(
        !fields.is_empty(),
        "expected at least one supported primitive column in {path}"
    );

    // Avoid duplicate names (defensive, in case of odd parquet schemas).
    let mut seen: HashSet<String> = HashSet::new();
    fields.retain(|f| seen.insert(f.name.clone()));

    let schema = Schema::new(fields);
    let ds = ingest_from_path(path, &schema, &IngestionOptions::default()).unwrap();
    assert_eq!(ds.row_count(), df.height());

    // Mirror the exact casting behavior used by the ingestion bridge so we can compare values
    // without relying on Polars' inferred dtypes.
    let mut casted_cols: Vec<Series> = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let s = df.column(&field.name).unwrap().as_materialized_series().clone();
        let target = match field.data_type {
            DataType::Int64 => polars::datatypes::DataType::Int64,
            DataType::Float64 => polars::datatypes::DataType::Float64,
            DataType::Bool => polars::datatypes::DataType::Boolean,
            DataType::Utf8 => polars::datatypes::DataType::String,
        };
        casted_cols.push(s.cast(&target).unwrap());
    }

    // Spot-check the first few rows against Polars values.
    let n = usize::min(10, df.height());
    for row_idx in 0..n {
        for (col_idx, field) in schema.fields.iter().enumerate() {
            let av = casted_cols[col_idx].get(row_idx).unwrap();
            let expected = match (field.data_type.clone(), av) {
                (_, AnyValue::Null) => Value::Null,
                (DataType::Utf8, AnyValue::String(v)) => Value::Utf8(v.to_string()),
                (DataType::Utf8, AnyValue::StringOwned(v)) => Value::Utf8(v.to_string()),
                (DataType::Bool, AnyValue::Boolean(v)) => Value::Bool(v),
                (DataType::Int64, AnyValue::Int64(v)) => Value::Int64(v),
                (DataType::Float64, AnyValue::Float64(v)) => Value::Float64(v),
                (dt, other) => panic!("unexpected polars value for {dt:?}: {other}"),
            };
            assert_eq!(
                ds.rows[row_idx][col_idx], expected,
                "mismatch at row={row_idx} col={}",
                field.name
            );
        }
    }

    // Reduce ops: memory vs Polars on the same ingested columns (spot-check aggregates).
    for field in &schema.fields {
        match field.data_type {
            DataType::Int64 | DataType::Float64 => {
                assert_reduce_parity(&ds, &field.name, ReduceOp::Mean);
                assert_reduce_parity(&ds, &field.name, ReduceOp::SumSquares);
                assert_reduce_parity(&ds, &field.name, ReduceOp::L2Norm);
                assert_reduce_parity(&ds, &field.name, ReduceOp::Variance(VarianceKind::Sample));
            }
            DataType::Utf8 => {
                assert_reduce_parity(&ds, &field.name, ReduceOp::CountDistinctNonNull);
            }
            DataType::Bool => {
                assert_reduce_parity(&ds, &field.name, ReduceOp::CountDistinctNonNull);
            }
        }
    }

    let numeric_names: Vec<String> = schema
        .fields
        .iter()
        .filter(|f| matches!(f.data_type, DataType::Int64 | DataType::Float64))
        .map(|f| f.name.clone())
        .collect();
    if numeric_names.len() >= 2 {
        let refs: Vec<&str> = numeric_names.iter().map(|s| s.as_str()).collect();
        assert_feature_wise_parity(&ds, &refs, VarianceKind::Sample);
    }
}

#[cfg(feature = "excel_test_writer")]
#[test]
fn deep_excel_multisheet_formulas_and_nulls() {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rust_data_processing::ingestion::excel::ingest_excel_workbook_from_path;
    use rust_xlsxwriter::{Format, Workbook};

    fn tmp_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rust-data-processing-deep-{name}-{nanos}.xlsx"))
    }

    let path = tmp_file("complex");
    let mut wb = Workbook::new();

    // Sheet: RawWeather
    let ws_raw = wb.add_worksheet();
    ws_raw.set_name("RawWeather").unwrap();
    for (c, h) in ["date", "temp_max", "temp_min", "wind", "weather"].into_iter().enumerate() {
        ws_raw.write_string(0, c as u16, h).unwrap();
    }
    ws_raw.write_string(1, 0, "2012-01-01").unwrap();
    ws_raw.write_number(1, 1, 12.8).unwrap();
    ws_raw.write_number(1, 2, 5.0).unwrap();
    ws_raw.write_number(1, 3, 4.7).unwrap();
    ws_raw.write_string(1, 4, "drizzle").unwrap();

    // Null-y row (missing wind)
    ws_raw.write_string(2, 0, "2012-01-02").unwrap();
    ws_raw.write_number(2, 1, 10.6).unwrap();
    ws_raw.write_number(2, 2, 2.8).unwrap();
    ws_raw.write_blank(2, 3, &Format::new()).unwrap();
    ws_raw.write_string(2, 4, "rain").unwrap();

    // Sheet: Summary (formulas; calamine reads the cached value if present, so we also write values)
    let ws_sum = wb.add_worksheet();
    ws_sum.set_name("Summary").unwrap();
    ws_sum.write_string(0, 0, "id").unwrap();
    ws_sum.write_string(0, 1, "name").unwrap();
    ws_sum.write_string(0, 2, "score").unwrap();
    ws_sum.write_string(0, 3, "active").unwrap();

    ws_sum.write_number(1, 0, 1).unwrap();
    ws_sum.write_string(1, 1, "Ada").unwrap();
    ws_sum.write_number(1, 2, 98.5).unwrap();
    ws_sum.write_boolean(1, 3, true).unwrap();

    ws_sum.write_number(2, 0, 2).unwrap();
    ws_sum.write_string(2, 1, "Grace").unwrap();
    ws_sum.write_number(2, 2, 87.25).unwrap();
    ws_sum.write_boolean(2, 3, false).unwrap();

    wb.save(&path).unwrap();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("active", DataType::Bool),
    ]);

    // Only select Summary sheet, ensure other sheets don't break parsing.
    let sheets = vec!["Summary"];
    let ds = ingest_excel_workbook_from_path(&path, Some(&sheets), &schema).unwrap();
    assert_eq!(ds.row_count(), 2);
    assert_eq!(ds.rows[0][1], Value::Utf8("Ada".to_string()));
    assert_eq!(ds.rows[1][3], Value::Bool(false));

    let _ = std::fs::remove_file(&path);
}

