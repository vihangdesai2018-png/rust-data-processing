use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use rust_data_processing::execution::{ExecutionEngine, ExecutionOptions};
use rust_data_processing::pipeline::{Agg, DataFrame};
use rust_data_processing::processing::{
    arg_max_row, feature_wise_mean_std, reduce, top_k_by_frequency, ReduceOp, VarianceKind,
};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

fn make_dataset(rows: usize) -> DataSet {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("active", DataType::Bool),
        Field::new("score", DataType::Float64),
    ]);

    let mut data = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = i as i64;
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;
        data.push(vec![Value::Int64(id), Value::Bool(active), Value::Float64(score)]);
    }

    DataSet::new(schema, data)
}

/// Wider schema for feature-wise + group-by benchmarks (numeric + categorical key).
fn make_dataset_grouped(rows: usize) -> DataSet {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("active", DataType::Bool),
        Field::new("score", DataType::Float64),
        Field::new("aux", DataType::Float64),
        Field::new("grp", DataType::Utf8),
    ]);

    let mut data = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = i as i64;
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;
        let aux = (i as f64) * 0.03 + 1.0;
        let grp = format!("g{}", i % 8);
        data.push(vec![
            Value::Int64(id),
            Value::Bool(active),
            Value::Float64(score),
            Value::Float64(aux),
            Value::Utf8(grp),
        ]);
    }

    DataSet::new(schema, data)
}

fn bench_map_reduce(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_reduce");

    for &n in &[20_000usize, 100_000] {
        let ds = make_dataset(n);

        group.bench_with_input(BenchmarkId::new("in_memory_filter_map_reduce_sum", n), &ds, |b, ds| {
            b.iter(|| {
                let active_idx = ds.schema.index_of("active").unwrap();
                let id_idx = ds.schema.index_of("id").unwrap();

                let filtered = rust_data_processing::processing::filter(black_box(ds), |row| {
                    let is_active = matches!(row.get(active_idx), Some(Value::Bool(true)));
                    let even_id = matches!(row.get(id_idx), Some(Value::Int64(v)) if *v % 2 == 0);
                    is_active && even_id
                });
                let mapped = rust_data_processing::processing::map(black_box(&filtered), |row| {
                    let mut out = row.to_vec();
                    if let Some(Value::Float64(v)) = out.get(2) {
                        out[2] = Value::Float64(v * 1.1);
                    }
                    out
                });
                let out =
                    reduce(black_box(&mapped), black_box("score"), black_box(ReduceOp::Sum)).unwrap();
                black_box(out)
            })
        });

        let engine = ExecutionEngine::new(ExecutionOptions {
            num_threads: None,
            chunk_size: 1_024,
            max_in_flight_chunks: 4,
        });

        group.bench_with_input(
            BenchmarkId::new("engine_parallel_filter_map_reduce_sum", n),
            &ds,
            |b, ds| {
                b.iter(|| {
                    let active_idx = ds.schema.index_of("active").unwrap();
                    let id_idx = ds.schema.index_of("id").unwrap();

                    let filtered = engine.filter_parallel(black_box(ds), |row| {
                        let is_active = matches!(row.get(active_idx), Some(Value::Bool(true)));
                        let even_id = matches!(row.get(id_idx), Some(Value::Int64(v)) if *v % 2 == 0);
                        is_active && even_id
                    });
                    let mapped = engine.map_parallel(black_box(&filtered), |row| {
                        let mut out = row.to_vec();
                        if let Some(Value::Float64(v)) = out.get(2) {
                            out[2] = Value::Float64(v * 1.1);
                        }
                        out
                    });
                    let out = engine
                        .reduce(black_box(&mapped), black_box("score"), black_box(ReduceOp::Sum))
                        .unwrap();
                    black_box(out)
                })
            },
        );
    }

    group.finish();
}

fn bench_scalar_reduces(c: &mut Criterion) {
    let mut group = c.benchmark_group("reduce_scalar_ops");
    let n = 100_000usize;
    let ds = make_dataset(n);

    group.bench_function("in_memory_mean", |b| {
        b.iter(|| black_box(reduce(black_box(&ds), black_box("score"), black_box(ReduceOp::Mean)).unwrap()))
    });
    group.bench_function("in_memory_variance_sample", |b| {
        b.iter(|| {
            black_box(
                reduce(
                    black_box(&ds),
                    black_box("score"),
                    black_box(ReduceOp::Variance(VarianceKind::Sample)),
                )
                .unwrap(),
            )
        })
    });
    group.bench_function("polars_mean", |b| {
        b.iter(|| {
            black_box(
                DataFrame::from_dataset(black_box(&ds))
                    .unwrap()
                    .reduce(black_box("score"), black_box(ReduceOp::Mean))
                    .unwrap()
                    .unwrap(),
            )
        })
    });
    group.bench_function("polars_variance_sample", |b| {
        b.iter(|| {
            black_box(
                DataFrame::from_dataset(black_box(&ds))
                    .unwrap()
                    .reduce(
                        black_box("score"),
                        black_box(ReduceOp::Variance(VarianceKind::Sample)),
                    )
                    .unwrap()
                    .unwrap(),
            )
        })
    });

    group.finish();
}

fn bench_feature_wise(c: &mut Criterion) {
    let mut group = c.benchmark_group("feature_wise_mean_std");
    let cols = ["score", "aux"];
    for &n in &[50_000usize, 100_000] {
        let ds = make_dataset_grouped(n);

        group.bench_with_input(
            BenchmarkId::new("in_memory_one_pass", n),
            &ds,
            |b, ds| {
                b.iter(|| {
                    black_box(feature_wise_mean_std(
                        black_box(ds),
                        black_box(&cols[..]),
                        black_box(VarianceKind::Sample),
                    )
                    .unwrap())
                })
            },
        );

        group.bench_with_input(BenchmarkId::new("polars_single_collect", n), &ds, |b, ds| {
            b.iter(|| {
                black_box(
                    DataFrame::from_dataset(black_box(ds))
                        .unwrap()
                        .feature_wise_mean_std(black_box(&cols[..]), black_box(VarianceKind::Sample))
                        .unwrap(),
                )
            })
        });

        group.bench_with_input(
            BenchmarkId::new("in_memory_naive_per_column_reduce", n),
            &ds,
            |b, ds| {
                b.iter(|| {
                    let m1 = reduce(ds, "score", ReduceOp::Mean).unwrap();
                    let s1 = reduce(ds, "score", ReduceOp::StdDev(VarianceKind::Sample)).unwrap();
                    let m2 = reduce(ds, "aux", ReduceOp::Mean).unwrap();
                    let s2 = reduce(ds, "aux", ReduceOp::StdDev(VarianceKind::Sample)).unwrap();
                    black_box((m1, s1, m2, s2))
                })
            },
        );
    }
    group.finish();
}

fn bench_arg_topk(c: &mut Criterion) {
    let mut group = c.benchmark_group("arg_topk");
    let n = 100_000usize;
    let ds = make_dataset_grouped(n);

    group.bench_function("arg_max_row_score", |b| {
        b.iter(|| black_box(arg_max_row(black_box(&ds), black_box("score")).unwrap()))
    });

    group.bench_function("top_k_frequency_grp_k8", |b| {
        b.iter(|| black_box(top_k_by_frequency(black_box(&ds), black_box("grp"), black_box(8)).unwrap()))
    });

    group.finish();
}

fn bench_group_by_polars(c: &mut Criterion) {
    let mut group = c.benchmark_group("polars_group_by_ml_aggs");
    for &n in &[20_000usize, 100_000] {
        let ds = make_dataset_grouped(n);
        group.bench_with_input(BenchmarkId::new("mean_std_sum_count_distinct", n), &ds, |b, ds| {
            b.iter(|| {
                black_box(
                    DataFrame::from_dataset(black_box(ds))
                        .unwrap()
                        .group_by(
                            black_box(&["grp"]),
                            black_box(&[
                                Agg::Mean {
                                    column: "score".to_string(),
                                    alias: "mu_s".to_string(),
                                },
                                Agg::StdDev {
                                    column: "score".to_string(),
                                    alias: "sd_s".to_string(),
                                    kind: VarianceKind::Sample,
                                },
                                Agg::Min {
                                    column: "aux".to_string(),
                                    alias: "mn_a".to_string(),
                                },
                                Agg::Max {
                                    column: "aux".to_string(),
                                    alias: "mx_a".to_string(),
                                },
                                Agg::CountRows {
                                    alias: "n".to_string(),
                                },
                                Agg::CountDistinctNonNull {
                                    column: "id".to_string(),
                                    alias: "d_id".to_string(),
                                },
                            ]),
                        )
                        .unwrap()
                        .collect()
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_map_reduce,
    bench_scalar_reduces,
    bench_feature_wise,
    bench_arg_topk,
    bench_group_by_polars
);
criterion_main!(benches);
