use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use rust_data_processing::outliers::{detect_outliers_dataset, OutlierMethod, OutlierOptions};
use rust_data_processing::profiling::SamplingMode;
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

fn make_dataset(rows: usize) -> DataSet {
    let schema = Schema::new(vec![Field::new("x", DataType::Float64)]);
    let mut data = Vec::with_capacity(rows);
    for i in 0..rows {
        let v = if i % 10_000 == 0 { 10_000.0 } else { (i as f64) * 0.001 };
        data.push(vec![Value::Float64(v)]);
    }
    DataSet::new(schema, data)
}

fn bench_outliers(c: &mut Criterion) {
    let mut group = c.benchmark_group("outliers");

    let opts_full = OutlierOptions {
        sampling: SamplingMode::Full,
        max_examples: 0,
    };
    let opts_head = OutlierOptions {
        sampling: SamplingMode::Head(20_000),
        max_examples: 0,
    };

    for &n in &[20_000usize, 100_000] {
        let ds = make_dataset(n);
        group.bench_with_input(BenchmarkId::new("iqr_full", n), &ds, |b, ds| {
            b.iter(|| {
                let rep = detect_outliers_dataset(
                    black_box(ds),
                    "x",
                    OutlierMethod::Iqr { k: 1.5 },
                    black_box(&opts_full),
                )
                .unwrap();
                black_box(rep.outlier_count)
            })
        });
        group.bench_with_input(BenchmarkId::new("iqr_head_20k", n), &ds, |b, ds| {
            b.iter(|| {
                let rep = detect_outliers_dataset(
                    black_box(ds),
                    "x",
                    OutlierMethod::Iqr { k: 1.5 },
                    black_box(&opts_head),
                )
                .unwrap();
                black_box(rep.outlier_count)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_outliers);
criterion_main!(benches);

