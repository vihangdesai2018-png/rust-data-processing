use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use rust_data_processing::profiling::{ProfileOptions, SamplingMode, profile_dataset};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

fn make_dataset(rows: usize) -> DataSet {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("active", DataType::Bool),
        Field::new("score", DataType::Float64),
        Field::new("name", DataType::Utf8),
    ]);

    let mut data = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = i as i64;
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;
        let name = format!("name_{}", i % 1_000);
        data.push(vec![
            Value::Int64(id),
            Value::Bool(active),
            Value::Float64(score),
            Value::Utf8(name),
        ]);
    }

    DataSet::new(schema, data)
}

fn bench_profiling(c: &mut Criterion) {
    let mut group = c.benchmark_group("profiling");

    let opts_full = ProfileOptions::default();
    let opts_head = ProfileOptions {
        sampling: SamplingMode::Head(10_000),
        quantiles: vec![0.5, 0.95],
    };

    for &n in &[20_000usize, 100_000] {
        let ds = make_dataset(n);

        group.bench_with_input(BenchmarkId::new("profile_full", n), &ds, |b, ds| {
            b.iter(|| {
                let rep = profile_dataset(black_box(ds), black_box(&opts_full)).unwrap();
                black_box(rep.row_count)
            })
        });

        group.bench_with_input(BenchmarkId::new("profile_head_10k", n), &ds, |b, ds| {
            b.iter(|| {
                let rep = profile_dataset(black_box(ds), black_box(&opts_head)).unwrap();
                black_box(rep.row_count)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_profiling);
criterion_main!(benches);
