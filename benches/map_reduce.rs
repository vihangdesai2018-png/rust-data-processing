use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use rust_data_processing::execution::{ExecutionEngine, ExecutionOptions};
use rust_data_processing::processing::{filter, map, reduce, ReduceOp};
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

fn bench_map_reduce(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_reduce");

    for &n in &[20_000usize, 100_000] {
        let ds = make_dataset(n);

        // Single-threaded, in-memory.
        group.bench_with_input(BenchmarkId::new("in_memory_filter_map_reduce_sum", n), &ds, |b, ds| {
            b.iter(|| {
                let active_idx = ds.schema.index_of("active").unwrap();
                let id_idx = ds.schema.index_of("id").unwrap();

                let filtered = filter(black_box(ds), |row| {
                    let is_active = matches!(row.get(active_idx), Some(Value::Bool(true)));
                    let even_id = matches!(row.get(id_idx), Some(Value::Int64(v)) if *v % 2 == 0);
                    is_active && even_id
                });
                let mapped = map(black_box(&filtered), |row| {
                    let mut out = row.to_vec();
                    if let Some(Value::Float64(v)) = out.get(2) {
                        out[2] = Value::Float64(v * 1.1);
                    }
                    out
                });
                let out = reduce(black_box(&mapped), black_box("score"), black_box(ReduceOp::Sum)).unwrap();
                black_box(out)
            })
        });

        // Multi-threaded execution engine path.
        group.bench_with_input(BenchmarkId::new("engine_parallel_filter_map_reduce_sum", n), &ds, |b, ds| {
            let engine = ExecutionEngine::new(ExecutionOptions {
                num_threads: None,
                chunk_size: 1_024,
                max_in_flight_chunks: 4,
            });

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
                let out = engine.reduce(black_box(&mapped), black_box("score"), black_box(ReduceOp::Sum)).unwrap();
                black_box(out)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_map_reduce);
criterion_main!(benches);

