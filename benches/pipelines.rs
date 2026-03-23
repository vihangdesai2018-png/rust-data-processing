use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use rust_data_processing::pipeline::{DataFrame, Predicate};
use rust_data_processing::processing::{ReduceOp, filter, map, reduce};
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
        data.push(vec![
            Value::Int64(id),
            Value::Bool(active),
            Value::Float64(score),
        ]);
    }

    DataSet::new(schema, data)
}

fn bench_pipelines(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipelines");

    for &n in &[100_000usize, 1_000_000] {
        let ds = make_dataset(n);

        group.bench_with_input(
            BenchmarkId::new("processing_filter_map_reduce_sum", n),
            &ds,
            |b, ds| {
                b.iter(|| {
                    let active_idx = ds.schema.index_of("active").unwrap();
                    let id_idx = ds.schema.index_of("id").unwrap();

                    let filtered = filter(black_box(ds), |row| {
                        let is_active = matches!(row.get(active_idx), Some(Value::Bool(true)));
                        let even_id =
                            matches!(row.get(id_idx), Some(Value::Int64(v)) if *v % 2 == 0);
                        is_active && even_id
                    });
                    let mapped = map(black_box(&filtered), |row| {
                        let mut out = row.to_vec();
                        if let Some(Value::Float64(v)) = out.get(2) {
                            out[2] = Value::Float64(v * 1.1);
                        }
                        out
                    });
                    let out = reduce(
                        black_box(&mapped),
                        black_box("score"),
                        black_box(ReduceOp::Sum),
                    )
                    .unwrap();
                    black_box(out)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("pipeline_lazy_filter_map_sum", n),
            &ds,
            |b, ds| {
                b.iter(|| {
                    let out = DataFrame::from_dataset(black_box(ds))
                        .unwrap()
                        .filter(Predicate::Eq {
                            column: "active".to_string(),
                            value: Value::Bool(true),
                        })
                        .unwrap()
                        .filter(Predicate::ModEqInt64 {
                            column: "id".to_string(),
                            modulus: 2,
                            equals: 0,
                        })
                        .unwrap()
                        .multiply_f64("score", 1.1)
                        .unwrap()
                        .sum("score")
                        .unwrap()
                        .unwrap();
                    black_box(out)
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_pipelines);
criterion_main!(benches);
