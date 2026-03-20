use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
use rust_data_processing::validation::{validate_dataset, Check, Severity, ValidationSpec};

fn make_dataset(rows: usize) -> DataSet {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
        Field::new("score", DataType::Float64),
    ]);

    let mut data = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = (i % 10_000) as i64; // duplicates on purpose
        let name = if i % 100 == 0 {
            Value::Null
        } else {
            Value::Utf8(format!("name_{i}"))
        };
        let score = Value::Float64((i as f64) * 0.01);
        data.push(vec![Value::Int64(id), name, score]);
    }
    DataSet::new(schema, data)
}

fn bench_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("validation");

    let spec = ValidationSpec::new(vec![
        Check::NotNull {
            column: "name".to_string(),
            severity: Severity::Error,
        },
        Check::RangeF64 {
            column: "score".to_string(),
            min: 0.0,
            max: 10_000.0,
            severity: Severity::Warn,
        },
        Check::Unique {
            column: "id".to_string(),
            severity: Severity::Warn,
        },
    ]);

    for &n in &[20_000usize, 100_000] {
        let ds = make_dataset(n);
        group.bench_with_input(BenchmarkId::new("validate_dataset", n), &ds, |b, ds| {
            b.iter(|| {
                let rep = validate_dataset(black_box(ds), black_box(&spec)).unwrap();
                black_box(rep.summary.failed_checks)
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_validation);
criterion_main!(benches);

