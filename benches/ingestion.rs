use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

use rust_data_processing::ingestion::{ingest_from_path, infer_schema_from_path, IngestionOptions};
use rust_data_processing::processing::{reduce, ReduceOp};
use rust_data_processing::types::{DataType, Field, Schema};

use polars::prelude::{Column, DataFrame, NamedFrom, ParquetWriter, Series};
use rust_xlsxwriter::Workbook;

const N_ROWS: usize = 20_000;
const ROTATING_COPIES: usize = 64;

struct Fixtures {
    csv: PathBuf,
    csv_rotating: Vec<PathBuf>,
    json_array: PathBuf,
    json_array_rotating: Vec<PathBuf>,
    ndjson: PathBuf,
    ndjson_rotating: Vec<PathBuf>,
    json_nested: PathBuf,
    json_nested_rotating: Vec<PathBuf>,
    parquet: PathBuf,
    parquet_rotating: Vec<PathBuf>,
    xlsx: PathBuf,
    xlsx_rotating: Vec<PathBuf>,
}

fn fixtures() -> &'static Fixtures {
    static FIX: OnceLock<Fixtures> = OnceLock::new();
    FIX.get_or_init(|| build_fixtures().expect("failed to build benchmark fixtures"))
}

fn build_fixtures() -> std::io::Result<Fixtures> {
    let dir = PathBuf::from("target/bench-fixtures/ingestion_20k");
    fs::create_dir_all(&dir)?;

    let csv = dir.join("data_20000.csv");
    let json_array = dir.join("data_20000.json");
    let ndjson = dir.join("data_20000.ndjson");
    let json_nested = dir.join("nested_20000.json");
    let parquet = dir.join("data_20000.parquet");
    let xlsx = dir.join("data_20000.xlsx");

    if !csv.exists() {
        write_csv(&csv, N_ROWS)?;
    }
    if !json_array.exists() {
        write_json_array(&json_array, N_ROWS)?;
    }
    if !ndjson.exists() {
        write_ndjson(&ndjson, N_ROWS)?;
    }
    if !json_nested.exists() {
        write_json_nested_array(&json_nested, N_ROWS)?;
    }
    if !parquet.exists() {
        write_parquet(&parquet, N_ROWS)?;
    }
    if !xlsx.exists() {
        write_xlsx(&xlsx, N_ROWS)?;
    }

    let csv_rotating = ensure_copies(&csv, &dir, "data_20000_copy", "csv", ROTATING_COPIES)?;
    let json_array_rotating = ensure_copies(&json_array, &dir, "data_20000_copy", "json", ROTATING_COPIES)?;
    let ndjson_rotating = ensure_copies(&ndjson, &dir, "data_20000_copy", "ndjson", ROTATING_COPIES)?;
    let json_nested_rotating =
        ensure_copies(&json_nested, &dir, "nested_20000_copy", "json", ROTATING_COPIES)?;
    let parquet_rotating =
        ensure_copies(&parquet, &dir, "data_20000_copy", "parquet", ROTATING_COPIES)?;
    let xlsx_rotating = ensure_copies(&xlsx, &dir, "data_20000_copy", "xlsx", ROTATING_COPIES)?;

    Ok(Fixtures {
        csv,
        csv_rotating,
        json_array,
        json_array_rotating,
        ndjson,
        ndjson_rotating,
        json_nested,
        json_nested_rotating,
        parquet,
        parquet_rotating,
        xlsx,
        xlsx_rotating,
    })
}

fn ensure_copies(src: &Path, dir: &Path, stem: &str, ext: &str, n: usize) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let p = dir.join(format!("{stem}_{i:02}.{ext}"));
        if !p.exists() {
            fs::copy(src, &p)?;
        }
        out.push(p);
    }
    Ok(out)
}

fn write_csv(path: &Path, n: usize) -> std::io::Result<()> {
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    writeln!(w, "id,active,score,name")?;
    for i in 0..n {
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;
        writeln!(w, "{i},{active},{score},name_{i}")?;
    }
    w.flush()?;
    Ok(())
}

fn write_json_array(path: &Path, n: usize) -> std::io::Result<()> {
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    writeln!(w, "[")?;
    for i in 0..n {
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;
        let sep = if i + 1 == n { "" } else { "," };
        writeln!(
            w,
            "{{\"id\":{i},\"active\":{},\"score\":{},\"name\":\"name_{i}\"}}{sep}",
            if active { "true" } else { "false" },
            score
        )?;
    }
    writeln!(w, "]")?;
    w.flush()?;
    Ok(())
}

fn write_ndjson(path: &Path, n: usize) -> std::io::Result<()> {
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    for i in 0..n {
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;
        writeln!(
            w,
            "{{\"id\":{i},\"active\":{},\"score\":{},\"name\":\"name_{i}\"}}",
            if active { "true" } else { "false" },
            score
        )?;
    }
    w.flush()?;
    Ok(())
}

fn write_json_nested_array(path: &Path, n: usize) -> std::io::Result<()> {
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    writeln!(w, "[")?;
    for i in 0..n {
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;
        let sep = if i + 1 == n { "" } else { "," };
        writeln!(
            w,
            "{{\"id\":{i},\"user\":{{\"name\":\"name_{i}\",\"active\":{}}},\"score\":{}}}{sep}",
            if active { "true" } else { "false" },
            score
        )?;
    }
    writeln!(w, "]")?;
    w.flush()?;
    Ok(())
}

fn write_parquet(path: &Path, n: usize) -> std::io::Result<()> {
    let ids: Vec<i64> = (0..n as i64).collect();
    let active: Vec<bool> = (0..n).map(|i| (i % 3) != 0).collect();
    let score: Vec<f64> = (0..n).map(|i| (i as f64) * 0.1).collect();
    let names: Vec<String> = (0..n).map(|i| format!("name_{i}")).collect();

    let s_id = Series::new("id".into(), ids);
    let s_active = Series::new("active".into(), active);
    let s_score = Series::new("score".into(), score);
    let s_name = Series::new("name".into(), names);

    let cols: Vec<Column> = vec![s_id.into(), s_active.into(), s_score.into(), s_name.into()];
    let mut df = DataFrame::new(n, cols)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    let mut f = File::create(path)?;
    ParquetWriter::new(&mut f)
        .finish(&mut df)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    Ok(())
}

fn write_xlsx(path: &Path, n: usize) -> std::io::Result<()> {
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    ws.set_name("Sheet1")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    ws.write_string(0, 0, "id")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    ws.write_string(0, 1, "active")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    ws.write_string(0, 2, "score")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    ws.write_string(0, 3, "name")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

    for i in 0..n {
        let row = (i + 1) as u32;
        let active = (i % 3) != 0;
        let score = (i as f64) * 0.1;

        ws.write_number(row, 0, i as f64)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        ws.write_boolean(row, 1, active)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        ws.write_number(row, 2, score)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        ws.write_string(row, 3, &format!("name_{i}"))
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    }

    wb.save(path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    Ok(())
}

fn schema_flat() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("active", DataType::Bool),
        Field::new("score", DataType::Float64),
        Field::new("name", DataType::Utf8),
    ])
}

fn schema_nested() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("user.name", DataType::Utf8),
        Field::new("user.active", DataType::Bool),
        Field::new("score", DataType::Float64),
    ])
}

fn bench_ingestion(c: &mut Criterion) {
    let fx = fixtures();
    let opts = IngestionOptions::default();

    let mut group = c.benchmark_group("ingestion_20k");

    // Helper to compare schema-known vs schema-inferred, and warm vs rotating files.
    fn bench_case(
        group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
        id: &str,
        warm_path: &Path,
        rotating_paths: &[PathBuf],
        schema_known: Option<Schema>,
        opts: &IngestionOptions,
    ) {
        static ROT_IDX: AtomicUsize = AtomicUsize::new(0);

        // "Warm": same path each iteration (OS cache likely warm after first).
        group.bench_function(BenchmarkId::new(id, "warm_known_schema"), |b| {
            let schema = schema_known.as_ref().expect("schema required for known_schema");
            b.iter(|| {
                let ds = ingest_from_path(black_box(warm_path), black_box(schema), black_box(opts)).unwrap();
                black_box(ds)
            })
        });

        group.bench_function(BenchmarkId::new(id, "warm_infer_schema"), |b| {
            b.iter(|| {
                let schema = infer_schema_from_path(black_box(warm_path), black_box(opts)).unwrap();
                let ds = ingest_from_path(black_box(warm_path), black_box(&schema), black_box(opts)).unwrap();
                black_box(ds)
            })
        });

        // "Cold-like": rotate across many identical copies to reduce repeated-file locality.
        group.bench_function(BenchmarkId::new(id, "rotating_known_schema"), |b| {
            let schema = schema_known.as_ref().expect("schema required for known_schema");
            b.iter_batched(
                || {
                    let i = ROT_IDX.fetch_add(1, Ordering::Relaxed) % rotating_paths.len();
                    rotating_paths[i].clone()
                },
                |p| {
                    let ds = ingest_from_path(black_box(p), black_box(schema), black_box(opts)).unwrap();
                    black_box(ds)
                },
                BatchSize::SmallInput,
            )
        });

        group.bench_function(BenchmarkId::new(id, "rotating_infer_schema"), |b| {
            b.iter_batched(
                || {
                    let i = ROT_IDX.fetch_add(1, Ordering::Relaxed) % rotating_paths.len();
                    rotating_paths[i].clone()
                },
                |p| {
                    let schema = infer_schema_from_path(black_box(&p), black_box(opts)).unwrap();
                    let ds = ingest_from_path(black_box(p), black_box(&schema), black_box(opts)).unwrap();
                    black_box(ds)
                },
                BatchSize::SmallInput,
            )
        });
    }

    bench_case(
        &mut group,
        "csv",
        &fx.csv,
        &fx.csv_rotating,
        Some(schema_flat()),
        &opts,
    );
    bench_case(
        &mut group,
        "json_array",
        &fx.json_array,
        &fx.json_array_rotating,
        Some(schema_flat()),
        &opts,
    );
    bench_case(
        &mut group,
        "ndjson",
        &fx.ndjson,
        &fx.ndjson_rotating,
        Some(schema_flat()),
        &opts,
    );

    // Nested JSON: schema inference only sees top-level fields; keep this as schema-known only.
    group.bench_function(BenchmarkId::new("json_nested", "warm_known_schema"), |b| {
        let schema = schema_nested();
        b.iter(|| {
            let ds = ingest_from_path(black_box(&fx.json_nested), black_box(&schema), black_box(&opts)).unwrap();
            black_box(ds)
        })
    });
    group.bench_function(BenchmarkId::new("json_nested", "rotating_known_schema"), |b| {
        static ROT_IDX: AtomicUsize = AtomicUsize::new(0);
        let schema = schema_nested();
        b.iter_batched(
            || {
                let i = ROT_IDX.fetch_add(1, Ordering::Relaxed) % fx.json_nested_rotating.len();
                fx.json_nested_rotating[i].clone()
            },
            |p| {
                let ds = ingest_from_path(black_box(p), black_box(&schema), black_box(&opts)).unwrap();
                black_box(ds)
            },
            BatchSize::SmallInput,
        )
    });

    bench_case(
        &mut group,
        "parquet",
        &fx.parquet,
        &fx.parquet_rotating,
        Some(schema_flat()),
        &opts,
    );

    bench_case(
        &mut group,
        "xlsx",
        &fx.xlsx,
        &fx.xlsx_rotating,
        Some(schema_flat()),
        &opts,
    );

    group.finish();
}

fn bench_ingest_then_reduce(c: &mut Criterion) {
    let fx = fixtures();
    let opts = IngestionOptions::default();
    let schema = schema_flat();

    let mut group = c.benchmark_group("ingest_then_reduce_sum_score_20k");

    fn bench_e2e(
        group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
        id: &str,
        path: &Path,
        schema: &Schema,
        opts: &IngestionOptions,
    ) {
        group.bench_function(BenchmarkId::new(id, "known_schema"), |b| {
            b.iter(|| {
                let ds = ingest_from_path(black_box(path), black_box(schema), black_box(opts)).unwrap();
                let out = reduce(black_box(&ds), black_box("score"), black_box(ReduceOp::Sum)).unwrap();
                black_box(out)
            })
        });

        group.bench_function(BenchmarkId::new(id, "infer_schema"), |b| {
            b.iter(|| {
                let inferred = infer_schema_from_path(black_box(path), black_box(opts)).unwrap();
                let ds = ingest_from_path(black_box(path), black_box(&inferred), black_box(opts)).unwrap();
                let out = reduce(black_box(&ds), black_box("score"), black_box(ReduceOp::Sum)).unwrap();
                black_box(out)
            })
        });
    }

    bench_e2e(&mut group, "csv", &fx.csv, &schema, &opts);
    bench_e2e(&mut group, "json_array", &fx.json_array, &schema, &opts);
    bench_e2e(&mut group, "ndjson", &fx.ndjson, &schema, &opts);
    bench_e2e(&mut group, "parquet", &fx.parquet, &schema, &opts);
    bench_e2e(&mut group, "xlsx", &fx.xlsx, &schema, &opts);

    group.finish();
}

criterion_group!(benches, bench_ingestion, bench_ingest_then_reduce);
criterion_main!(benches);

