# rust-data-processing

**Rust** crate and **Python** package for ingesting common file formats (CSV / JSON / Parquet / Excel (feature-gated)) into an in-memory `DataSet`, with schema validation + inference helpers, in-memory + parallel processing primitives, and a Polars-backed DataFrame-centric pipeline API. Phase 1 also targets profiling, validation, and outlier-detection APIs and report formats, while keeping a small engine-agnostic public surface (SQL support is Polars-backed). Use whichever binding fits your stack—the capabilities are the same under the hood.

## Documentation (read the APIs online)

| | Link |
| --- | --- |
| **Combined Rust + Python (main branch, HTML)** | [GitHub Pages — rust-data-processing](https://vihangdesai2018-png.github.io/rust-data-processing/) — *enable Pages → GitHub Actions in repo Settings if the site is not live yet; see [`Planning/DOCUMENTATION.md`](Planning/DOCUMENTATION.md).* |
| **Rust crate on crates.io** | [docs.rs — rust-data-processing](https://docs.rs/rust-data-processing) *(populates after the first successful publish)* |
| **Markdown API guides** | [`API.md`](API.md) (Rust); Python: [`python-wrapper/API.md`](python-wrapper/API.md) |
| **Rust examples (this repo)** | [`docs/rust/README.md`](docs/rust/README.md) — `Cargo.toml`, ingestion, DataFrame/SQL, cookbook, execution, benchmarks |
| **Python examples (this repo)** | [`docs/python/README.md`](docs/python/README.md) — same topics via `rust_data_processing` |

## Quick start (Python)

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "name", "data_type": "utf8"},
]
ds = rdp.ingest_from_path("tests/fixtures/people.csv", schema, {"format": "csv"})
print("rows", ds.row_count())

report = rdp.profile_dataset(ds, {"head_rows": 50, "quantiles": [0.5]})
print("profile rows sampled", report["row_count"])

validation = rdp.validate_dataset(
    ds,
    {"checks": [{"kind": "not_null", "column": "id", "severity": "error"}]},
)
print("checks", validation["summary"]["total_checks"])
```

**From a checkout** (Rust + [uv](https://docs.astral.sh/uv/) required for the editable build):

```bash
cd python-wrapper
uv sync --group dev
uv run maturin develop --release
```

**From PyPI** (after you publish a release — see [`Planning/RELEASE_CHECKLIST.md`](Planning/RELEASE_CHECKLIST.md)):

```bash
pip install rust-data-processing
```

Use the same `import rust_data_processing as rdp` pattern; point `ingest_from_path` at your own CSV, JSON, or Parquet files and schema.

**Rust:** [`docs/rust/README.md`](docs/rust/README.md) has copy-paste examples for `Cargo.toml`, ingestion, Polars-backed pipelines, SQL, transforms, profiling, validation, execution, and benchmarks. **Python (expanded):** [`docs/python/README.md`](docs/python/README.md). The conceptual Rust API overview is in [`API.md`](API.md).

Generate the same HTML as CI locally: `./scripts/build_docs.ps1` (Rust only) or `./scripts/build_docs.ps1 -All` (Rust + Python → `_site/python/`). Maintainer notes: [`Planning/DOCUMENTATION.md`](Planning/DOCUMENTATION.md).

## Reporting bugs

- Open a **[GitHub Issue](https://github.com/vihangdesai2018-png/rust-data-processing/issues)** and use **Bug Report** or **Feature Request** so we get version, OS, and repro steps.
- **Security:** do not file publicly — read [`SECURITY.md`](SECURITY.md).
- How we triage and prioritize: [`Planning/ISSUE_TRIAGE.md`](Planning/ISSUE_TRIAGE.md).

- **Status**: library APIs are in `src/lib.rs`; the binary (`src/main.rs`) is currently just a placeholder.
- **Developer guide**: see `README_DEV.md` (module map, workflows, conventions)
- **Benchmark snapshot (pipeline bench)**: on this repo (Windows, Criterion), `filter → map → reduce(sum)`:
  - **In-memory `processing`**:
    - 100k rows: ~10.42 ms median (\(\approx 9.6\) million rows/sec)
    - 1M rows: ~113.5 ms median (\(\approx 8.8\) million rows/sec)
  - **Polars-backed `pipeline::DataFrame` (lazy)**:
    - 100k rows: ~7.80 ms median (\(\approx 12.8\) million rows/sec)
    - 1M rows: ~74.10 ms median (\(\approx 13.5\) million rows/sec)
  - **Reproduce**: `cargo bench --bench pipelines -- --warm-up-time 2 --measurement-time 6 --sample-size 50`

## Phase 1 scope (roadmap)

Canonical checklist lives in `Planning/PHASE1_PLAN.md`; this section is the README-friendly summary.

- [x] Polars-first delegation for ingestion + DataFrame-centric pipelines
- [x] Polars-backed SQL support (default-on)
- [x] Engine-agnostic configuration (`IngestionOptionsBuilder`) + unified error model (`IngestionError`)
- [x] Benchmarks + parity checks (ingestion + pipelines + end-to-end)
- [x] Cookbook examples (Polars-first docs + SQL examples)
- [x] “Pit of success” defaults (sane knobs; avoid promising engine-specific tuning we can’t support)
- [x] Feature flags + dependency surface minimization
- [x] Transformation wrappers + end-user transformation schema/spec (“to/from”) on top of Polars + existing in-memory layers
- [x] Feature-gated direct DB ingestion via ConnectorX (DB → Arrow → `DataSet`) + compatibility research notes
- [x] CDC feasibility spike + interface boundary (Phase 2 candidate)
- [x] Profiling APIs: metrics set + sampling/large-data modes
- [x] Validation APIs: DSL + built-in checks + severity handling + reporting
- [x] Outlier detection: primitives + explainable outputs

## Platform support

- **Supported OSes**: Windows, Linux, and macOS.
- **Works out of the box**: the library is written in portable Rust (no OS-specific runtime assumptions).
- **Build prerequisites**:
  - **macOS**: install Xcode Command Line Tools (`xcode-select --install`) for the system linker/C toolchain.
  - **Linux**: install a basic build toolchain (e.g. GCC/Clang via your distro’s `build-essential` equivalent).
  - **Windows**: see [Development on Windows (toolchain + linker)](#development-on-windows-toolchain--linker).

  Parquet support pulls in native compression dependencies (e.g. `zstd-sys`); Cargo will build them automatically once a C toolchain is available.

- **Benchmarks**:
  - `cargo bench --bench pipelines` is cross-platform.
  - `benchmarks.ps1` is a Windows/PowerShell convenience wrapper; on Linux/macOS you can run it via `pwsh` or just run `cargo bench` directly.
  - `scripts/run_benchmarks.ps1` runs all Criterion benchmarks (pipelines + ingestion + map/reduce + profiling + validation + outliers).

## Python bindings

Bindings live under **`python-wrapper/`** (**PyO3** + **maturin** + **uv**). User-facing docs: **`python-wrapper/README.md`**, **`python-wrapper/API.md`**, **`python-wrapper/README_DEV.md`**. The native module calls this crate; Polars stays on the Rust side.

**Rust** examples (ingestion, DataFrame/SQL, transforms, profiling, execution, benchmarks): [`docs/rust/README.md`](docs/rust/README.md).

## What data can be consumed? (Epic 1 / Stories 1.1–1.2)

### File formats (auto-detected by extension)

- **CSV**: `.csv` (must include headers)
- **JSON**: `.json` (array-of-objects) and `.ndjson` (newline-delimited objects)
  - Nested fields are supported via **dot paths** in schema field names (e.g. `user.name`)
- **Parquet**: `.parquet`, `.pq`
- **Excel/workbooks**: `.xlsx`, `.xls`, `.xlsm`, `.xlsb`, `.ods` (requires feature `excel`)

### Supported value types

You define a `Schema` using these logical types:

- `DataType::Int64`
- `DataType::Float64`
- `DataType::Bool`
- `DataType::Utf8`

Ingestion yields a `DataSet` whose cells are `Value::{Int64, Float64, Bool, Utf8, Null}`.

- **Null handling**:
  - CSV: empty/whitespace-only cells become `Value::Null`
  - JSON: explicit `null` becomes `Value::Null`
  - Excel: empty cells become `Value::Null`
  - Parquet: nulls become `Value::Null`

## Processing pipelines (Epic 1 / Story 1.2)

Once you have a `DataSet` (typically from `ingestion::ingest_from_path`), you can apply in-memory
transformations using `rust_data_processing::processing`:

- `filter(&DataSet, predicate) -> DataSet`
- `map(&DataSet, mapper) -> DataSet`
- `reduce(&DataSet, column, ReduceOp) -> Option<Value>` — includes **mean**, **variance**, **std dev** (`VarianceKind::{Population, Sample}`), **sum of squares**, **L2 norm**, **count distinct** (non-null), plus **count** / **sum** / **min** / **max**
- `feature_wise_mean_std(&DataSet, &[&str], VarianceKind)` — one pass over rows for mean + std on several numeric columns (`FeatureMeanStd`)
- `arg_max_row` / `arg_min_row` — first row index where a column is max/min (ties: smallest index)
- `top_k_by_frequency` — top‑\(k\) `(value, count)` pairs for label-style columns

Polars-backed equivalents for whole-frame scalars: `pipeline::DataFrame::reduce`, `feature_wise_mean_std`. **Semantics**: [`Planning/REDUCE_AGG_SEMANTICS.md`](Planning/REDUCE_AGG_SEMANTICS.md).

Full **Rust** examples (filter/map/reduce, aggregates, parallel `ExecutionEngine`, Criterion benchmarks, ingestion observers): [`docs/rust/README.md`](docs/rust/README.md) § *Processing pipelines*.

## Supported formats

- **CSV**: headers required; schema fields must exist; columns may be reordered.
- **JSON**: supports JSON array of objects or NDJSON; nested fields via dot paths (e.g. `user.name`).
- **Parquet**: validates required columns; uses the Parquet record API for reading.
- **Excel**: behind the Cargo feature `excel`.

## Features

- `excel`: enable Excel ingestion (adds `calamine`)
- `excel_test_writer`: enables Excel integration tests that generate an `.xlsx` at runtime
- `sql`: enable Polars-backed SQL support (adds `polars-sql`). **Enabled by default**.
- `db_connectorx`: enable direct DB ingestion via ConnectorX (DB → Arrow → `DataSet`) including Postgres/MySQL/MS SQL/Oracle sources
- `arrow`: enable Arrow interop helpers (adds `arrow`)
- `serde_arrow`: enable Serde-based Arrow interop helpers (adds `serde` + `serde_arrow`)
- Note: ConnectorX’s Postgres support uses OpenSSL; on Windows you may need additional build prerequisites (e.g. Perl for vendored OpenSSL or a system OpenSSL install).

Disable default features (including SQL) if you want a smaller dependency surface:

```toml
[dependencies]
rust-data-processing = { path = ".", default-features = false }
```

## “Pit of success” defaults (Phase 1)

- **Ingestion defaults**: format is auto-detected by extension; observers are off by default; failures are surfaced as `IngestionError` (no automatic retries).
- **Execution defaults**: `ExecutionOptions::default()` uses available parallelism, a moderate `chunk_size`, and throttles in-flight chunks.
- **Polars pipelines + SQL**: we intentionally avoid exposing engine-specific tuning knobs in the public API. If you need low-level tuning, use Polars’ own configuration (e.g. environment variables) and treat behavior as Polars-owned.

## Run tests

```powershell
./scripts/run_unit_tests.ps1
```

## Generate API docs (Rustdoc + optional Python pdoc)

Rust uses **Rustdoc**; Python bindings use **pdoc** (same as CI). See [Documentation](#documentation-read-the-apis-online) for published links.

```powershell
./scripts/build_docs.ps1              # Rust only → target/doc/
./scripts/build_docs.ps1 -All         # Rust + Python → target/doc/ and _site/python/
```

## Deep tests (large/realistic fixtures)

Deep tests are **not** run as part of `./scripts/run_unit_tests.ps1`. They are feature-gated behind `deep_tests`.

```powershell
./scripts/run_deep_tests.ps1
```

## Development on Windows (toolchain + linker)

Rust installs its tools into:

- `%USERPROFILE%\.cargo\bin` (example: `C:\Users\Vihan\.cargo\bin`)

That directory must be on your `PATH` so `rustc`, `cargo`, and `rustup` can be found.

If you see `error: linker 'link.exe' not found`, install **Build Tools for Visual Studio 2026** and select:

- **Desktop development with C++**
- **MSVC v144 - VS 2026 C++ x64/x86 build tools**
- **Windows 10/11 SDK**

Then open the project from **Developer PowerShell for VS 2026** (or restart your terminal) and rerun:

```powershell
cargo test
```

### Verify toolchain

```powershell
where.exe rustc
rustc --version
cargo --version
rustup --version
```

### Fix PATH for the current PowerShell session (no restart)

```powershell
$env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' + `
            [Environment]::GetEnvironmentVariable('Path','User')
```

### Ensure `%USERPROFILE%\.cargo\bin` is on your *User* PATH (permanent)

```powershell
$cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'
$userPath = [Environment]::GetEnvironmentVariable('Path','User')
if ([string]::IsNullOrWhiteSpace($userPath)) { $userPath = '' }

$parts = $userPath -split ';' | Where-Object { $_ -and $_.Trim() -ne '' }
if ($parts -notcontains $cargoBin) {
  [Environment]::SetEnvironmentVariable('Path', (@($parts + $cargoBin) -join ';'), 'User')
}
```

After changing the *User* PATH, **restart your terminal** (or log out/in) so new shells inherit it.

## License

Dual-licensed under your choice of **Apache License 2.0** or **MIT**. See [`LICENSE-APACHE`](LICENSE-APACHE) and [`LICENSE-MIT`](LICENSE-MIT).

SPDX-License-Identifier: `MIT OR Apache-2.0`

## Publishing to crates.io

Maintainers: see [`Planning/RELEASE_CHECKLIST.md`](Planning/RELEASE_CHECKLIST.md) and [`How_TO_deploy.md`](Planning/How_TO_deploy.md). After the first successful `cargo publish`, API docs appear on [docs.rs](https://docs.rs/rust-data-processing) for the published version.