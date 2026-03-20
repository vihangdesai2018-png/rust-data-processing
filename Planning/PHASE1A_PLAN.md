# Phase 1a Plan (post-Phase 1 hardening + Python packaging)

Phase 1a goal: turn the Phase 1 Rust library into a **production-usable**, **publishable** artifact with:
- ML-friendly map/reduce primitives (the most common aggregates used in feature engineering + training loops)
- Stable report formats + minimal product-layer ergonomics
- A first-class **Python wrapper** (PyO3 + maturin) and a clear deployment process

Non-goals:
- Rebuilding Polars/DataFusion in our own DSL
- Distributed compute framework integration (Spark/Ray/etc.) beyond exporting results in portable formats

---

## 1) ML-oriented map/reduce: what’s most common, what we should add

### Why map/reduce matters for ML
Most “classic” ML preprocessing and training reduces to repeated passes of:
- **map**: transform rows/columns into features/gradients
- **reduce**: aggregate statistics (mean/std/min/max/count, etc.) or aggregate gradients (sum, average)

These are the most common *reduce* primitives used directly or indirectly in ML systems:

- **Count**: row counts; class counts; missingness counts
- **Sum / Mean**: feature means; gradient sums; average losses
- **Variance / Standard deviation**: z-score standardization, anomaly scoring, normalization
  - Implementation note: use **numerically stable** one-pass algorithms (e.g. **Welford**) to avoid catastrophic cancellation.
- **Min / Max**: min-max scaling; clipping thresholds; bounds checks
- **Quantiles (p50/p95/p99)**: robust scaling, winsorization, outlier thresholds, feature distribution summaries
- **Distinct count / cardinality**: categorical feature monitoring; leakage checks; uniqueness checks
- **Sum of squares / L2 norm**: regularization, feature vector norms, gradient norms
- **Covariance / correlation** (often “Phase 1b/2”): feature selection, diagnostics

### Proposed additions (Phase 1a)
Implement as both:
- **in-memory** (`processing::reduce` style) for small data / deterministic tests, and
- **Polars-backed** (`pipeline::DataFrame` aggregations) for large data / performance.

#### 1.1 Reduce ops to add (single-column)
- [x] **Mean**: numeric columns; ignore nulls; all-null → Null
- [x] **Variance**: numeric; stable algorithm (Welford); sample vs population option
- [x] **StdDev**: derived from variance; stable
- [x] **SumSquares**: \(\sum x^2\) (nulls ignored); helps with norms/variance
- [x] **L2Norm**: \(\sqrt{\sum x^2}\)
- [x] **CountDistinctNonNull**: distinct excluding nulls

#### 1.2 Multi-column / vector-style reductions (optional in 1a)
- [x] **Feature-wise mean/std** for multiple columns in one pass (API sugar)
- [x] **TopK** / **ArgMax/ArgMin** (often used for label distribution and debugging)

#### 1.3 Group-by reductions (key for ML feature engineering)
- [x] Group-by mean/std/min/max/count on numeric features (per-key aggregates)
- [x] Group-by count-distinct (categorical)

### Semantics to decide and document (important for users)
- [x] **Null handling**: ignore nulls for numeric aggregates; count includes nulls unless explicitly “non-null count”
- [x] **All-null groups**: variance/std/mean should return Null (not 0)
- [x] **Float rounding**: deterministic formatting for report outputs
- [x] **Strict vs lossy casting**: re-use `CastMode` in any spec-driven transforms

### Test plan (Phase 1a)
- [x] Unit tests for each new reduce op (edge cases: empty, all-null, mixed types)
- [x] Deep tests on realistic fixtures: compute mean/std/min/max on numeric columns and validate invariants
- [x] Benchmarks: compare in-memory vs Polars-backed implementations where both exist

---

## 2) Deployment plan (Rust crate + Python package)

### 2.1 Publish Rust crate to crates.io
- [x] Ensure `Cargo.toml` metadata is production-ready:
  - name, version, description, license, repository, readme, categories/keywords, `rust-version`, `exclude`
- [x] Confirm licensing files (`LICENSE-MIT`, `LICENSE-APACHE`) and README are present and accurate
- [ ] **Maintainer step:** Tag a release (e.g. `v0.1.0`) and publish (requires crates.io token + clean commit):
  - `cargo login <token>`
  - `cargo publish --dry-run` then `cargo publish`
  - See `Planning/RELEASE_CHECKLIST.md`
- [x] Release hygiene:
  - `CHANGELOG.md` at repo root (Keep a Changelog style); GitHub Releases optional but recommended when tagging
  - SemVer documented in release checklist; bump `version` in `Cargo.toml` for every crates.io upload

### 2.2 Python wrapper strategy (Phase 1a)
**Recommended tooling:** **PyO3 + maturin** using `pyproject.toml` (modern Python packaging).


1. We will be using uv to manage our dependencies. 
2. Please use python-wrapper folder I have created for all python specific development and documentation. 
3. Everything we can do in the rust library shold be aviliable to the python developer in the wrapper api so research if anything is missing in the lib below and add it. 
4. same api.md, python documentation, readme and readme-dev should be managed for the python wrapper. 



Goals:
- Provide Python users a “pit of success” API around the existing Rust crate
- Keep the wrapper thin and stable; avoid exposing Polars internals
- Ship prebuilt wheels for Windows/macOS/Linux

#### 2.2.1 Python package layout
- [ ] Create `python-wrapper/` folder at repo root (new package workspace)
- [ ] Add `python-wrapper/pyproject.toml` configured for maturin
- [ ] Add `python-wrapper/rust_data_processing/__init__.py` (thin Python API surface)
- [ ] Add `python-wrapper/src/lib.rs` (PyO3 module entrypoint) that calls into the Rust crate

#### 2.2.2 What the Python API should expose (Phase 1a)
Minimum useful surface:
- [ ] `ingest_from_path(path, schema, options=None) -> DataSet`
- [ ] `ingest_from_path_infer(path, options=None) -> (DataSet, Schema)` (or return report with inferred schema)
- [ ] `infer_schema_from_path(path, options=None) -> Schema`
- [ ] `sql.query(dataset_or_df, sql) -> DataSet`
- [ ] `transform.apply(dataset, spec) -> DataSet`
- [ ] `profiling.profile(dataset, options) -> dict` + `to_markdown(report)` / `to_json(report)`
- [ ] `validation.validate(dataset, spec) -> report`
- [ ] `outliers.detect(dataset, column, method, options) -> report`
- [ ] **Map/Reduce parity**:
  - [ ] `processing.filter(dataset, predicate) -> DataSet`
  - [ ] `processing.map(dataset, mapper) -> DataSet`
  - [ ] `processing.reduce(dataset, column, op) -> Optional[Value]`
- [ ] **Pipeline (Polars-backed) parity** (Python class wrapping `pipeline::DataFrame`):
  - [ ] `pipeline.DataFrame.from_dataset(ds) -> DataFrame`
  - [ ] `DataFrame.collect() -> DataSet`
  - [ ] Transform wrappers parity (`select/rename/drop/cast/fill_null/derive/filter/group_by/join`)
- [ ] **Parallel execution parity** (thin wrapper over `execution::ExecutionEngine`):
  - [ ] `execution.filter_parallel(...)`, `execution.map_parallel(...)`, `execution.reduce(...)`
  - [ ] `execution.metrics_snapshot() -> dict`
- [ ] **Observability parity** (optional but preferred for “pit of success”):
  - [ ] Python callbacks for observer hooks (at least: on_error/on_alert/on_metric)
  - [ ] Expose `alert_at_or_above`-style configuration
- [ ] **DB ingestion parity** (feature-gated in Rust; surfaced in Python with clear install docs):
  - [ ] `ingest_from_db(conn, query) -> DataSet`
  - [ ] `ingest_from_db_infer(conn, query) -> DataSet` (or `(DataSet, Schema)`)
- [ ] **CDC boundary parity** (types only; no connector shipped in Phase 1a):
  - [ ] `cdc.CdcEvent`, `cdc.CdcOp`, `cdc.TableRef`, `cdc.RowImage`, `cdc.SourceMeta`

Interop considerations:
- [ ] Decide on Python-side “Schema” representation (likely dict/list of fields)
- [ ] Decide on DataSet representation:
  - Option A: expose a Python `DataSet` class (backed by Rust)
  - Option B: convert to/from `pyarrow.Table` (heavier deps; likely Phase 1b)
  - Option C: **optional** conversion to/from `pandas.DataFrame` **only when explicitly requested by the end user**
- [ ] Decide on Python dataframe story:
  - **Default**: return **Polars** objects (or crate-owned `DataSet`) from Python APIs
  - **Opt-in**: provide `to_pandas()` / `from_pandas()` conversion utilities behind an explicit extra/flag

#### 2.2.5 Python “must not fall short” parity rules (Phase 1a)
- [ ] **Rule**: If it exists in the Phase 1 Rust public API and is safe/portable, it must be callable from Python.
- [ ] **Rule**: The Python wrapper must not depend on pandas by default; **Polars-first** with explicit conversion APIs.
- [ ] **Story**: Build a “Rust→Python parity matrix” table (module → functions/types → status), reviewed at each release.
- [ ] **Story**: Ensure Python docs mirror Rust docs (same examples, adapted to Python), including:
  - ingestion + schema inference
  - pipelines + SQL
  - TransformSpec
  - profiling/validation/outliers report rendering (JSON/Markdown)
  - map/reduce + execution engine
  - feature-gated DB ingestion (document prerequisites)

#### 2.2.3 Packaging + wheels
- [ ] Use maturin to build wheels locally:
  - `maturin develop` for local dev installs
  - `maturin build --release` for wheels
- [ ] Consider `abi3` wheels (optional): reduces per-Python-version wheel builds but constrains API usage.
- [ ] CI: GitHub Actions matrix build using `PyO3/maturin-action`
  - Linux manylinux wheels + macOS + Windows
  - publish to PyPI on tagged releases

#### 2.2.4 Versioning strategy
- [ ] Keep Python package version aligned with Rust crate version for Phase 1a (simplest)
- [ ] Document compatibility and feature flags (e.g., DB ingestion)

---

## 3) Documentation deliverables (Phase 1a)
- [ ] Update `Planning/How_TO_deploy.md` to match maturin/PyO3 packaging (remove `setup.py` guidance)
- [ ] Add “Python quickstart” section in README (install, ingest, profile, validate)
- [ ] Add a short “Release checklist” for both crates.io + PyPI

---

## 4) GitHub Actions CI/CD (Phase 1a) + “small story” workflow

### Branching rule (applies to every story)
- [ ] Create a **new branch per story** (small unit of work) and merge via PR to `main`.
- [ ] Keep stories “1–2 hours” sized where possible (single workflow, single template, single doc update, etc.).

### Global CI research (shared prerequisite)
- [ ] **(Research)** Decide what “deploy after merge to `main`” means safely for us:
  - crates.io **cannot** publish the same version twice; publishing on every merge will fail unless versions bump.
  - PyPI similarly requires version bumps (or unique build tags depending on strategy).
  - Outcome: document the chosen trigger policy (recommended options to evaluate):
    - A) deploy on **tagged release** (safer, typical)
    - B) deploy on merge to `main` **only if** version changed and the registry doesn’t already contain it
    - C) deploy on merge to `main` to a staging index (TestPyPI) + deploy to prod on tag

---

## 5) CI/CD Story set A: Build + test + deploy to crates.io (Rust)

Goal: after merging a PR into `main`, have a predictable pipeline that builds/tests and (per policy) publishes the Rust crate.

- [ ] **(Research)** crates.io publish automation constraints:
  - how to detect “version already published”
  - required token scopes and best practices for storing token in GitHub Secrets
  - whether we should enforce “tag required” for publish even if merge-to-main builds happen
- [ ] **Story**: Add `.github/workflows/rust_ci.yml` (build + test only)
  - triggers: PRs to `main` + pushes to `main`
  - steps: `cargo fmt --check`, `cargo clippy`, `cargo test` (include doctests if used), `cargo test --all-features` if appropriate
- [ ] **Story**: Add `.github/workflows/rust_release.yml` (deploy)
  - trigger: push to `main` (post-merge) **or** tag (depends on research decision)
  - steps: `cargo publish --dry-run`, then `cargo publish`
  - guardrails: fail fast if version not bumped / already published (define mechanism in the research story)
- [ ] **Story**: Add a “release checklist” doc snippet (README or Planning) describing:
  - bump version
  - merge PR
  - verify CI green
  - publish performed by CI (or by tag)

---

## 6) CI/CD Story set B: Build + test + deploy to PyPI (Python wrapper)

Goal: after merging a PR into `main`, build wheels across OSes and (per policy) publish to PyPI.

- [ ] **(Research)** Python packaging decisions to lock down:
  - maturin configuration in `pyproject.toml`
  - wheel strategy: per-Python wheels vs `abi3` (and minimum Python version)
  - Linux wheel policy: manylinux target used by CI
  - publishing policy: PyPI vs TestPyPI
- [ ] **Story**: Add `.github/workflows/python_ci.yml` (build + test only)
  - triggers: PRs to `main` + pushes to `main`
  - steps: build extension (maturin), run Python smoke tests, run `python -m pip install .` style checks (as decided)
- [ ] **Story**: Add `.github/workflows/python_release.yml` (deploy)
  - trigger: push to `main` (post-merge) **or** tag (depends on research decision)
  - steps: build wheels (Windows/macOS/Linux), upload to PyPI using token in GitHub Secrets
  - guardrails: only publish when version changed and release is intended
- [ ] **Story**: Add “Python release checklist” covering:
  - version bump location (pyproject version) and alignment with Rust
  - how to validate wheel artifacts (basic import + smoke run)

---

## 7) Bug reporting + triage plan (so users can report issues post-release)

### GitHub bug reporting stories
- [ ] **(Research)** Decide between GitHub Issue Templates vs Issue Forms (YAML).
  - Issue Forms are YAML under `.github/ISSUE_TEMPLATE/` and collect structured fields (repro steps, versions, logs).
  - Outcome: choose the format and define required fields for a high-signal bug report.
- [ ] **Story**: Add a bug report template/form:
  - required: “what happened”, “expected”, “steps to reproduce”, “version”, “OS”, “dataset size”, “feature flags enabled”, “logs/stack trace”
  - auto-label: `bug`
- [ ] **Story**: Add a feature request template/form (label `enhancement`)
- [ ] **Story**: Add `SECURITY.md` with security reporting instructions (even if minimal)
- [ ] **Story**: Add README section “Reporting bugs” pointing to GitHub Issues and describing what info to include

### “Top 10 bug reports” research story (for prioritization)
- [ ] **(Research)** Define how we will compute “top 10 bugs” for our library:
  - Option A: GitHub Issue search sorted by **most comments** for `label:bug`
  - Option B: GitHub GraphQL/REST to rank by **reactions** (thumbs up / total reactions)
  - Option C: include external signals (if any) such as CVEs / advisory feeds (future)
  - Deliverable: a repeatable query (or small script later) and a lightweight triage process:
    - weekly review cadence
    - how to tag: `bug`, `needs-triage`, `confirmed`, `help wanted`

