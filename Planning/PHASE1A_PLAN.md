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
- [x] Create `python-wrapper/` folder at repo root (new package workspace)
- [x] Add `python-wrapper/pyproject.toml` configured for maturin
- [x] Add `python-wrapper/rust_data_processing/__init__.py` (thin Python API surface)
- [x] Add `python-wrapper/src/lib.rs` (PyO3 module entrypoint) that calls into the Rust crate

#### 2.2.2 What the Python API should expose (Phase 1a)
Minimum useful surface:
- [x] `ingest_from_path(path, schema, options=None) -> DataSet`
- [x] `ingest_from_path_infer(path, options=None) -> (DataSet, Schema)` (via `ingest_with_inferred_schema`; single-step infer+ingest remains `ingest_from_path_infer`)
- [x] `infer_schema_from_path(path, options=None) -> Schema`
- [x] `sql.query(dataset_or_df, sql) -> DataSet` (`sql_query_dataset`; multi-table: `SqlContext`)
- [x] `transform.apply(dataset, spec) -> DataSet` (`transform_apply` / `transform_apply_json`)
- [x] `profiling.profile(dataset, options) -> dict` + `profile_dataset_markdown` / `profile_dataset_json`
- [x] `validation.validate(dataset, spec) -> report` (+ markdown/json helpers)
- [x] `outliers.detect(dataset, column, method, options) -> report` (+ markdown/json helpers)
- [x] **Map/Reduce parity**:
  - [x] `processing.filter(dataset, predicate) -> DataSet` (`processing_filter`)
  - [x] `processing.map(dataset, mapper) -> DataSet` (`processing_map`)
  - [x] `processing.reduce(dataset, column, op) -> Optional[Value]` (`processing_reduce`)
- [x] **Pipeline (Polars-backed) parity** (Python class wrapping `pipeline::DataFrame`):
  - [x] `pipeline.DataFrame.from_dataset(ds) -> DataFrame`
  - [x] `DataFrame.collect() -> DataSet`
  - [x] Transform wrappers parity (`select/rename/drop/cast/fill_null/derive/filter/group_by/join`)
- [x] **Parallel execution parity** (thin wrapper over `execution::ExecutionEngine`):
  - [x] `execution.filter_parallel(...)`, `execution.map_parallel(...)` — Python row callbacks use `Py::clone_ref` + `Python::with_gil` per row inside Rayon chunks (throughput is still GIL-limited for pure Python predicates; chunk scheduling/throttling matches Rust)
  - [x] `execution.reduce(...)` (`ExecutionEngine.reduce`)
  - [x] `execution.metrics_snapshot() -> dict`
- [x] **Observability parity** (optional but preferred for “pit of success”):
  - [x] Python callbacks for ingestion observer hooks (`on_success` / `on_failure` / `on_alert` on `options["observer"]`); execution stream via `ExecutionEngine(..., on_execution_event=...)` (`on_metric` ≈ execution event dicts with `kind` + metrics)
  - [x] Expose `alert_at_or_above` on path ingest `options` dict (`info` / `warning` / `error` / `critical`)
- [x] **DB ingestion parity** (feature-gated in Rust; surfaced in Python with clear install docs):
  - [x] `ingest_from_db(conn, query, schema) -> DataSet` — build extension with `cargo` / maturin `--features db`
  - [x] `ingest_from_db_infer(conn, query) -> DataSet` — same feature gate; returns dataset with inferred schema (use `DataSet.schema()` if you need the schema list)
- [x] **CDC boundary parity** (types only; no connector shipped in Phase 1a):
  - [x] `cdc.CdcEvent`, `cdc.CdcOp`, `cdc.TableRef`, `cdc.RowImage`, `cdc.SourceMeta` — Python dataclasses under `rust_data_processing.cdc`

Interop considerations:
- [x] Decide on Python-side “Schema” representation (likely dict/list of fields)
- [x] Decide on DataSet representation:
  - Option A: expose a Python `DataSet` class (backed by Rust)
  - Option B: convert to/from `pyarrow.Table` (heavier deps; likely Phase 1b)
  - Option C: **optional** conversion to/from `pandas.DataFrame` **only when explicitly requested by the end user**
- [x] Decide on Python dataframe story:
  - **Default**: return **Polars** objects (or crate-owned `DataSet`) from Python APIs — **implemented**: crate-owned `DataSet` + lazy `DataFrame` wrapper (no Polars types in Python)
  - **Opt-in**: provide `to_pandas()` / `from_pandas()` conversion utilities behind an explicit extra/flag

#### 2.2.5 Python “must not fall short” parity rules (Phase 1a)
- [x] **Rule**: If it exists in the Phase 1 Rust public API and is safe/portable, it must be callable from Python. *(Remaining gaps: optional Arrow/pandas interop; DB requires `--features db` build; CDC types are Python mirrors without live events.)*
- [x] **Rule**: The Python wrapper must not depend on pandas by default; **Polars-first** with explicit conversion APIs.
- [x] **Story**: Build a “Rust→Python parity matrix” table (module → functions/types → status), reviewed at each release — see `python-wrapper/PARITY.md`.
- [x] **Story**: Ensure Python docs mirror Rust docs (same examples, adapted to Python), including:
  - ingestion + schema inference + **observer / alert threshold**
  - pipelines + SQL
  - TransformSpec
  - profiling/validation/outliers report rendering (JSON/Markdown)
  - map/reduce + **parallel** execution engine + **execution events**
  - feature-gated DB ingestion (document prerequisites — `README_DEV.md`, `API.md`)

#### 2.2.3 Packaging + wheels
- [x] Use maturin to build wheels locally:
  - `maturin develop` for local dev installs
  - `maturin build --release` for wheels
- [x] Consider `abi3` wheels (optional): reduces per-Python-version wheel builds but constrains API usage. *(Documented in `How_TO_deploy.md` + `pyproject.toml` comment; not enabled.)*
- [x] CI: GitHub Actions matrix build using `PyO3/maturin-action`
  - Linux manylinux wheels + macOS + Windows
  - publish to PyPI on tagged releases *(`.github/workflows/python_ci.yml`, `python_release.yml`; tags `v*`; secret `PYPI_API_TOKEN`)*

#### 2.2.4 Versioning strategy
- [x] Keep Python package version aligned with Rust crate version for Phase 1a (simplest)
- [x] Document compatibility and feature flags (e.g., DB ingestion)

---

## 3) Documentation deliverables (Phase 1a)
- [x] Update `Planning/How_TO_deploy.md` to match maturin/PyO3 packaging (remove `setup.py` guidance)
- [x] Add “Python quickstart” section in README (install, ingest, profile, validate)
- [x] Add a short “Release checklist” for both crates.io + PyPI *(see `Planning/RELEASE_CHECKLIST.md`)*

---

## 4) GitHub Actions CI/CD (Phase 1a) + “small story” workflow

### Branching rule (applies to every story)
- [x] Create a **new branch per story** (small unit of work) and merge via PR to `main`. *(Team norm; documented in `Planning/CI_DEPLOY_POLICY.md`.)*
- [x] Keep stories “1–2 hours” sized where possible (single workflow, single template, single doc update, etc.). *(Same.)*

### Global CI research (shared prerequisite)
- [x] **(Research)** Decide what “deploy after merge to `main`” means safely for us:
  - crates.io **cannot** publish the same version twice; publishing on every merge will fail unless versions bump.
  - PyPI similarly requires version bumps (or unique build tags depending on strategy).
  - Outcome: document the chosen trigger policy (recommended options to evaluate):
    - A) deploy on **tagged release** (safer, typical) — **chosen**, with **tag commit must be on `origin/main`**
    - B) deploy on merge to `main` **only if** version changed and the registry doesn’t already contain it
    - C) deploy on merge to `main` to a staging index (TestPyPI) + deploy to prod on tag  
  - *Deliverable:* **`Planning/CI_DEPLOY_POLICY.md`**

---

## 5) CI/CD Story set A: Build + test + deploy to crates.io (Rust)

Goal: after merging a PR into `main`, have a predictable pipeline that builds/tests and (per policy) publishes the Rust crate.

- [x] **(Research)** crates.io publish automation constraints:
  - how to detect “version already published” — **`cargo publish` fails if version exists**; no extra probe in Phase 1a
  - required token scopes and best practices for storing token in GitHub Secrets — **`CRATES_IO_TOKEN`**; see **`How_TO_deploy.md`**
  - whether we should enforce “tag required” for publish even if merge-to-main builds happen — **yes: publish only on tag `v*` + main ancestry guard**
- [x] **Story**: Add `.github/workflows/rust_ci.yml` (build + test only)
  - triggers: PRs + pushes to `main`
  - steps: `cargo fmt --check`, `cargo clippy`, `cargo test` (incl. doctests), **ubuntu job:** `cargo test --features ci_expanded` (`db_connectorx` omitted — OpenSSL/Perl on Windows; run `--features db_connectorx` locally when tooling is ready)
- [x] **Story**: Add `.github/workflows/rust_release.yml` (deploy)
  - trigger: push tag **`v*`** with **main** ancestry check *(not bare merge-to-main publish)*
  - steps: **`cargo publish --dry-run`**, then **`cargo publish`**
  - guardrails: **registry rejects duplicate version**; tag off-`main` rejected by workflow
- [x] **Story**: Add a “release checklist” doc snippet (README or Planning) describing:
  - bump version
  - merge PR
  - verify CI green
  - publish performed by CI (or by tag) — **`Planning/RELEASE_CHECKLIST.md`**

---

## 6) CI/CD Story set B: Build + test + deploy to PyPI (Python wrapper)

Goal: after merging a PR into `main`, build wheels across OSes and (per policy) publish to PyPI.

- [x] **(Research)** Python packaging decisions to lock down:
  - maturin configuration in **`python-wrapper/pyproject.toml`** **`[tool.maturin]`** + **`README_DEV.md`**
  - wheel strategy: **per-interpreter wheels** via **`maturin-action --find-interpreter`**; **`abi3`** documented but **not** enabled (see **`How_TO_deploy.md`** + **`pyproject.toml`** comment)
  - Linux wheel policy: **`manylinux: auto`** in **`python_release.yml`**
  - publishing policy: **production PyPI** on tag **`v*`** + **`main`** ancestry guard; **TestPyPI** optional / manual
- [x] **Story**: Add `.github/workflows/python_ci.yml` (build + test only)
  - triggers: PRs + pushes to **`main`** (path-filtered: wrapper / `src/` / root `Cargo.toml` + lockfile)
  - steps: **`uv sync`**, **`maturin develop --release`**, **`pytest`**; plus **Ubuntu + Python 3.12** wheel + **`pip install`** smoke
- [x] **Story**: Add `.github/workflows/python_release.yml` (deploy)
  - trigger: push tag **`v*`** with **origin/main** ancestry check (same policy as Rust release)
  - steps: **`PyO3/maturin-action`** wheels (manylinux + sdist on Linux) + **Windows/macOS** → **`pypa/gh-action-pypi-publish`**
  - guardrails: **tag must be on `main`**; **PyPI rejects duplicate version**; secrets **`PYPI_API_TOKEN`** (+ **`CRATES_IO_TOKEN`** for Rust sibling workflow)
- [x] **Story**: Add “Python release checklist” covering:
  - version bump / Rust alignment — **`Planning/RELEASE_CHECKLIST.md`** §1, §4
  - validate wheels — same doc §4 (local **`maturin build`**) + §5 **`pip install ...==X.Y.Z`**

---

## 7) Bug reporting + triage plan (so users can report issues post-release)

### GitHub bug reporting stories
- [x] **(Research)** Decide between GitHub Issue Templates vs Issue Forms (YAML).
  - Issue Forms are YAML under `.github/ISSUE_TEMPLATE/` and collect structured fields (repro steps, versions, logs).
  - **Outcome:** **Issue Forms (YAML)** — see `.github/ISSUE_TEMPLATE/bug_report.yml` and `feature_request.yml`.
- [x] **Story**: Add a bug report template/form:
  - required: “what happened”, “expected”, “steps to reproduce”, “version”, “OS”, “dataset size”, “feature flags enabled”, “logs/stack trace”
  - auto-label: `bug`
- [x] **Story**: Add a feature request template/form (label `enhancement`)
- [x] **Story**: Add `SECURITY.md` with security reporting instructions (even if minimal)
- [x] **Story**: Add README section “Reporting bugs” pointing to GitHub Issues and describing what info to include
  - Extended with **`Planning/ISSUE_TRIAGE.md`**, **`Planning/DOCUMENTATION.md`**, **`CONTRIBUTING.md`**, combined **rustdoc + pdoc** CI (`.github/workflows/docs.yml`) and README **Documentation** links.

### “Top 10 bug reports” research story (for prioritization)
- [x] **(Research)** Define how we will compute “top 10 bugs” for our library:
  - Option A: GitHub Issue search sorted by **most comments** for `label:bug`
  - Option B: GitHub GraphQL/REST to rank by **reactions** (thumbs up / total reactions)
  - Option C: include external signals (if any) such as CVEs / advisory feeds (future)
  - **Deliverable:** documented in **`Planning/ISSUE_TRIAGE.md`**: primary bookmarked query `is:issue is:open label:bug sort:reactions-+1-desc`, optional `sort:comments-desc`, weekly triage cadence, labels `bug`, `needs-triage`, `confirmed`, `help wanted`. (Script/automation deferred.)

