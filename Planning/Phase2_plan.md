# Phase 2 plan — initial draft (scope + Rust vs Python)

This is a **working draft**: what might ship **inside** `rust-data-processing` (Rust core + Python wrapper) versus **outside** the library but worth **documenting, examples, or thin integrations**.

**Principles (carry forward from Phase 1 / 1a):**

- **Single-node / library-first** ETL + QA + transforms — **not** a distributed execution engine (no Spark/Ray/Ballista *inside* this crate).
- **Rust** for correctness, performance, and shared contracts; **Python** for notebooks, orchestration glue, and ecosystem reach.
- **“Good support”** below means: stable export types, docs, optional adapters, or reference patterns — **not** re-implementing Airflow, dbt, or every cloud vector DB.

---

## Summary table

| Idea | Mostly in Rust? | Mostly Python? | Outside core library (“good support”) |
|------|-----------------|----------------|--------------------------------------|
| 1. Notebook support | — | **Yes** (primary) | Docs, templates, Colab/Kaggle examples |
| 2. Passive long-term memory | Partial | **Yes** (orchestration) | App architecture; optional export formats |
| 3. LLM support | Partial | **Yes** (ecosystem) | Prompts from profiles/reports; no model hosting |
| 4. Job scheduler | **No** (not in-crate) | Glue / ops | Cron, Airflow, Dagster, K8s CronJob |
| 5. Partitions | **Yes** (single-node) | Expose same | Not cluster partitions / shuffle |
| 6. Incremental load | **Yes** | Thin wrappers | Watermarks, file lists, CDC handoff |
| 7. Iceberg / Delta | **Yes** (via ecosystem crates) | PyArrow path | Table *read* paths; not full catalog ops |
| 8. Airflow / dbt structure | **No** | Operators / macros | Example DAGs, dbt Python models calling the lib |
| 9. Vector DB integration | Limited in Rust core | **Yes** (SDKs) | Export embeddings + metadata as `DataSet` or files |

---

## 1. Notebook support

**Reality:** Interactive notebooks are a **Python/Jupyter** (or WASM) story. Rust kernels exist (e.g. `evcxr`) but data science users expect **IPython**, `matplotlib`, and `%timeit`.

**Recommendation:**

- **Python-first:** curated **Jupyter / Colab** notebooks in-repo (or a `notebooks/` repo), `pip install rust-data-processing`, copy-paste from `docs/python/README.md`.
- **Rust:** not “notebook support” inside the crate — keep **examples** as `cargo run` + optional **mdBook** for Rust-only tutorials.
- **Not the library:** hosting on Binder, Kaggle dataset badges, video walkthroughs.

---

## 2. Passive long-term memory (LTM)

**Interpretation:** Usually **application-level** (vector stores + retrieval + session state), not a tabular ETL primitive.

**Rust:** You can **serialize** profiling outputs, validation reports, and `DataSet` summaries to JSON for *downstream* memory systems — that fits the library’s **report artifacts** direction.

**Python:** LangChain / LlamaIndex / custom stores own “memory”; this library should **export structured facts** (schemas, profile dicts, validation summaries) rather than implement memory.

**Not the library:** Choosing Pinecone vs Weaviate vs pgvector for LTM; **good support** = clear JSON/Markdown report shapes + small examples of “feed this JSON into your RAG pipeline.”

---

## 3. LLM support

**Rust:** No need to embed **inference** here. Optional: small **helpers** to turn `profile_dataset` / `validate_dataset` results into **prompt-sized text** (deterministic, privacy-aware truncation) — still optional.

**Python:** Natural place for **LangChain tools**, **structured output** to OpenAI API, etc.

**Not the library:** Model weights, hosting, fine-tuning — **good support** = one **example** of “summarize this profile JSON with your LLM of choice.”

---

## 4. Job scheduler support

**Rust:** Internal `tokio`/`async` **does not** replace **cron** or **workflow engines**. Avoid growing a scheduler inside this repo.

**Python / ops:** **Airflow**, **Prefect**, **Dagster**, **systemd timers**, **Kubernetes CronJob** — schedule **processes** that call Rust or Python entrypoints.

**Good support:** Document **exit codes**, **idempotent** ingest patterns, and a **minimal** “run this binary with this config” contract — **not** shipping a scheduler.

---

## 5. Partitions support

**Clarify “partition”:**

- **In scope (single node):** **Partitioned reads** of *files* (hive-style `dt=YYYY-MM-DD/` layout), **bucketed** local processing, or **chunked** `DataSet` / lazy frame operations — all **without** a cluster coordinator.
- **Out of scope:** **Distributed** partitions (shuffle across workers) — that stays “not this library.”

**Rust:** Feasible to add **directory-aware** ingest and **parallel** file lists over a **local** or **object-store** listing (still single-process orchestration unless you integrate an external runner).

**Python:** Same API surface; maybe **Polars** scan patterns where we delegate.

---

## 6. Incremental load support

**Fits the ETL story well:** **watermarks** (high-water mark timestamps), **CDC**-style row images (you already have **boundary CDC types** in Python), **append-only** file sets, **merge** into a typed `DataSet`.

**Rust:** Core logic for **diff-friendly** ingest (e.g. “read only rows where `updated_at` > last_run”) belongs here if inputs are **files** or **DB** (with `db` feature).

**Not the library:** **Exactly-once** distributed pipelines, distributed locks — document **patterns** and let **Airflow/dbt** own orchestration.

---

## 7. Iceberg / Delta support

**Aligned with “not distributed” but “serious tables”:** Both have **Rust** ecosystem traction:

- **Apache Iceberg:** Rust readers/writers exist in the broader Arrow/Iceberg community (maturity varies by format version).
- **Delta Lake:** **`delta-rs`** (Rust) is widely used; Polars and Arrow interop are relevant.

**Reasonable Phase 2 scope:**

- **Read paths** (and maybe limited **append**) into **`DataSet`** / Arrow-backed flow — **single-node** scans, **not** replacing Spark for massive cluster shuffles.
- **Catalog / REST / full multi-engine transactions** — **not** core; **good support** = document **when** to use Spark/Databricks for catalog operations and this library for **local** or **single-driver** pulls.

**Python:** Often **`pyarrow`** + **`deltalake`** / Iceberg Python — a **thin** path: read Arrow → convert to rows/schema this library accepts (until native Rust path is worth it).

---

## 8. Airflow / dbt project structure support

**Not inside the Rust crate.** These are **repo layouts** and **orchestration** conventions.

**Good support:**

- **dbt:** **Python** models or **SQL** models** that call **`rust-data-processing`** (or a thin CLI) as a **transformation** step; document **vars** and **artifacts** (JSON reports as dbt artifacts or side files).
- **Airflow:** Example **DAG** that runs a **container** or **PythonOperator** with fixed CLI args; pass **connections** via env, not hard-coded in the library.

**Deliverable type:** `examples/airflow/` and `examples/dbt/` with **README only** in early Phase 2 — code as time allows.

---

## 9. Vector database integration (many DBs + clouds)

**Rust:** Pure Rust clients exist for some vector stores; coverage of **“most databases and clouds”** is **not** maintainable inside one data-prep crate.

**Python:** **SDK-heavy** — Pinecone, Weaviate, `pgvector`, OpenSearch, etc.

**Reasonable scope:**

- **Export:** embeddings + keys + metadata as **CSV/Parquet** or **`DataSet`** for bulk load tools.
- **Optional:** one **reference** integration (e.g. **pgvector** via `psycopg2` + `DataSet`) in **docs**, not nine cloud SDKs in-tree.

**Not the library:** Managed vector SaaS lifecycle — **good support** = **one** end-to-end recipe and a **matrix** in docs (“we test X; Y is community-contributed”).

---

## Suggested Phase 2 sequencing (draft)

1. **Partitions + incremental** (single-node semantics) — strengthens the core story without becoming “distributed.”
2. **Iceberg/Delta read path** — evaluate **`delta-rs` / Iceberg Rust** vs Arrow → `DataSet` bridge; start **narrow** (one format version).
3. **Notebooks + examples** — Python notebooks + one **“ML QA”** narrative notebook.
4. **Airflow/dbt examples** — documentation + minimal samples.
5. **Vector DB** — export patterns + one reference; avoid N SDKs in core.
6. **LLM / LTM** — thin **serialization + prompt helpers** only if demand is clear; otherwise stay **export JSON** only.

---

## Changelog

- Initial draft: Rust vs Python vs adjacent tooling; aligns with single-node, non-distributed scope.
