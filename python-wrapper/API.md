# Python API overview (`rust_data_processing`)

High-level reference for the bindings in `python-wrapper/`. The underlying behavior matches the Rust crate (see repository root `API.md` and `README.md`).

## Module: `rust_data_processing`

### Version

- **`__version__`**: PEP 440 version from package metadata when installed; falls back to **`extension_version()`** if metadata is missing (e.g. bare extension on `PYTHONPATH`).
- **`extension_version()`** → `str`: version of the PyO3 extension crate (`python-wrapper/Cargo.toml`).

### Class: `DataSet`

Constructor: **`DataSet(schema, rows)`**

- **`schema`**: `list[dict]` with keys `name` (`str`) and `data_type` (`"int64"` \| `"float64"` \| `"bool"` \| `"utf8"`; aliases like `"string"` accepted where noted in Rust docs).
- **`rows`**: `list[list]` of Python values aligned to schema order. Use `None` for null cells.

Methods:

- **`row_count()`** → `int`
- **`column_names()`** → `list[str]`
- **`schema()`** → `list[dict]` (same shape as input schema descriptors)
- **`to_rows()`** → `list[list]` of scalars / `None`

### Ingestion

- **`ingest_from_path(path, schema, options=None)`** → `DataSet`  
  Schema-driven ingest. `path` is a filesystem path string.

- **`infer_schema_from_path(path, options=None)`** → `list[dict]`  
  Best-effort inferred schema (same dict shape as `DataSet` schema).

- **`ingest_from_path_infer(path, options=None)`** → `DataSet`  
  Infer schema then ingest.

**`options`** (optional `dict`):

- **`format`**: `"csv"`, `"json"` / `"ndjson"`, `"parquet"` / `"pq"`, or `"excel"` (and common aliases). If omitted, format is inferred from the file extension.
- **`excel_sheet_selection`**: `dict` with:
  - **`mode`**: `"first"` \| `"all"` \| `"sheet"` \| `"sheets"`
  - **`name`**: sheet name (required for `"sheet"`)
  - **`names`**: list of sheet names (required for `"sheets"`)

### Errors

Failures from the Rust layer are raised as **`ValueError`** (most ingestion errors) or **`OSError`** / **`BlockingIOError`**-style subclasses where the Rust error maps to I/O (`PermissionDenied`, etc. are surfaced as `OSError` with message).

## Not wrapped yet (Phase 1a roadmap)

Processing (`filter` / `map` / `reduce`), Polars **`DataFrame`** pipeline, SQL, transform specs, profiling, validation, outliers, and the execution engine are specified in `Planning/PHASE1A_PLAN.md` §2.2.2. They will be added incrementally on top of this layout.
