# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-03-31

### Changed

- (summarize this release)

## [0.1.2] - 2026-03-31

### Fixed

- **docs.rs / crates.io**: The published crate now ships **`README_CRATE.md`** only (Rust-focused). The monorepo **`README.md`** is excluded from the `.crate` tarball so docs.rs no longer shows Python quick starts or mixed Python/Rust landing copy. PyPI continues to use **`python-wrapper/README_PYPI.md`**.

## [0.1.1] - 2026-03-30

### Changed

- `scripts/release_tag.ps1`: optional `-Comment`, interactive release comment, prints last `v*` tag, fetches tags; clearer error text.
- CI: Documentation workflow uses `astral-sh/setup-uv@v8.0.0` and drops redundant `configure-pages` for static Pages deploy.

## [0.1.0] - 2026-03-20

### Added

- Initial crates.io release of `rust-data-processing`.
- Schema-first ingestion: CSV, JSON / NDJSON (nested dot paths), Parquet; Excel via `excel` feature.
- In-memory `DataSet` model (`types`) with `Int64`, `Float64`, `Bool`, `Utf8`, `Null`.
- `processing`: `filter`, `map`, `reduce` with `ReduceOp` (count, sum, min/max, mean, variance, std dev, sum-squares, L2 norm, count-distinct).
- `processing::multi`: `feature_wise_mean_std`, `arg_max_row`, `arg_min_row`, `top_k_by_frequency`.
- Polars-backed `pipeline::DataFrame` (lazy plan, `collect` to `DataSet`), `group_by` with `Agg`, joins, casts, filters.
- SQL over `DataFrame` (`sql` feature, default-on).
- `execution` engine: parallel filter/map, metrics, observers.
- `profiling`, `validation`, `outliers`, `transform` (TransformSpec), `cdc` boundary types.
- Optional `db_connectorx` for DB → Arrow → `DataSet` ingestion.

[0.1.3]: https://github.com/vihangdesai2018-png/rust-data-processing/releases/tag/v0.1.3
[0.1.2]: https://github.com/vihangdesai2018-png/rust-data-processing/releases/tag/v0.1.2
[0.1.1]: https://github.com/vihangdesai2018-png/rust-data-processing/releases/tag/v0.1.1
[0.1.0]: https://github.com/vihangdesai2018-png/rust-data-processing/releases/tag/v0.1.0
