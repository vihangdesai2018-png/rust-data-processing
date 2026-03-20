# Deploy: crates.io + PyPI (Rust + Python wrapper)

This project is primarily a Rust library (publish to **crates.io**) and (Phase 1a) will add a Python wrapper (publish to **PyPI**) using **PyO3 + maturin**.

---

## Deploy to crates.io (Rust)

Step-by-step checklist: **`Planning/RELEASE_CHECKLIST.md`**.

### Prerequisites checklist
- `Cargo.toml` contains correct **name**, **version**, **description**, **license** (`MIT OR Apache-2.0`), **repository**, **readme**, **keywords**, **categories**, **rust-version**
- README is accurate and includes feature flags + basic examples; **License** section points at `LICENSE-MIT` / `LICENSE-APACHE`
- Licensing files are present (`LICENSE-MIT`, `LICENSE-APACHE`)
- `CHANGELOG.md` updated for the version you intend to publish
- You can build and test locally (including doctests if used); `cargo publish --dry-run` succeeds

### Publish steps
1. Create/login to crates.io, then:

```bash
cargo login <CRATES_IO_TOKEN>
```

2. Dry-run publish (recommended):

```bash
cargo publish --dry-run
```

3. Publish:

```bash
cargo publish
```

### Release hygiene (recommended)
- Tag the release in git (e.g. `v0.1.0`)
- Write release notes (GitHub Releases or `CHANGELOG.md`)
- Follow SemVer for breaking API changes

---

## Deploy to PyPI (Python wrapper)

### Recommended approach: PyO3 + maturin (no `setup.py`)
Modern Rust-backed Python packages typically use a `pyproject.toml` + maturin build flow.

High-level plan:
- Create a `python/` folder containing the Python package + maturin config
- Use PyO3 to expose a Python module implemented in Rust
- Use maturin to build wheels for Windows/macOS/Linux and publish to PyPI

### Minimal repository layout (Phase 1a target)
- `python/pyproject.toml` (maturin config + Python metadata)
- `python/rust_data_processing/__init__.py` (thin Python API surface)
- `python/src/lib.rs` (PyO3 module entrypoint; calls into the Rust crate)

### Local development workflow
From `python/`:

```bash
maturin develop
```

This builds the Rust extension and installs it into your active Python environment.

### Build wheels locally
From `python/`:

```bash
maturin build --release
```

Wheels land in `python/target/wheels/` by default.

### CI/CD: build + publish wheels with GitHub Actions
Use the official action: `PyO3/maturin-action` to build wheels for:
- Windows
- macOS
- Linux (manylinux)

Maturin can generate a starter workflow:

```bash
maturin generate-ci github
```

Recommended publishing flow:
- On git tag (e.g. `v0.1.0`), build wheels in CI
- Upload wheels + sdist to PyPI using an API token stored in GitHub Secrets

### Versioning recommendations
- Easiest for Phase 1a: keep the **Python package version aligned** with the Rust crate version.
- Document which Rust features are enabled/disabled in Python wheels (e.g. optional DB ingestion).

---

## Reporting bugs
- Use GitHub Issues and link it from README
- Add a bug report template (optional but helpful)
