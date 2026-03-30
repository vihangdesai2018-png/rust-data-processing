# Python wrapper — PowerShell test runners

Run from anywhere; each script `cd`s to `python-wrapper` and uses `uv run pytest`.

| Script | Purpose |
|--------|---------|
| `Run-UnitTests.ps1` | `pytest -m "not deep and not benchmark"` |
| `Run-DeepTests.ps1` | `pytest -m deep` |
| `Run-BenchmarkTests.ps1` | `pytest -m benchmark` |

All scripts support:

- **`-Build`** — `uv run maturin develop --release` before pytest (needed after Rust changes).
- **`-PytestArgs`** — extra pytest flags, e.g. `-PytestArgs @('-v','--tb=long')`.

Examples:

```powershell
cd python-wrapper
.\scripts\Run-UnitTests.ps1
.\scripts\Run-DeepTests.ps1 -Build
.\scripts\Run-BenchmarkTests.ps1 -PytestArgs @('--benchmark-only')
```

Requires `uv` on PATH and a prior `uv sync --group dev` (or first `uv run` will sync).
