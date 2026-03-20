# Release checklist (Rust crate → crates.io)

Use this for **§2.1** of `PHASE1A_PLAN.md`. Python / PyPI is **§2.2** (separate).

## Before the first (or any) publish

1. **Version** — Bump `version` in `Cargo.toml` (SemVer). For breaking API changes, bump **minor** (0.x) or **major** (1.x+) per your policy.
2. **Changelog** — Add a section under `CHANGELOG.md` for the new version (see [Keep a Changelog](https://keepachangelog.com/)).
3. **CI green** — `cargo fmt --check`, `cargo clippy`, `cargo test`, `cargo test --doc` (and any feature matrices you care about, e.g. `--all-features` if applicable).
4. **Dry run** — From the repo root:

   ```bash
   cargo publish --dry-run
   ```

   Fix any packaging errors (missing files, invalid `license` / `categories`, oversized crate, etc.).

5. **Login** (one-time per machine):

   ```bash
   cargo login <CRATES_IO_TOKEN>
   ```

## Publish

```bash
cargo publish
```

crates.io does not allow republishing the same version; if this fails with “already uploaded”, bump the version and repeat.

## After publish

1. **Git tag** — Tag the commit that matches the published version, e.g. `v0.1.0`:

   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

2. **GitHub Release** — Create a release from that tag and paste the relevant `CHANGELOG.md` section (or link to the file).

3. **docs.rs** — Builds automatically for published crates; ensure `README.md` and public docs are accurate.

## Crate metadata (maintainers)

Canonical fields live in **`Cargo.toml`**: `description`, `license`, `repository`, `readme`, `keywords`, `categories`, `rust-version`.

License files: **`LICENSE-MIT`**, **`LICENSE-APACHE`** (dual **MIT OR Apache-2.0**).

More detail: `How_TO_deploy.md` and `API.md`.
