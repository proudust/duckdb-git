# duckdb-git

DuckDB extension that exposes git repository data via SQL table functions. Written in Rust using `duckdb-rs`, with a default `libgit2` (`git2` crate) backend and an optional `gix` (gitoxide) backend.

## Commands

```bash
make debug # Build (debug, libgit-only binary)
make debug_gix # Build (debug, libgit + gix)
make release # Build (release, libgit-only binary)
make release_gix # Build (release, libgit + gix)
make test # E2E tests (sqllogictest, libgit-only binary)
make test_gix # E2E tests (sqllogictest, libgit + gix)
make bench # Run benchmarks (libgit + gix backends)
BENCH_REPO=/path/to/large/repo make bench # Run benchmarks on a specific repo
make bench_baseline # Run benchmarks and save results as baseline
make bench_compare # Re-run benchmarks and print before/after comparison table
```

## Project structure

- `src/git/` — git domain (shared types + backend leaf ops under `backend/{libgit,gix}/`)
- `src/git_log/` — `git_log` DuckDB table function (VTab, params, scanners)
- `test/sql/{libgit,gix}/` — E2E tests (sqllogictest; gix gated by `GIX_BACKEND=1`)
- `benches/` — benchmarks (`BENCH_REPO` selects the target repo)
