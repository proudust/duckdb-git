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

- `src/lib.rs` — extension entry point (`register`, `extension_entrypoint`)
- `src/git_log/mod.rs` — `git_log` VTab, `open_planner` backend dispatch
- `src/git_log/types.rs` — `CommitData`, `GitLogReadPlanner`/`GitLogReader` traits
- `src/git_log/params.rs` — `GitLogParameter`, `BackendKind`, `DecorateFormat`
- `src/git_log/schema.rs` — column definitions and projection helpers
- `src/git_log/vector.rs` — DuckDB vector write helpers
- `src/git_log/libgit.rs` — `LibGitLogReadPlanner`/`LibGitLogReader` (default)
- `src/git_log/xdiff.rs` — `xdl_diff` FFI bindings for fast add/del line counting (libgit-backend only)
- `src/git_log/gix.rs` — `GixLogReadPlanner`/`GixLogReader` (experimental, feature-gated)
- `src/wasm_lib.rs` — wasm entry point (swaps crate-type for emscripten builds)
- `test/sql/libgit/` — libgit backend E2E tests (sqllogictest format)
- `test/sql/gix/` — gix backend E2E tests (gated by `require-env GIX_BACKEND 1`)
- `benches/git_log_with_duckdb.rs` — Benchmarks (`BENCH_REPO` env var selects the target repo)
