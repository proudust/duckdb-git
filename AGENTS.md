# duckdb-git

DuckDB extension that exposes git repository data via SQL table functions. Written in Rust using `duckdb-rs` and `libgit2` (`git2` crate).

## Commands

```bash
make debug # Build (debug)
make release # Build (release)
make test # Run E2E tests (sqllogictest, default features = libgit-backend only)
make bench # Run benchmarks (cargo bench, requires --features bundled)
BENCH_REPO=/path/to/large/repo make bench # Run benchmarks on a specific repo
```

## Project structure

- `src/lib.rs` — DuckDB VTab implementation
- `src/git_log/mod.rs` — Shared types (`DiffMerges`, `FileChange`) + backend re-export
- `src/git_log/libgit.rs` — libgit2 backend (default)
- `src/git_log/gix.rs` — gix/gitoxide backend (experimental)
- `src/vector.rs` — DuckDB vector write helpers
- `test/sql/git_log.test` — E2E tests (sqllogictest format)
- `benches/git_log_with_duckdb.rs` — Benchmarks (`BENCH_REPO` env var selects the target repo)
