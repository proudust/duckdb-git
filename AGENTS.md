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

## Cursor Cloud specific instructions

Build/test/lint commands are in the `## Commands` section and `README.md`; run them from the repo root. Notes below cover non-obvious caveats only.

- Rust toolchain: requires Rust >= 1.85 (dependency `duckdb-loadable-macros` needs `edition2024`). The VM's default `stable` toolchain is set via `rustup` and is new enough; the older system `rustc` at `/usr/local/cargo` is a `rustup` shim.
- The `extension-ci-tools` git submodule provides the build/test makefiles and is required before any `make` target. The update script initializes it, but if `make` errors with a missing include, run `git submodule update --init --recursive`.
- Lint: use `cargo clippy --lib` (and `cargo test --lib`). `cargo clippy --all-targets` fails because the `duckdb-git` example (`src/wasm_lib.rs`) only compiles under an emscripten/WASM target; CI excludes it. `cargo fmt --check` reports pre-existing diffs and is not enforced by CI.
- No standalone DuckDB CLI is installed. To manually load/run the extension, use the Python DuckDB in the configure venv (version matches `TARGET_DUCKDB_VERSION`): `./configure/venv/bin/python3` then `duckdb.connect(config={"allow_unsigned_extensions":"true"})` and `LOAD './build/debug/duckdb_git.duckdb_extension'`. The extension binary is ABI-pinned to its target DuckDB version, so it only loads into that exact version.
- `make test` runs libgit E2E tests only; gix tests are skipped unless run via `make test_gix` (which sets `GIX_BACKEND=1` and builds `--features gix-backend`).
