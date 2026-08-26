//! Benchmarks for the git_log function provided by this extension.
//! Use the `BENCH_REPO` environment variable to configure the target repository.
//!
//! Measures execution time and memory allocation across different query scenarios:
//! - metadata_only: core revwalk + metadata reading (`count(*)` → commit_id-only projection)
//! - metadata_wide: wide metadata projection (author/message/parents; emit-cache path)
//! - with_diff: diff computation
//! - limit_10: `SELECT *` LIMIT (with_diff / Prefetch path; early stop via buffer cancel)
//!
//! Each scenario is run for `backend='libgit'` and, when compiled with `gix-backend`,
//! `backend='gix'`.

use duckdb::Connection;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

fn repo_path() -> String {
    std::env::var("BENCH_REPO").unwrap_or_else(|_| ".".to_string())
}

fn setup_duckdb(threads: usize) -> Connection {
    let db = Connection::open_in_memory().expect("failed to open duckdb");
    duckdb_git::register(&db).expect("failed to register git_log");
    db.execute_batch(&format!("SET threads={threads}"))
        .expect("failed to set threads");
    db
}

fn git_log_sql(suffix: &str, backend: &str) -> String {
    format!("SELECT {suffix} FROM git_log(?, backend='{backend}')")
}

#[derive(Clone, Copy, Debug)]
struct ThreadedConfig {
    backend: &'static str,
    threads: usize,
}

impl std::fmt::Display for ThreadedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_t{}", self.backend, self.threads)
    }
}

#[cfg(feature = "gix-backend")]
const THREADED_CONFIGS: &[ThreadedConfig] = &[
    ThreadedConfig {
        backend: "libgit",
        threads: 1,
    },
    ThreadedConfig {
        backend: "libgit",
        threads: 2,
    },
    ThreadedConfig {
        backend: "libgit",
        threads: 4,
    },
    ThreadedConfig {
        backend: "gix",
        threads: 1,
    },
    ThreadedConfig {
        backend: "gix",
        threads: 2,
    },
    ThreadedConfig {
        backend: "gix",
        threads: 4,
    },
];
#[cfg(not(feature = "gix-backend"))]
const THREADED_CONFIGS: &[ThreadedConfig] = &[
    ThreadedConfig {
        backend: "libgit",
        threads: 1,
    },
    ThreadedConfig {
        backend: "libgit",
        threads: 2,
    },
    ThreadedConfig {
        backend: "libgit",
        threads: 4,
    },
];

#[cfg(feature = "gix-backend")]
const BACKENDS: &[&str] = &["libgit", "gix"];
#[cfg(not(feature = "gix-backend"))]
const BACKENDS: &[&str] = &["libgit"];

#[divan::bench(args = THREADED_CONFIGS, sample_count = 10)]
fn metadata_only(bencher: divan::Bencher, config: ThreadedConfig) {
    let path = repo_path();
    let db = setup_duckdb(config.threads);
    let sql = git_log_sql("count(*)", config.backend);
    bencher.bench_local(|| {
        let mut stmt = db.prepare(&sql).unwrap();
        stmt.query_row([&path], |row| row.get::<_, i64>(0)).unwrap()
    });
}

/// Forces author / message / parents projection so `MetaProjection::needs_emit_cache()` is true
/// (unlike `metadata_only`'s `count(*)`, which typically projects only `commit_id`).
/// Note: a subquery `count(*)` over a wide SELECT is insufficient — DuckDB prunes unused columns.
#[divan::bench(args = THREADED_CONFIGS, sample_count = 10)]
fn metadata_wide(bencher: divan::Bencher, config: ThreadedConfig) {
    let path = repo_path();
    let db = setup_duckdb(config.threads);
    let sql = git_log_sql(
        "count(message), count(author), count(len(parents))",
        config.backend,
    );
    bencher.bench_local(|| {
        let mut stmt = db.prepare(&sql).unwrap();
        stmt.query_row([&path], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .unwrap()
    });
}

#[divan::bench(args = THREADED_CONFIGS, sample_count = 10)]
fn with_diff(bencher: divan::Bencher, config: ThreadedConfig) {
    let path = repo_path();
    let db = setup_duckdb(config.threads);
    let sql = git_log_sql("count(file_changes)", config.backend);
    bencher.bench_local(|| {
        let mut stmt = db.prepare(&sql).unwrap();
        stmt.query_row([&path], |row| row.get::<_, i64>(0)).unwrap()
    });
}

#[divan::bench(args = BACKENDS, sample_count = 10)]
fn limit_10(bencher: divan::Bencher, backend: &str) {
    let path = repo_path();
    let db = setup_duckdb(1);
    let sql = format!("SELECT * FROM git_log(?, backend='{backend}') LIMIT 10");
    bencher.bench_local(|| {
        let mut stmt = db.prepare(&sql).unwrap();
        let mut rows = stmt.query([&path]).unwrap();
        let mut count = 0;
        while rows.next().unwrap().is_some() {
            count += 1;
        }
        count
    });
}
