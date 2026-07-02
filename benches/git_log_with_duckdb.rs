//! Benchmarks for the git_log function provided by this extension.
//! Use the `BENCH_REPO` environment variable to configure the target repository.
//!
//! Measures execution time and memory allocation across different query scenarios:
//! - metadata_only: core revwalk + metadata reading
//! - with_diff: diff computation
//! - limit_10: LIMIT query performance

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

#[divan::bench(args = [1, 2, 4], sample_count = 10)]
fn metadata_only(bencher: divan::Bencher, threads: usize) {
    let path = repo_path();
    let db = setup_duckdb(threads);
    bencher.bench_local(|| {
        let mut stmt = db.prepare("SELECT count(*) FROM git_log(?)").unwrap();
        stmt.query_row([&path], |row| row.get::<_, i64>(0)).unwrap()
    });
}

#[divan::bench(args = [1, 2, 4], sample_count = 10)]
fn with_diff(bencher: divan::Bencher, threads: usize) {
    let path = repo_path();
    let db = setup_duckdb(threads);
    bencher.bench_local(|| {
        let mut stmt = db
            .prepare("SELECT count(*) FROM git_log(?, diff_merges='first-parent')")
            .unwrap();
        stmt.query_row([&path], |row| row.get::<_, i64>(0)).unwrap()
    });
}

#[divan::bench(sample_count = 10)]
fn limit_10(bencher: divan::Bencher) {
    let path = repo_path();
    let db = setup_duckdb(1);
    bencher.bench_local(|| {
        let mut stmt = db.prepare("SELECT * FROM git_log(?) LIMIT 10").unwrap();
        let mut rows = stmt.query([&path]).unwrap();
        let mut count = 0;
        while let Some(_) = rows.next().unwrap() {
            count += 1;
        }
        count
    });
}
