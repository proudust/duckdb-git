//! git_log scan diagnostics: Inline metadata (`count(*)`) and Prefetch with_diff.
//!
//! ```text
//! GIT_LOG_STATS=1 cargo run --example git_log_diag --release \
//!   --no-default-features \
//!   --features bundled,libgit-backend,gix-backend,git-log-stats \
//!   -- /path/to/repo
//! ```
//!
//! Env:
//! - `GIT_LOG_DIAG_REPO` — repo path if argv[1] omitted
//! - `GIT_LOG_STATS=1` — eprint on Inline scan completion or Prefetch buffer drop

use duckdb::Connection;
use duckdb_git::microbench::{reset_git_log_stats, snapshot_git_log_stats};
use std::time::Instant;

fn repo_path() -> String {
    std::env::args()
        .nth(1)
        .or_else(|| std::env::var("GIT_LOG_DIAG_REPO").ok())
        .or_else(|| std::env::var("BENCH_REPO").ok())
        .expect("pass repo path as argv[1] or set GIT_LOG_DIAG_REPO / BENCH_REPO")
}

fn setup(threads: usize) -> Connection {
    let db = Connection::open_in_memory().expect("duckdb");
    duckdb_git::register(&db).expect("register");
    db.execute_batch(&format!("SET threads={threads}"))
        .expect("SET threads");
    db
}

fn run_count(db: &Connection, path: &str, backend: &str) -> (i64, std::time::Duration) {
    reset_git_log_stats();
    let sql = format!("SELECT count(*) FROM git_log(?, backend='{backend}')");
    let t0 = Instant::now();
    let mut stmt = db.prepare(&sql).unwrap();
    let n: i64 = stmt.query_row([path], |row| row.get(0)).unwrap();
    let wall = t0.elapsed();
    // Inline dumps at scan exhaustion; Prefetch dumps when its buffer drops (often here).
    drop(stmt);
    (n, wall)
}

fn print_run(label: &str, path: &str, backend: &str, threads: usize) {
    let db = setup(threads);
    let (n, wall) = run_count(&db, path, backend);
    let stats = snapshot_git_log_stats();
    println!("=== {label} backend={backend} threads={threads} count={n} wall_ms={:.3} ===", wall.as_secs_f64() * 1000.0);
    println!("{}", stats.format_report());
    assert_eq!(
        stats.push_count, 0,
        "count(*) uses Inline engine (ring push_count should be 0, got {})",
        stats.push_count
    );
    println!(
        "\tderived: walk/wall={:.2} emit/wall={:.2}",
        (stats.walk_ns as f64) / (wall.as_nanos() as f64).max(1.0),
        (stats.emit_ns as f64) / (wall.as_nanos() as f64).max(1.0),
    );
}

fn find_commit_same_vs_split(path: &str) {
    use git2::{Oid, Repository};
    use std::collections::HashSet;

    println!("=== find_commit same-repo vs second Repository ({path}) ===");
    let repo_a = Repository::open(path).expect("open A");
    let mut revwalk = repo_a.revwalk().unwrap();
    revwalk.push_head().unwrap();
    let oids: Vec<Oid> = revwalk.take(5_000).map(|r| r.unwrap()).collect();
    assert!(!oids.is_empty());

    // Warm A by walking parents (similar to date-walk lookups).
    for &oid in &oids {
        let _ = repo_a.find_commit(oid).unwrap();
    }

    let t0 = Instant::now();
    for &oid in &oids {
        let _ = repo_a.find_commit(oid).unwrap();
    }
    let same_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let repo_b = Repository::open(path).expect("open B");
    let t1 = Instant::now();
    for &oid in &oids {
        let _ = repo_b.find_commit(oid).unwrap();
    }
    let split_cold_ms = t1.elapsed().as_secs_f64() * 1000.0;

    let t2 = Instant::now();
    for &oid in &oids {
        let _ = repo_b.find_commit(oid).unwrap();
    }
    let split_warm_ms = t2.elapsed().as_secs_f64() * 1000.0;

    // Distinct object pointers (and pack-cache state) across two Repository opens.
    let pa = &repo_a as *const _ as usize;
    let pb = &repo_b as *const _ as usize;
    let unique: HashSet<_> = oids.iter().copied().collect();
    println!(
        "\tn_oids={} unique={} repo_a={pa:#x} repo_b={pb:#x} same_ptr={}",
        oids.len(),
        unique.len(),
        pa == pb
    );
    println!(
        "\tsame_repo_hot_ms={same_ms:.3} second_repo_cold_ms={split_cold_ms:.3} second_repo_warm_ms={split_warm_ms:.3} cold/hot={:.2}x",
        split_cold_ms / same_ms.max(0.001)
    );
}

fn run_with_diff(db: &Connection, path: &str, backend: &str) -> (i64, std::time::Duration) {
    reset_git_log_stats();
    // Force file_changes projection (subquery+count(*) can drop it → Inline path).
    let sql = format!(
        "SELECT count(*) FROM git_log(?, backend='{backend}') WHERE len(file_changes) >= 0"
    );
    let t0 = Instant::now();
    let mut stmt = db.prepare(&sql).unwrap();
    let n: i64 = stmt.query_row([path], |row| row.get(0)).unwrap();
    let wall = t0.elapsed();
    drop(stmt);
    (n, wall)
}

fn print_with_diff(label: &str, path: &str, backend: &str, threads: usize) {
    let db = setup(threads);
    let (n, wall) = run_with_diff(&db, path, backend);
    let stats = snapshot_git_log_stats();
    println!(
        "=== {label} backend={backend} threads={threads} count={n} wall_ms={:.3} ===",
        wall.as_secs_f64() * 1000.0
    );
    println!("{}", stats.format_report());
    assert!(
        stats.push_count > 0,
        "expected Prefetch ring pushes, got push_count=0 (Inline?)"
    );
    assert_eq!(
        stats.emit_find_commit, 0,
        "Prefetch with_diff must not call emit find_commit (got {})",
        stats.emit_find_commit
    );
    assert!(
        stats.walker_find_commit > 0,
        "walker should find_commit during inspect (got {})",
        stats.walker_find_commit
    );
    println!(
        "\tok: emit_find_commit=0 walker_find_commit={} push_count={}",
        stats.walker_find_commit, stats.push_count
    );
}

fn main() {
    let path = repo_path();
    println!("repo={path}");

    find_commit_same_vs_split(&path);

    // Required matrix.
    print_run("git_log count(*)", &path, "libgit", 1);
    print_run("git_log count(*)", &path, "libgit", 4);
    print_run("git_log count(*)", &path, "gix", 1);
    // Optional control for gix inverse scaling.
    print_run("git_log count(*)", &path, "gix", 4);

    // Prefetch ring path (file_changes projected).
    print_with_diff("git_log with_diff", &path, "libgit", 1);
    print_with_diff("git_log with_diff", &path, "libgit", 4);
    print_with_diff("git_log with_diff", &path, "gix", 1);
    print_with_diff("git_log with_diff", &path, "gix", 4);
}
