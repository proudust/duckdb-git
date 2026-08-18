//! BlobRing hit rate for a real `git_log` scan (`count(file_changes)`).
//!
//! ```bash
//! BENCH_REPO=/path/to/repo cargo bench --bench blob_ring_hits \
//!   --no-default-features --features bundled,libgit-backend
//! ```
//!
//! Optional: `ONESHOT_THREADS=1,4` (default `1,4`).

use duckdb::Connection;
use duckdb_git::microbench::{self, BlobRingStats};

fn repo_path() -> String {
    std::env::var("BENCH_REPO").unwrap_or_else(|_| ".".to_string())
}

fn thread_counts() -> Vec<usize> {
    std::env::var("ONESHOT_THREADS")
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|p| p.parse().ok())
                .filter(|n| *n > 0)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| vec![1, 4])
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn pct(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        100.0 * num as f64 / den as f64
    }
}

fn print_side(label: &str, lookups: u64, hits: u64, bytes: u64, hit_bytes: u64) {
    println!(
        "  {label:<12} lookups={lookups:<10} hits={hits:<10} {:>5.1}%   bytes={:>8.1} MiB  hit={:>8.1} MiB  {:>5.1}%",
        pct(hits, lookups),
        mib(bytes),
        mib(hit_bytes),
        pct(hit_bytes, bytes)
    );
}

fn print_stats(label: &str, n: i64, s: &BlobRingStats) {
    println!("\n--- {label}  commits_sql={n}  finishes={} ---", s.commits);
    print_side("all", s.lookups, s.hits, s.lookup_bytes, s.hit_bytes);
    print_side(
        "old",
        s.old_lookups,
        s.old_hits,
        s.old_bytes,
        s.old_hit_bytes,
    );
    print_side(
        "new",
        s.new_lookups,
        s.new_hits,
        s.new_bytes,
        s.new_hit_bytes,
    );
    print_side(
        "typechange",
        s.typechange_lookups,
        s.typechange_hits,
        s.typechange_bytes,
        s.typechange_hit_bytes,
    );
    println!(
        "  inserts={} ({:.1} MiB)  hit-bumps={}",
        s.inserts,
        mib(s.insert_bytes),
        s.bumps
    );
}

fn run(path: &str, threads: usize) -> (i64, BlobRingStats) {
    let db = Connection::open_in_memory().expect("duckdb");
    duckdb_git::register(&db).expect("register git_log");
    db.execute_batch(&format!("SET threads={threads}"))
        .expect("SET threads");

    microbench::reset_blob_ring_stats();
    let mut stmt = db
        .prepare("SELECT count(file_changes) FROM git_log(?, backend='libgit')")
        .unwrap();
    let n = stmt.query_row([path], |row| row.get::<_, i64>(0)).unwrap();
    let stats = microbench::snapshot_blob_ring_stats();
    (n, stats)
}

fn main() {
    let path = repo_path();
    println!("=== blob_ring_hits ===");
    println!("repo={path}");
    println!("BlobRing cap=32 MiB per DuckDB batch (fresh ring each git_log chunk)");
    println!(
        "stored cap 32 MiB × threads (max 4); pending copies / insert-before-evict can exceed it"
    );

    for threads in thread_counts() {
        let (n, stats) = run(&path, threads);
        print_stats(&format!("t{threads}"), n, &stats);
    }
}
