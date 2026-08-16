//! Is `VectorInserter::file_change` dominant vs full `git_log` with_diff?
//!
//! 1. Probes `BENCH_REPO` for commit / file_changes volume.
//! 2. Prints a one-shot grow vs reserve vs e2e timing summary (dominance %).
//! 3. Runs divan benches for the same comparisons.
//!
//! ```bash
//! BENCH_REPO=/path/to/repo cargo bench --bench file_changes_insert \
//!   --no-default-features --features bundled,libgit-backend
//! ```

use divan::AllocProfiler;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::Connection;
use std::sync::OnceLock;
use std::time::Instant;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn repo_path() -> String {
    std::env::var("BENCH_REPO").unwrap_or_else(|_| ".".to_string())
}

fn setup_duckdb() -> Connection {
    let db = Connection::open_in_memory().expect("failed to open duckdb");
    duckdb_git::register(&db).expect("failed to register git_log");
    db.execute_batch("SET threads=1")
        .expect("failed to set threads");
    db
}

#[derive(Clone, Copy, Debug)]
struct RepoVolume {
    commits: usize,
    file_changes: usize,
    /// Ceil average; slightly over-estimates insert work (conservative for
    /// "is insert dominant?").
    per_commit: usize,
    batch_rows: usize,
}

impl RepoVolume {
    fn probe(path: &str) -> Self {
        let db = setup_duckdb();
        let (commits, file_changes): (i64, i64) = db
            .query_row(
                "SELECT count(*)::BIGINT, coalesce(sum(len(file_changes)), 0)::BIGINT \
                 FROM git_log(?, backend='libgit')",
                [path],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("probe git_log volume");
        let commits = commits.max(0) as usize;
        let file_changes = file_changes.max(0) as usize;
        let per_commit = if commits == 0 {
            0
        } else {
            file_changes.div_ceil(commits).max(1)
        };
        // Match libgit scanner: batch_size = (commits / cores).clamp(1, 2048)
        // with threads=1 → up to 2048.
        let batch_rows = commits.clamp(1, 2048);
        Self {
            commits,
            file_changes,
            per_commit,
            batch_rows,
        }
    }

    fn cached() -> Self {
        static VOLUME: OnceLock<RepoVolume> = OnceLock::new();
        *VOLUME.get_or_init(|| Self::probe(&repo_path()))
    }
}

fn replay_insert(volume: RepoVolume, reserve: bool) {
    duckdb_git::microbench::fill_file_changes_history(
        volume.commits,
        volume.per_commit,
        reserve,
        volume.batch_rows,
    );
}

fn with_diff_e2e(path: &str) {
    let db = setup_duckdb();
    let mut stmt = db
        .prepare("SELECT count(file_changes) FROM git_log(?, backend='libgit')")
        .unwrap();
    let _: i64 = stmt.query_row([path], |row| row.get(0)).unwrap();
}

fn timed_ms(iters: u32, mut f: impl FnMut()) -> f64 {
    // Warmup
    f();
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed().as_secs_f64() * 1000.0 / f64::from(iters)
}

fn print_dominance_summary(path: &str, volume: RepoVolume) {
    println!("=== file_changes insert dominance ({path}) ===");
    println!(
        "volume: {} commits, {} file_changes (≈{} / commit), batch_rows={}",
        volume.commits, volume.file_changes, volume.per_commit, volume.batch_rows
    );

    // Full with_diff is expensive; fewer iters. Insert is cheap; more iters.
    let e2e_iters = 3;
    let insert_iters = 20;

    let grow_ms = timed_ms(insert_iters, || replay_insert(volume, false));
    let reserve_ms = timed_ms(insert_iters, || replay_insert(volume, true));
    let e2e_ms = timed_ms(e2e_iters, || with_diff_e2e(path));

    let grow_pct = 100.0 * grow_ms / e2e_ms;
    let reserve_pct = 100.0 * reserve_ms / e2e_ms;
    let save_pct = 100.0 * (grow_ms - reserve_ms) / e2e_ms;

    println!("insert_grow:     {grow_ms:.2} ms  ({grow_pct:.2}% of e2e)");
    println!("insert_reserve:  {reserve_ms:.2} ms  ({reserve_pct:.2}% of e2e)");
    println!("with_diff_e2e:   {e2e_ms:.2} ms");
    println!(
        "reserve saves:   {:.2} ms  ({save_pct:.2}% of e2e)",
        grow_ms - reserve_ms
    );

    let verdict = if grow_pct < 5.0 {
        "insert NOT dominant — further insert-path polish (root/gix begin, struct_child tidy) is low priority"
    } else if grow_pct < 20.0 {
        "insert is a modest fraction — polish only if reserve delta is large"
    } else {
        "insert is material — worth further insert-path optimization"
    };
    println!("verdict: {verdict}");
    println!();
}

fn main() {
    let path = repo_path();
    let volume = RepoVolume::cached();
    print_dominance_summary(&path, volume);
    // Filter example (skip synthetic microbenches after the summary):
    //   cargo bench --bench file_changes_insert -- 'insert_replay_repo|with_diff_repo'
    divan::main();
}

#[derive(Clone, Copy, Debug)]
struct Config {
    /// Rows in one DuckDB chunk.
    rows: usize,
    /// `file_changes` entries per row.
    per_row: usize,
    /// Call `begin_file_changes(per_row)` before inserting.
    reserve: bool,
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = if self.reserve { "reserve" } else { "grow" };
        write!(f, "{mode}_{}x{}", self.rows, self.per_row)
    }
}

const CONFIGS: &[Config] = &[
    Config {
        rows: 64,
        per_row: 8,
        reserve: false,
    },
    Config {
        rows: 64,
        per_row: 8,
        reserve: true,
    },
    Config {
        rows: 64,
        per_row: 64,
        reserve: false,
    },
    Config {
        rows: 64,
        per_row: 64,
        reserve: true,
    },
    Config {
        rows: 64,
        per_row: 256,
        reserve: false,
    },
    Config {
        rows: 64,
        per_row: 256,
        reserve: true,
    },
    Config {
        rows: 32,
        per_row: 1024,
        reserve: false,
    },
    Config {
        rows: 32,
        per_row: 1024,
        reserve: true,
    },
    Config {
        rows: 16,
        per_row: 4096,
        reserve: false,
    },
    Config {
        rows: 16,
        per_row: 4096,
        reserve: true,
    },
];

#[divan::bench(args = CONFIGS, sample_count = 50)]
fn insert_file_changes(bencher: divan::Bencher, config: Config) {
    bencher.bench_local(|| {
        duckdb_git::microbench::fill_file_changes_chunk(
            config.rows,
            config.per_row,
            config.reserve,
        );
    });
}

#[derive(Clone, Copy, Debug)]
struct ReplayMode {
    reserve: bool,
}

impl std::fmt::Display for ReplayMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.reserve { "reserve" } else { "grow" })
    }
}

const REPLAY_MODES: &[ReplayMode] = &[
    ReplayMode { reserve: false },
    ReplayMode { reserve: true },
];

/// Full-history insert replay sized from `BENCH_REPO`.
#[divan::bench(args = REPLAY_MODES, sample_count = 20)]
fn insert_replay_repo(bencher: divan::Bencher, mode: ReplayMode) {
    let volume = RepoVolume::cached();
    bencher.bench_local(|| {
        replay_insert(volume, mode.reserve);
    });
}

#[divan::bench(sample_count = 10)]
fn with_diff_repo(bencher: divan::Bencher) {
    let path = repo_path();
    let db = setup_duckdb();
    let sql = "SELECT count(file_changes) FROM git_log(?, backend='libgit')";
    bencher.bench_local(|| {
        let mut stmt = db.prepare(sql).unwrap();
        let _: i64 = stmt.query_row([&path], |row| row.get(0)).unwrap();
    });
}

/// LIST(STRUCT) child growth only — no VARCHAR inserts.
///
/// Measures the cost that `begin_file_changes` is meant to amortize.
#[derive(Clone, Copy, Debug)]
struct ReserveConfig {
    total: usize,
    reserve_upfront: bool,
}

impl std::fmt::Display for ReserveConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode = if self.reserve_upfront {
            "upfront"
        } else {
            "incremental"
        };
        write!(f, "{mode}_{}", self.total)
    }
}

const RESERVE_CONFIGS: &[ReserveConfig] = &[
    ReserveConfig {
        total: 256,
        reserve_upfront: false,
    },
    ReserveConfig {
        total: 256,
        reserve_upfront: true,
    },
    ReserveConfig {
        total: 1024,
        reserve_upfront: false,
    },
    ReserveConfig {
        total: 1024,
        reserve_upfront: true,
    },
    ReserveConfig {
        total: 4096,
        reserve_upfront: false,
    },
    ReserveConfig {
        total: 4096,
        reserve_upfront: true,
    },
    ReserveConfig {
        total: 16384,
        reserve_upfront: false,
    },
    ReserveConfig {
        total: 16384,
        reserve_upfront: true,
    },
    ReserveConfig {
        total: 65536,
        reserve_upfront: false,
    },
    ReserveConfig {
        total: 65536,
        reserve_upfront: true,
    },
];

fn struct_list_type() -> LogicalTypeHandle {
    let child = LogicalTypeHandle::struct_type(&[
        ("path", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("old_path", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("status", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("blob_id", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("file_size", LogicalTypeHandle::from(LogicalTypeId::Bigint)),
        ("add_lines", LogicalTypeHandle::from(LogicalTypeId::Integer)),
        ("del_lines", LogicalTypeHandle::from(LogicalTypeId::Integer)),
    ]);
    LogicalTypeHandle::list(&child)
}

#[divan::bench(args = RESERVE_CONFIGS, sample_count = 100)]
fn list_struct_child_reserve(bencher: divan::Bencher, config: ReserveConfig) {
    bencher.bench_local(|| {
        let chunk = DataChunkHandle::new(&[struct_list_type()]);
        let list = chunk.list_vector(0);
        if config.reserve_upfront {
            let _ = list.struct_child(config.total);
        } else {
            for n in 1..=config.total {
                let _ = list.struct_child(n);
            }
        }
    });
}
