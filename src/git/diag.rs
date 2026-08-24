//! Temporary diagnostics for git_log prefetch regressions (`prefetch-stats` feature).
//!
//! Enable with `--features prefetch-stats` and optionally `GIT_LOG_PREFETCH_STATS=1`
//! to eprint a snapshot when a scan finishes (buffer drop).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

fn env_dump_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var_os("GIT_LOG_PREFETCH_STATS")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

#[derive(Debug, Clone, Default)]
pub struct PrefetchStats {
    pub push_count: u64,
    pub full_blocks: u64,
    pub full_wait_ns: u64,
    pub take_batch_count: u64,
    pub empty_blocks: u64,
    pub empty_wait_ns: u64,
    pub walk_ns: u64,
    pub take_batch_ns: u64,
    pub emit_ns: u64,
    pub read_ns: u64,
    pub walker_tid: u64,
    pub first_read_tid: u64,
    pub unique_read_threads: u64,
    pub walker_repo_ptr: usize,
    pub first_emit_repo_ptr: usize,
    pub walker_find_commit: u64,
    pub emit_find_commit: u64,
    pub cached_repo_hits: u64,
    pub cached_repo_misses: u64,
}

impl PrefetchStats {
    pub fn format_report(&self) -> String {
        let same_thread = self.walker_tid != 0
            && self.first_read_tid != 0
            && self.walker_tid == self.first_read_tid;
        let same_repo = self.walker_repo_ptr != 0
            && self.first_emit_repo_ptr != 0
            && self.walker_repo_ptr == self.first_emit_repo_ptr;
        format!(
            "prefetch-stats\n\
             \tpush_count={}\n\
             \tfull_blocks={} full_wait_ms={:.3}\n\
             \ttake_batch_count={} empty_blocks={} empty_wait_ms={:.3}\n\
             \twalk_ms={:.3} take_batch_ms={:.3} emit_ms={:.3} read_ms={:.3}\n\
             \twalker_tid={} first_read_tid={} same_thread={} unique_read_threads={}\n\
             \twalker_repo={:#x} emit_repo={:#x} same_repo={}\n\
             \tfind_commit walker={} emit={}\n\
             \tcached_repo hits={} misses={}",
            self.push_count,
            self.full_blocks,
            ns_ms(self.full_wait_ns),
            self.take_batch_count,
            self.empty_blocks,
            ns_ms(self.empty_wait_ns),
            ns_ms(self.walk_ns),
            ns_ms(self.take_batch_ns),
            ns_ms(self.emit_ns),
            ns_ms(self.read_ns),
            self.walker_tid,
            self.first_read_tid,
            same_thread,
            self.unique_read_threads,
            self.walker_repo_ptr,
            self.first_emit_repo_ptr,
            same_repo,
            self.walker_find_commit,
            self.emit_find_commit,
            self.cached_repo_hits,
            self.cached_repo_misses,
        )
    }
}

fn ns_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

static PUSH_COUNT: AtomicU64 = AtomicU64::new(0);
static FULL_BLOCKS: AtomicU64 = AtomicU64::new(0);
static FULL_WAIT_NS: AtomicU64 = AtomicU64::new(0);
static TAKE_BATCH_COUNT: AtomicU64 = AtomicU64::new(0);
static EMPTY_BLOCKS: AtomicU64 = AtomicU64::new(0);
static EMPTY_WAIT_NS: AtomicU64 = AtomicU64::new(0);
static WALK_NS: AtomicU64 = AtomicU64::new(0);
static TAKE_BATCH_NS: AtomicU64 = AtomicU64::new(0);
static EMIT_NS: AtomicU64 = AtomicU64::new(0);
static READ_NS: AtomicU64 = AtomicU64::new(0);
static WALKER_TID: AtomicU64 = AtomicU64::new(0);
static FIRST_READ_TID: AtomicU64 = AtomicU64::new(0);
static UNIQUE_READ_THREADS: AtomicU64 = AtomicU64::new(0);
static WALKER_REPO: AtomicUsize = AtomicUsize::new(0);
static EMIT_REPO: AtomicUsize = AtomicUsize::new(0);
static WALKER_FIND: AtomicU64 = AtomicU64::new(0);
static EMIT_FIND: AtomicU64 = AtomicU64::new(0);
static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

static READ_TIDS: Mutex<Option<HashSet<u64>>> = Mutex::new(None);

pub fn reset_prefetch_stats() {
    PUSH_COUNT.store(0, Ordering::Relaxed);
    FULL_BLOCKS.store(0, Ordering::Relaxed);
    FULL_WAIT_NS.store(0, Ordering::Relaxed);
    TAKE_BATCH_COUNT.store(0, Ordering::Relaxed);
    EMPTY_BLOCKS.store(0, Ordering::Relaxed);
    EMPTY_WAIT_NS.store(0, Ordering::Relaxed);
    WALK_NS.store(0, Ordering::Relaxed);
    TAKE_BATCH_NS.store(0, Ordering::Relaxed);
    EMIT_NS.store(0, Ordering::Relaxed);
    READ_NS.store(0, Ordering::Relaxed);
    WALKER_TID.store(0, Ordering::Relaxed);
    FIRST_READ_TID.store(0, Ordering::Relaxed);
    UNIQUE_READ_THREADS.store(0, Ordering::Relaxed);
    WALKER_REPO.store(0, Ordering::Relaxed);
    EMIT_REPO.store(0, Ordering::Relaxed);
    WALKER_FIND.store(0, Ordering::Relaxed);
    EMIT_FIND.store(0, Ordering::Relaxed);
    CACHE_HITS.store(0, Ordering::Relaxed);
    CACHE_MISSES.store(0, Ordering::Relaxed);
    *READ_TIDS.lock().unwrap() = Some(HashSet::new());
}

pub fn snapshot_prefetch_stats() -> PrefetchStats {
    PrefetchStats {
        push_count: PUSH_COUNT.load(Ordering::Relaxed),
        full_blocks: FULL_BLOCKS.load(Ordering::Relaxed),
        full_wait_ns: FULL_WAIT_NS.load(Ordering::Relaxed),
        take_batch_count: TAKE_BATCH_COUNT.load(Ordering::Relaxed),
        empty_blocks: EMPTY_BLOCKS.load(Ordering::Relaxed),
        empty_wait_ns: EMPTY_WAIT_NS.load(Ordering::Relaxed),
        walk_ns: WALK_NS.load(Ordering::Relaxed),
        take_batch_ns: TAKE_BATCH_NS.load(Ordering::Relaxed),
        emit_ns: EMIT_NS.load(Ordering::Relaxed),
        read_ns: READ_NS.load(Ordering::Relaxed),
        walker_tid: WALKER_TID.load(Ordering::Relaxed),
        first_read_tid: FIRST_READ_TID.load(Ordering::Relaxed),
        unique_read_threads: UNIQUE_READ_THREADS.load(Ordering::Relaxed),
        walker_repo_ptr: WALKER_REPO.load(Ordering::Relaxed),
        first_emit_repo_ptr: EMIT_REPO.load(Ordering::Relaxed),
        walker_find_commit: WALKER_FIND.load(Ordering::Relaxed),
        emit_find_commit: EMIT_FIND.load(Ordering::Relaxed),
        cached_repo_hits: CACHE_HITS.load(Ordering::Relaxed),
        cached_repo_misses: CACHE_MISSES.load(Ordering::Relaxed),
    }
}

pub fn dump_prefetch_stats_if_env() {
    if env_dump_enabled() {
        eprintln!("{}", snapshot_prefetch_stats().format_report());
    }
}

pub fn thread_id_bits() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::thread::current().id().hash(&mut h);
    h.finish()
}

pub fn record_push() {
    PUSH_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn record_full_wait(d: Duration) {
    FULL_BLOCKS.fetch_add(1, Ordering::Relaxed);
    FULL_WAIT_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

pub fn record_take_batch(d: Duration) {
    TAKE_BATCH_COUNT.fetch_add(1, Ordering::Relaxed);
    TAKE_BATCH_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

pub fn record_empty_wait(d: Duration) {
    EMPTY_BLOCKS.fetch_add(1, Ordering::Relaxed);
    EMPTY_WAIT_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

pub fn record_walk(d: Duration) {
    WALK_NS.store(d.as_nanos() as u64, Ordering::Relaxed);
}

pub fn record_emit(d: Duration) {
    EMIT_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

pub fn record_read(d: Duration) {
    READ_NS.fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
}

pub fn record_walker_identity(tid: u64, repo_ptr: usize) {
    WALKER_TID.store(tid, Ordering::Relaxed);
    WALKER_REPO.store(repo_ptr, Ordering::Relaxed);
}

pub fn record_read_identity(tid: u64, repo_ptr: usize) {
    let _ = FIRST_READ_TID.compare_exchange(0, tid, Ordering::Relaxed, Ordering::Relaxed);
    let _ = EMIT_REPO.compare_exchange(0, repo_ptr, Ordering::Relaxed, Ordering::Relaxed);
    let mut guard = READ_TIDS.lock().unwrap();
    let set = guard.get_or_insert_with(HashSet::new);
    if set.insert(tid) {
        UNIQUE_READ_THREADS.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn record_walker_find_commit() {
    WALKER_FIND.fetch_add(1, Ordering::Relaxed);
}

pub fn record_emit_find_commit() {
    EMIT_FIND.fetch_add(1, Ordering::Relaxed);
}

pub fn record_cached_repo_open(from_cache: bool) {
    if from_cache {
        CACHE_HITS.fetch_add(1, Ordering::Relaxed);
    } else {
        CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    }
}
