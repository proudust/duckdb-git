//! Bounded prefetch ring between a single walker thread and parallel emit workers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
#[cfg(feature = "git-log-stats")]
use std::time::Instant;

/// Ring depth for prefetch (also caps in-flight window with batch reads).
pub const RING_CAPACITY: usize = 2048;

/// Max items per `read()` batch when the ring has enough ready.
///
/// Kept smaller than [`RING_CAPACITY`] so multiple DuckDB workers can share the
/// ring on `with_diff` scans; large batches let one thread drain and starve peers.
pub const READ_BATCH_SIZE: usize = 128;

/// Soft cap on DuckDB worker threads for libgit scans (BlobRing RSS).
pub const MAX_LIBGIT_THREADS: usize = 4;

struct Inner<T> {
    queue: Mutex<VecDeque<T>>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity: usize,
    finished: AtomicBool,
    cancelled: AtomicBool,
    error: Mutex<Option<String>>,
    pushed: AtomicUsize,
    walker_join: Mutex<Option<JoinHandle<()>>>,
}

impl<T> Inner<T> {
    fn push(&self, item: T) -> bool {
        if self.cancelled.load(Ordering::Acquire) {
            return false;
        }
        let mut queue = self.queue.lock().unwrap();
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                return false;
            }
            if self.error.lock().unwrap().is_some() {
                return false;
            }
            if queue.len() < self.capacity {
                queue.push_back(item);
                self.pushed.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "git-log-stats")]
                crate::git::diag::record_push();
                self.not_empty.notify_one();
                return true;
            }
            #[cfg(feature = "git-log-stats")]
            let t0 = Instant::now();
            queue = self.not_full.wait(queue).unwrap();
            #[cfg(feature = "git-log-stats")]
            crate::git::diag::record_full_wait(t0.elapsed());
        }
    }

    fn set_error(&self, message: String) {
        let _guard = self.queue.lock().unwrap();
        *self.error.lock().unwrap() = Some(message);
        self.cancelled.store(true, Ordering::Release);
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    fn mark_finished(&self) {
        let _guard = self.queue.lock().unwrap();
        self.finished.store(true, Ordering::Release);
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    fn cancel_and_join(&self) {
        {
            let _guard = self.queue.lock().unwrap();
            self.cancelled.store(true, Ordering::Release);
            self.not_empty.notify_all();
            self.not_full.notify_all();
        }
        if let Some(handle) = self.walker_join.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

/// Walker-side handle: push items into the ring. Does not join the walker on drop.
pub struct PrefetchProducer<T> {
    inner: Arc<Inner<T>>,
}

impl<T> PrefetchProducer<T> {
    pub fn push(&self, item: T) -> bool {
        self.inner.push(item)
    }

    pub fn set_error(&self, message: String) {
        self.inner.set_error(message);
    }

    pub fn mark_finished(&self) {
        self.inner.mark_finished();
    }
}

/// Consumer-side owner: take batches and join the walker on drop.
pub struct PrefetchBuffer<T> {
    inner: Arc<Inner<T>>,
}

impl<T> PrefetchBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        #[cfg(feature = "git-log-stats")]
        crate::git::diag::reset_git_log_stats();
        Self {
            inner: Arc::new(Inner {
                queue: Mutex::new(VecDeque::with_capacity(capacity.min(256))),
                not_empty: Condvar::new(),
                not_full: Condvar::new(),
                capacity,
                finished: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                error: Mutex::new(None),
                pushed: AtomicUsize::new(0),
                walker_join: Mutex::new(None),
            }),
        }
    }

    pub fn attach_walker(&self, handle: JoinHandle<()>) {
        *self.inner.walker_join.lock().unwrap() = Some(handle);
    }

    pub fn producer(&self) -> PrefetchProducer<T> {
        PrefetchProducer {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn pushed_count(&self) -> usize {
        self.inner.pushed.load(Ordering::Relaxed)
    }

    /// Take up to `max_count` contiguous items already in the ring.
    pub fn take_batch(&self, max_count: usize) -> Result<Vec<T>, String> {
        #[cfg(feature = "git-log-stats")]
        let t_batch = Instant::now();
        let inner = &self.inner;
        let mut queue = inner.queue.lock().unwrap();
        loop {
            if let Some(err) = inner.error.lock().unwrap().clone() {
                #[cfg(feature = "git-log-stats")]
                crate::git::diag::record_take_batch(t_batch.elapsed());
                return Err(err);
            }
            if inner.cancelled.load(Ordering::Acquire) {
                if queue.is_empty() {
                    if let Some(err) = inner.error.lock().unwrap().clone() {
                        #[cfg(feature = "git-log-stats")]
                        crate::git::diag::record_take_batch(t_batch.elapsed());
                        return Err(err);
                    }
                    #[cfg(feature = "git-log-stats")]
                    crate::git::diag::record_take_batch(t_batch.elapsed());
                    return Ok(Vec::new());
                }
            }
            if !queue.is_empty() {
                let n = max_count.min(queue.len());
                let batch: Vec<_> = queue.drain(..n).collect();
                inner.not_full.notify_all();
                #[cfg(feature = "git-log-stats")]
                crate::git::diag::record_take_batch(t_batch.elapsed());
                return Ok(batch);
            }
            if inner.finished.load(Ordering::Acquire) {
                #[cfg(feature = "git-log-stats")]
                crate::git::diag::record_take_batch(t_batch.elapsed());
                return Ok(Vec::new());
            }
            // cancelled+empty is handled above under the same queue lock; flag
            // publishers also take queue before notify, so no separate branch here.
            #[cfg(feature = "git-log-stats")]
            let t0 = Instant::now();
            queue = inner.not_empty.wait(queue).unwrap();
            #[cfg(feature = "git-log-stats")]
            crate::git::diag::record_empty_wait(t0.elapsed());
        }
    }
}

impl<T> Drop for PrefetchBuffer<T> {
    fn drop(&mut self) {
        self.inner.cancel_and_join();
        #[cfg(feature = "git-log-stats")]
        crate::git::diag::dump_git_log_stats_if_env();
    }
}

pub fn fixed_max_threads(libgit: bool, needs_file_changes: bool) -> u64 {
    if !needs_file_changes {
        return 1;
    }
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if libgit {
        cores.min(MAX_LIBGIT_THREADS) as u64
    } else {
        cores as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::date_walk::OidBytes;
    use std::time::Duration;

    fn oid(n: u8) -> OidBytes {
        let mut b = [0u8; 20];
        b[19] = n;
        b
    }

    #[test]
    fn metadata_only_uses_one_thread() {
        assert_eq!(fixed_max_threads(true, false), 1);
        assert_eq!(fixed_max_threads(false, false), 1);
    }

    #[test]
    fn take_batch_waits_until_push() {
        let buf = PrefetchBuffer::new(4);
        let producer = buf.producer();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            assert!(producer.push(oid(1)));
            producer.mark_finished();
        });
        buf.attach_walker(t);
        let batch = buf.take_batch(10).unwrap();
        assert_eq!(batch, vec![oid(1)]);
        assert!(buf.take_batch(10).unwrap().is_empty());
    }

    #[test]
    fn take_batch_wakes_on_mark_finished_empty_ring() {
        use std::sync::mpsc;

        let buf: PrefetchBuffer<OidBytes> = PrefetchBuffer::new(4);
        let producer = buf.producer();
        let (started_tx, started_rx) = mpsc::sync_channel(0);
        let (done_tx, done_rx) = mpsc::sync_channel(0);
        let consumer = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let batch = buf.take_batch(10).unwrap();
            assert!(batch.is_empty());
            let _ = done_tx.send(());
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("consumer should enter take_batch");
        // Give consumer time to reach not_empty.wait on an empty ring.
        std::thread::sleep(Duration::from_millis(50));
        producer.mark_finished();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("mark_finished must wake empty-ring take_batch");
        consumer.join().unwrap();
    }

    #[test]
    fn cancel_stops_push() {
        let buf: PrefetchBuffer<OidBytes> = PrefetchBuffer::new(2);
        buf.inner.cancelled.store(true, Ordering::Release);
        assert!(!buf.producer().push(oid(1)));
    }

    #[test]
    fn drop_joins_blocked_walker_within_timeout() {
        use std::sync::mpsc;

        let buf = PrefetchBuffer::new(2);
        let producer = buf.producer();
        let (ready_tx, ready_rx) = mpsc::sync_channel(0);
        let walker = std::thread::spawn(move || {
            assert!(producer.push(oid(1)));
            assert!(producer.push(oid(2)));
            ready_tx.send(()).unwrap();
            assert!(!producer.push(oid(3)));
        });
        buf.attach_walker(walker);
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("walker should block on full ring");

        let (done_tx, done_rx) = mpsc::sync_channel(0);
        std::thread::spawn(move || {
            drop(buf);
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("drop should join blocked walker within 5s");
    }

    #[test]
    fn drop_cancels_walk_before_repo_exhausted() {
        use crate::git::backend::libgit::{
            prepare_walk, run_prefetch_commit_walk, CachedRepo, PrefetchItem,
        };
        use crate::git::meta_proj::MetaProjection;
        use crate::git::options::DiffMerges;
        use std::sync::mpsc;

        const PARITY: &str = "test/fixtures/parity.git";
        /// All refs in parity.git (see fixture `build.sh` `--all` count).
        const PARITY_ALL_REFS_COMMITS: usize = 14;

        let handle = CachedRepo::open(PARITY).unwrap();
        let prep = prepare_walk(handle.repo(), None, None, false, true).unwrap();

        let (ring_full_tx, ring_full_rx) = mpsc::sync_channel(0);
        let buf: PrefetchBuffer<PrefetchItem> = PrefetchBuffer::new(4);
        let producer = buf.producer();
        let path = PARITY.to_string();
        let walker = std::thread::spawn(move || {
            let handle = CachedRepo::open(&path).unwrap();
            let mut local_pushes = 0usize;
            let _ = run_prefetch_commit_walk(
                handle.repo(),
                prep,
                MetaProjection::default(),
                true,
                DiffMerges::Off,
                |item| {
                    if !producer.push(item) {
                        return Ok(false);
                    }
                    local_pushes += 1;
                    if local_pushes == 4 {
                        let _ = ring_full_tx.send(());
                    }
                    Ok(true)
                },
            );
            producer.mark_finished();
        });
        buf.attach_walker(walker);

        ring_full_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("walker should fill the ring before we drop the buffer");
        let batch = buf.take_batch(2).unwrap();
        assert_eq!(batch.len(), 2);
        let pushed = buf.pushed_count();
        drop(buf);

        assert!(
            pushed < PARITY_ALL_REFS_COMMITS,
            "buffer drop should cancel walk before all {PARITY_ALL_REFS_COMMITS} commits (pushed={pushed})"
        );
    }
}
