//! Bounded prefetch ring between a single walker thread and parallel emit workers.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
#[cfg(feature = "prefetch-stats")]
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
                #[cfg(feature = "prefetch-stats")]
                crate::git::diag::record_push();
                self.not_empty.notify_one();
                return true;
            }
            #[cfg(feature = "prefetch-stats")]
            {
                let t0 = Instant::now();
                queue = self
                    .not_full
                    .wait_timeout(queue, Duration::from_millis(5))
                    .unwrap()
                    .0;
                crate::git::diag::record_full_wait(t0.elapsed());
            }
            #[cfg(not(feature = "prefetch-stats"))]
            {
                queue = self
                    .not_full
                    .wait_timeout(queue, Duration::from_millis(5))
                    .unwrap()
                    .0;
            }
        }
    }

    fn set_error(&self, message: String) {
        *self.error.lock().unwrap() = Some(message);
        self.cancelled.store(true, Ordering::Release);
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    fn mark_finished(&self) {
        self.finished.store(true, Ordering::Release);
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }

    fn cancel_and_join(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.not_empty.notify_all();
        self.not_full.notify_all();
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
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::reset_prefetch_stats();
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
        #[cfg(feature = "prefetch-stats")]
        let t_batch = Instant::now();
        let inner = &self.inner;
        let mut queue = inner.queue.lock().unwrap();
        loop {
            if let Some(err) = inner.error.lock().unwrap().clone() {
                #[cfg(feature = "prefetch-stats")]
                crate::git::diag::record_take_batch(t_batch.elapsed());
                return Err(err);
            }
            if inner.cancelled.load(Ordering::Acquire) {
                if queue.is_empty() {
                    if let Some(err) = inner.error.lock().unwrap().clone() {
                        #[cfg(feature = "prefetch-stats")]
                        crate::git::diag::record_take_batch(t_batch.elapsed());
                        return Err(err);
                    }
                    #[cfg(feature = "prefetch-stats")]
                    crate::git::diag::record_take_batch(t_batch.elapsed());
                    return Ok(Vec::new());
                }
            }
            if !queue.is_empty() {
                let n = max_count.min(queue.len());
                let batch: Vec<_> = queue.drain(..n).collect();
                inner.not_full.notify_all();
                #[cfg(feature = "prefetch-stats")]
                crate::git::diag::record_take_batch(t_batch.elapsed());
                return Ok(batch);
            }
            if inner.finished.load(Ordering::Acquire) {
                #[cfg(feature = "prefetch-stats")]
                crate::git::diag::record_take_batch(t_batch.elapsed());
                return Ok(Vec::new());
            }
            if inner.cancelled.load(Ordering::Acquire) {
                #[cfg(feature = "prefetch-stats")]
                crate::git::diag::record_take_batch(t_batch.elapsed());
                return Ok(Vec::new());
            }
            #[cfg(feature = "prefetch-stats")]
            {
                let t0 = Instant::now();
                queue = inner
                    .not_empty
                    .wait_timeout(queue, Duration::from_millis(5))
                    .unwrap()
                    .0;
                crate::git::diag::record_empty_wait(t0.elapsed());
            }
            #[cfg(not(feature = "prefetch-stats"))]
            {
                queue = inner
                    .not_empty
                    .wait_timeout(queue, Duration::from_millis(5))
                    .unwrap()
                    .0;
            }
        }
    }
}

impl<T> Drop for PrefetchBuffer<T> {
    fn drop(&mut self) {
        self.inner.cancel_and_join();
        #[cfg(feature = "prefetch-stats")]
        crate::git::diag::dump_prefetch_stats_if_env();
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
    fn cancel_stops_push() {
        let buf: PrefetchBuffer<OidBytes> = PrefetchBuffer::new(2);
        buf.inner.cancelled.store(true, Ordering::Release);
        assert!(!buf.producer().push(oid(1)));
    }

    #[test]
    fn early_drop_limits_walk_on_small_ring() {
        use crate::git::backend::libgit::{
            prepare_walk, run_prefetch_commit_walk, CachedRepo, PrefetchItem,
        };
        use crate::git::meta_proj::MetaProjection;
        use crate::git::options::DiffMerges;

        const PARITY: &str = "test/fixtures/parity.git";
        let handle = CachedRepo::open(PARITY).unwrap();
        let prep = prepare_walk(handle.repo(), None, None, false, true).unwrap();

        let buf: PrefetchBuffer<PrefetchItem> = PrefetchBuffer::new(4);
        let producer = buf.producer();
        let path = PARITY.to_string();
        let walker = std::thread::spawn(move || {
            let handle = CachedRepo::open(&path).unwrap();
            let _ = run_prefetch_commit_walk(
                handle.repo(),
                prep,
                MetaProjection::default(),
                true,
                DiffMerges::Off,
                |item| Ok(producer.push(item)),
            );
            producer.mark_finished();
        });
        buf.attach_walker(walker);

        let batch = buf.take_batch(2).unwrap();
        assert!(!batch.is_empty());
        let pushed = buf.pushed_count();
        drop(buf);

        assert!(
            pushed <= 4,
            "ring capacity should bound walker ahead of consumer"
        );
    }
}
