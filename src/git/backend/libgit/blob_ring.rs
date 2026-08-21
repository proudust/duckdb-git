use git2::Oid;
#[cfg(feature = "blob-ring-stats")]
use std::cell::Cell;
#[cfg(feature = "blob-ring-stats")]
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "blob-ring-stats")]
use std::sync::Mutex;

/// Per-batch stored-blob cap after `finish_commit` eviction.
///
/// `read()` uses at most 4 threads, each with its own ring. Cap is not divided
/// by thread count. Peak RSS can exceed `4 × DEFAULT_CAP`: miss `to_vec` lives
/// in `PendingOlds` until finish, and surviving OIDs are inserted before evict.
pub const DEFAULT_CAP: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default)]
pub struct BlobRingStats {
    pub lookups: u64,
    pub hits: u64,
    pub lookup_bytes: u64,
    pub hit_bytes: u64,
    pub old_lookups: u64,
    pub old_hits: u64,
    pub old_bytes: u64,
    pub old_hit_bytes: u64,
    pub new_lookups: u64,
    pub new_hits: u64,
    pub new_bytes: u64,
    pub new_hit_bytes: u64,
    pub typechange_lookups: u64,
    pub typechange_hits: u64,
    pub typechange_bytes: u64,
    pub typechange_hit_bytes: u64,
    pub inserts: u64,
    pub insert_bytes: u64,
    pub bumps: u64,
    pub commits: u64,
}

impl BlobRingStats {
    pub const ZERO: Self = Self {
        lookups: 0,
        hits: 0,
        lookup_bytes: 0,
        hit_bytes: 0,
        old_lookups: 0,
        old_hits: 0,
        old_bytes: 0,
        old_hit_bytes: 0,
        new_lookups: 0,
        new_hits: 0,
        new_bytes: 0,
        new_hit_bytes: 0,
        typechange_lookups: 0,
        typechange_hits: 0,
        typechange_bytes: 0,
        typechange_hit_bytes: 0,
        inserts: 0,
        insert_bytes: 0,
        bumps: 0,
        commits: 0,
    };

    #[cfg(feature = "blob-ring-stats")]
    fn add(&mut self, other: Self) {
        self.lookups += other.lookups;
        self.hits += other.hits;
        self.lookup_bytes += other.lookup_bytes;
        self.hit_bytes += other.hit_bytes;
        self.old_lookups += other.old_lookups;
        self.old_hits += other.old_hits;
        self.old_bytes += other.old_bytes;
        self.old_hit_bytes += other.old_hit_bytes;
        self.new_lookups += other.new_lookups;
        self.new_hits += other.new_hits;
        self.new_bytes += other.new_bytes;
        self.new_hit_bytes += other.new_hit_bytes;
        self.typechange_lookups += other.typechange_lookups;
        self.typechange_hits += other.typechange_hits;
        self.typechange_bytes += other.typechange_bytes;
        self.typechange_hit_bytes += other.typechange_hit_bytes;
        self.inserts += other.inserts;
        self.insert_bytes += other.insert_bytes;
        self.bumps += other.bumps;
        self.commits += other.commits;
    }
}

#[derive(Clone, Copy)]
pub(super) enum LookupKind {
    Old,
    New,
    Typechange,
}

#[cfg(feature = "blob-ring-stats")]
thread_local! {
    static BATCH_STATS: Cell<BlobRingStats> = const { Cell::new(BlobRingStats::ZERO) };
}

#[cfg(feature = "blob-ring-stats")]
static GLOBAL: Mutex<BlobRingStats> = Mutex::new(BlobRingStats::ZERO);

pub(super) fn record_lookup(kind: LookupKind, hit: bool, bytes: usize) {
    #[cfg(feature = "blob-ring-stats")]
    {
        let bytes = bytes as u64;
        BATCH_STATS.with(|cell| {
            let mut s = cell.get();
            s.lookups += 1;
            s.lookup_bytes += bytes;
            if hit {
                s.hits += 1;
                s.hit_bytes += bytes;
            }
            match kind {
                LookupKind::Old => {
                    s.old_lookups += 1;
                    s.old_bytes += bytes;
                    if hit {
                        s.old_hits += 1;
                        s.old_hit_bytes += bytes;
                    }
                }
                LookupKind::New => {
                    s.new_lookups += 1;
                    s.new_bytes += bytes;
                    if hit {
                        s.new_hits += 1;
                        s.new_hit_bytes += bytes;
                    }
                }
                LookupKind::Typechange => {
                    s.typechange_lookups += 1;
                    s.typechange_bytes += bytes;
                    if hit {
                        s.typechange_hits += 1;
                        s.typechange_hit_bytes += bytes;
                    }
                }
            }
            cell.set(s);
        });
    }
    #[cfg(not(feature = "blob-ring-stats"))]
    {
        let _ = (kind, hit, bytes);
    }
}

#[cfg(feature = "blob-ring-stats")]
fn record_finish(inserts: u64, insert_bytes: u64, bumps: u64) {
    BATCH_STATS.with(|cell| {
        let mut s = cell.get();
        s.inserts += inserts;
        s.insert_bytes += insert_bytes;
        s.bumps += bumps;
        s.commits += 1;
        cell.set(s);
    });
}

pub(crate) fn flush_blob_ring_stats() {
    #[cfg(feature = "blob-ring-stats")]
    {
        let batch = BATCH_STATS.with(|cell| cell.replace(BlobRingStats::ZERO));
        GLOBAL.lock().expect("blob ring stats").add(batch);
    }
}

pub(crate) fn reset_blob_ring_stats() {
    #[cfg(feature = "blob-ring-stats")]
    {
        BATCH_STATS.with(|cell| cell.set(BlobRingStats::ZERO));
        *GLOBAL.lock().expect("blob ring stats") = BlobRingStats::ZERO;
    }
}

pub(crate) fn snapshot_blob_ring_stats() -> BlobRingStats {
    #[cfg(feature = "blob-ring-stats")]
    {
        flush_blob_ring_stats();
        *GLOBAL.lock().expect("blob ring stats")
    }
    #[cfg(not(feature = "blob-ring-stats"))]
    {
        BlobRingStats::ZERO
    }
}

struct CachedBlob {
    bytes: Vec<u8>,
    npaths: usize,
}

struct PathSlot {
    oid: Oid,
    tick: u64,
}

#[derive(Default)]
#[doc(hidden)]
pub struct PendingOlds {
    /// Last-write OID per path within this commit.
    paths: HashMap<Vec<u8>, Oid>,
    /// `Some(bytes)` = miss `to_vec` once; `None` = already in the ring.
    bytes: HashMap<Oid, Option<Vec<u8>>>,
}

impl PendingOlds {
    pub fn record_hit(&mut self, path: Vec<u8>, oid: Oid) {
        if oid.is_zero() {
            return;
        }
        self.paths.insert(path, oid);
        self.bytes.entry(oid).or_insert(None);
    }

    pub fn record_miss(&mut self, path: Vec<u8>, oid: Oid, bytes: Vec<u8>) {
        if oid.is_zero() {
            return;
        }
        self.paths.insert(path, oid);
        self.bytes.entry(oid).or_insert(Some(bytes));
    }
}

/// Path last-old cache: lookup by OID, evict by path LRU until `bytes <= cap`.
///
/// Lifetime is one DuckDB batch (`BlobRing::new()` in `LibGitLogScanner::read`).
#[doc(hidden)]
pub struct BlobRing {
    by_oid: HashMap<Oid, CachedBlob>,
    by_path: HashMap<Vec<u8>, PathSlot>,
    /// Lazy LRU: push on every touch; stale ticks stay until pop.
    lru: VecDeque<(Vec<u8>, u64)>,
    bytes: usize,
    cap: usize,
    tick: u64,
    #[cfg(test)]
    finish_count: u64,
}

impl BlobRing {
    pub fn new() -> Self {
        Self::with_cap(DEFAULT_CAP)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            by_oid: HashMap::new(),
            by_path: HashMap::new(),
            lru: VecDeque::new(),
            bytes: 0,
            cap,
            tick: 0,
            #[cfg(test)]
            finish_count: 0,
        }
    }

    #[cfg(test)]
    pub fn finish_count(&self) -> u64 {
        self.finish_count
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.by_oid.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.by_oid.is_empty()
    }

    #[cfg(test)]
    fn cached_bytes(&self) -> usize {
        self.bytes
    }

    #[cfg(test)]
    pub fn contains_path(&self, path: &[u8]) -> bool {
        self.by_path.contains_key(path)
    }

    pub fn lookup(&self, oid: Oid) -> Option<&[u8]> {
        if oid.is_zero() {
            return None;
        }
        self.by_oid.get(&oid).map(|cached| cached.bytes.as_slice())
    }

    /// Apply this commit's olds. Call only after lookups/xdiff have dropped
    /// ring borrows. Empty pending does not touch paths or the LRU, but still
    /// counts as a finish (`commits` / `finish_count`).
    ///
    /// After insert→bind, every path has a live LRU entry, so same-finish
    /// over-cap is resolved by the deque alone (`evict_to_cap`).
    pub fn finish_commit(&mut self, pending: PendingOlds) {
        if pending.paths.is_empty() {
            #[cfg(feature = "blob-ring-stats")]
            record_finish(0, 0, 0);
            #[cfg(test)]
            {
                self.finish_count += 1;
                self.assert_invariants();
            }
            return;
        }

        let PendingOlds {
            paths,
            bytes: mut oid_bytes,
        } = pending;

        // 1. Drop paths whose blob is larger than cap (leave existing last-old).
        let mut surviving: Vec<(Vec<u8>, Oid)> = Vec::new();
        for (path, oid) in paths {
            let len = if let Some(cached) = self.by_oid.get(&oid) {
                cached.bytes.len()
            } else {
                match oid_bytes.get(&oid) {
                    Some(Some(v)) => v.len(),
                    _ => continue,
                }
            };
            if len > self.cap {
                continue;
            }
            surviving.push((path, oid));
        }

        // 2. Ensure by_oid for surviving OIDs (len <= cap). Leftover Some with
        // no surviving path is not inserted.
        #[cfg(feature = "blob-ring-stats")]
        let mut inserts = 0u64;
        #[cfg(feature = "blob-ring-stats")]
        let mut insert_bytes = 0u64;
        #[cfg(feature = "blob-ring-stats")]
        let bumps = {
            let mut bump_oids = HashSet::new();
            for (_, oid) in &surviving {
                if self.by_oid.contains_key(oid) {
                    bump_oids.insert(*oid);
                }
            }
            bump_oids.len() as u64
        };
        for &(_, oid) in &surviving {
            if self.by_oid.contains_key(&oid) {
                continue;
            }
            if let Some(Some(v)) = oid_bytes.remove(&oid) {
                #[cfg(feature = "blob-ring-stats")]
                {
                    inserts += 1;
                    insert_bytes += v.len() as u64;
                }
                self.bytes += v.len();
                self.by_oid.insert(
                    oid,
                    CachedBlob {
                        bytes: v,
                        npaths: 0,
                    },
                );
            }
        }

        // 3. Bind paths (attach before unlink so a hit on an OID that another
        // path is leaving still sees the bytes).
        let mut unlinks: Vec<Oid> = Vec::new();
        for (path, oid) in surviving {
            if !self.by_oid.contains_key(&oid) {
                continue;
            }
            match self.by_path.get(&path).map(|slot| slot.oid) {
                Some(existing) if existing == oid => {
                    self.touch_path(&path);
                }
                Some(old_oid) => {
                    unlinks.push(old_oid);
                    self.bind_path(path, oid);
                }
                None => {
                    self.bind_path(path, oid);
                }
            }
        }

        // 4. Unlink old OIDs one path at a time (not a set).
        for oid in unlinks {
            self.dec_npaths(oid);
        }

        // 5. Lazy LRU until bytes <= cap.
        self.evict_to_cap();

        #[cfg(feature = "blob-ring-stats")]
        record_finish(inserts, insert_bytes, bumps);
        #[cfg(test)]
        {
            self.finish_count += 1;
            self.assert_invariants();
        }
    }

    fn touch_path(&mut self, path: &[u8]) {
        self.tick += 1;
        if let Some(slot) = self.by_path.get_mut(path) {
            slot.tick = self.tick;
        }
        self.lru.push_back((path.to_vec(), self.tick));
    }

    fn bind_path(&mut self, path: Vec<u8>, oid: Oid) {
        self.tick += 1;
        self.by_path.insert(
            path.clone(),
            PathSlot {
                oid,
                tick: self.tick,
            },
        );
        self.lru.push_back((path, self.tick));
        let cached = self.by_oid.get_mut(&oid).expect("oid ensured in by_oid");
        cached.npaths += 1;
    }

    fn dec_npaths(&mut self, oid: Oid) {
        let npaths = match self.by_oid.get_mut(&oid) {
            Some(cached) => {
                if cached.npaths == 0 {
                    return;
                }
                cached.npaths -= 1;
                cached.npaths
            }
            None => return,
        };
        if npaths == 0 {
            let cached = self.by_oid.remove(&oid).expect("npaths just reached 0");
            self.bytes -= cached.bytes.len();
        }
    }

    fn unlink_path(&mut self, path: &[u8]) {
        let Some(slot) = self.by_path.remove(path) else {
            return;
        };
        self.dec_npaths(slot.oid);
    }

    /// Evict by lazy LRU until `bytes <= cap`. Insert→bind guarantees every
    /// path has a live deque entry, so same-finish over-cap needs no other
    /// fallback. Stale ticks are no-ops (do not assert progress per pop).
    fn evict_to_cap(&mut self) {
        while self.bytes > self.cap {
            match self.lru.pop_front() {
                Some((path, tick)) => {
                    if self.by_path.get(&path).is_some_and(|s| s.tick == tick) {
                        self.unlink_path(&path);
                    }
                }
                None => {
                    debug_assert!(false, "deque empty but over cap");
                    break;
                }
            }
        }
        debug_assert!(self.bytes <= self.cap);
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        assert!(
            self.bytes <= self.cap,
            "bytes {} > cap {}",
            self.bytes,
            self.cap
        );
        let sum: usize = self.by_oid.values().map(|c| c.bytes.len()).sum();
        assert_eq!(
            self.bytes, sum,
            "cached_bytes must equal sum of blob lengths"
        );
        let mut counts: HashMap<Oid, usize> = HashMap::new();
        for slot in self.by_path.values() {
            *counts.entry(slot.oid).or_default() += 1;
            assert!(
                self.by_oid.contains_key(&slot.oid),
                "by_path oid must exist in by_oid"
            );
        }
        for (oid, cached) in &self.by_oid {
            assert_eq!(
                cached.npaths,
                *counts.get(oid).unwrap_or(&0),
                "npaths must match by_path refs"
            );
            assert!(
                cached.bytes.len() <= self.cap,
                "blob longer than cap must not be stored"
            );
        }
    }
}

impl Default for BlobRing {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(n: u8) -> Oid {
        let mut raw = [0u8; 20];
        raw[0] = n;
        Oid::from_bytes(&raw).unwrap()
    }

    fn finish_miss(ring: &mut BlobRing, path: &[u8], id: Oid, bytes: &[u8]) {
        let mut pending = PendingOlds::default();
        pending.record_miss(path.to_vec(), id, bytes.to_vec());
        ring.finish_commit(pending);
    }

    fn finish_hit(ring: &mut BlobRing, path: &[u8], id: Oid) {
        let mut pending = PendingOlds::default();
        pending.record_hit(path.to_vec(), id);
        ring.finish_commit(pending);
    }

    fn finish_empty(ring: &mut BlobRing) {
        ring.finish_commit(PendingOlds::default());
    }

    #[cfg(not(feature = "blob-ring-stats"))]
    #[test]
    fn snapshot_is_zero_without_stats_feature() {
        reset_blob_ring_stats();
        let mut ring = BlobRing::with_cap(1024);
        let _ = ring.lookup(oid(1));
        finish_miss(&mut ring, b"a", oid(1), b"now");
        let s = snapshot_blob_ring_stats();
        assert_eq!(s.lookups, 0);
        assert_eq!(s.hits, 0);
        assert_eq!(s.inserts, 0);
        assert_eq!(s.bumps, 0);
        assert_eq!(s.commits, 0);
    }

    #[test]
    fn path_overwrite_drops_unreferenced_oid() {
        let mut ring = BlobRing::with_cap(1024);
        finish_miss(&mut ring, b"a", oid(1), b"one");
        finish_miss(&mut ring, b"a", oid(2), b"two");
        assert!(ring.lookup(oid(1)).is_none());
        assert_eq!(ring.lookup(oid(2)), Some(&b"two"[..]));
        assert!(ring.contains_path(b"a"));
        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn shared_oid_survives_until_last_path_unlinks() {
        let mut ring = BlobRing::with_cap(1024);
        let mut pending = PendingOlds::default();
        pending.record_miss(b"a".to_vec(), oid(1), b"shared".to_vec());
        pending.record_miss(b"b".to_vec(), oid(1), b"ignored".to_vec());
        pending.record_miss(b"c".to_vec(), oid(1), b"ignored2".to_vec());
        ring.finish_commit(pending);
        assert_eq!(ring.lookup(oid(1)), Some(&b"shared"[..]));

        finish_miss(&mut ring, b"a", oid(2), b"aa");
        assert_eq!(ring.lookup(oid(1)), Some(&b"shared"[..]));

        finish_miss(&mut ring, b"b", oid(3), b"bb");
        assert_eq!(ring.lookup(oid(1)), Some(&b"shared"[..]));

        finish_miss(&mut ring, b"c", oid(4), b"cc");
        assert!(ring.lookup(oid(1)).is_none());
        assert_eq!(ring.len(), 3);
    }

    #[test]
    fn foo_leave_and_bar_hit_same_oid_in_one_finish() {
        let mut ring = BlobRing::with_cap(1024);
        finish_miss(&mut ring, b"foo", oid(1), b"keep");
        let mut pending = PendingOlds::default();
        pending.record_miss(b"foo".to_vec(), oid(2), b"new".to_vec());
        pending.record_hit(b"bar".to_vec(), oid(1));
        ring.finish_commit(pending);
        assert_eq!(ring.lookup(oid(1)), Some(&b"keep"[..]));
        assert_eq!(ring.lookup(oid(2)), Some(&b"new"[..]));
        assert!(ring.contains_path(b"foo"));
        assert!(ring.contains_path(b"bar"));
    }

    #[test]
    fn cap_evicts_lru_path_and_unref_oid() {
        let mut ring = BlobRing::with_cap(10);
        finish_miss(&mut ring, b"a", oid(1), b"aaaaa");
        finish_miss(&mut ring, b"b", oid(2), b"bbbbb");
        finish_miss(&mut ring, b"c", oid(3), b"ccccc");
        assert!(ring.lookup(oid(1)).is_none());
        assert!(!ring.contains_path(b"a"));
        assert_eq!(ring.lookup(oid(2)), Some(&b"bbbbb"[..]));
        assert_eq!(ring.lookup(oid(3)), Some(&b"ccccc"[..]));
        assert!(ring.cached_bytes() <= 10);
    }

    #[test]
    fn same_finish_over_cap_drops_this_commit_paths() {
        let mut ring = BlobRing::with_cap(10);
        let mut pending = PendingOlds::default();
        pending.record_miss(b"a".to_vec(), oid(1), b"aaaaa".to_vec());
        pending.record_miss(b"b".to_vec(), oid(2), b"bbbbb".to_vec());
        pending.record_miss(b"c".to_vec(), oid(3), b"ccccc".to_vec());
        ring.finish_commit(pending);
        assert!(ring.cached_bytes() <= 10);
        let remaining = [oid(1), oid(2), oid(3)]
            .into_iter()
            .filter(|id| ring.lookup(*id).is_some())
            .count();
        assert!(remaining <= 2);
        assert!(remaining >= 1);
    }

    #[test]
    fn stale_lru_pop_does_not_double_decrement() {
        let mut ring = BlobRing::with_cap(5);
        finish_miss(&mut ring, b"a", oid(1), b"1");
        finish_miss(&mut ring, b"a", oid(2), b"22");
        finish_miss(&mut ring, b"a", oid(3), b"333");
        finish_miss(&mut ring, b"b", oid(4), b"4444");
        assert!(ring.lookup(oid(3)).is_none());
        assert_eq!(ring.lookup(oid(4)), Some(&b"4444"[..]));
        assert_eq!(ring.cached_bytes(), 4);
        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn empty_commit_does_not_drop_last_old() {
        let mut ring = BlobRing::with_cap(1024);
        finish_miss(&mut ring, b"a", oid(1), b"stay");
        finish_empty(&mut ring);
        finish_empty(&mut ring);
        assert_eq!(ring.lookup(oid(1)), Some(&b"stay"[..]));
        assert!(ring.contains_path(b"a"));
        assert_eq!(ring.finish_count(), 3);
    }

    #[test]
    fn two_paths_leaving_same_oid_decrement_npaths_twice() {
        let mut ring = BlobRing::with_cap(1024);
        let mut pending = PendingOlds::default();
        pending.record_miss(b"a".to_vec(), oid(1), b"shared".to_vec());
        pending.record_miss(b"b".to_vec(), oid(1), b"ignored".to_vec());
        ring.finish_commit(pending);

        let mut pending = PendingOlds::default();
        pending.record_miss(b"a".to_vec(), oid(2), b"aa".to_vec());
        pending.record_miss(b"b".to_vec(), oid(3), b"bb".to_vec());
        ring.finish_commit(pending);
        assert!(ring.lookup(oid(1)).is_none());
        assert_eq!(ring.lookup(oid(2)), Some(&b"aa"[..]));
        assert_eq!(ring.lookup(oid(3)), Some(&b"bb"[..]));
        assert_eq!(ring.cached_bytes(), 4);
    }

    #[test]
    fn oversized_blob_skips_insert_and_keeps_path_last_old() {
        let mut ring = BlobRing::with_cap(5);
        finish_miss(&mut ring, b"a", oid(1), b"tiny");
        finish_miss(&mut ring, b"a", oid(2), b"too-big");
        assert_eq!(ring.lookup(oid(1)), Some(&b"tiny"[..]));
        assert!(ring.lookup(oid(2)).is_none());
        assert!(ring.contains_path(b"a"));
    }

    #[test]
    fn new_default_cap_keeps_few_kib() {
        let mut ring = BlobRing::new();
        let kib = vec![0u8; 4096];
        finish_miss(&mut ring, b"a", oid(1), &kib);
        assert_eq!(ring.lookup(oid(1)).map(|b| b.len()), Some(4096));
        assert!(ring.cached_bytes() <= DEFAULT_CAP);
    }

    #[test]
    fn over_cap_same_finish_stops_without_double_unlink() {
        let mut ring = BlobRing::with_cap(5);
        let mut pending = PendingOlds::default();
        for i in 1u8..=8 {
            pending.record_miss(vec![i], oid(i), vec![i; 3]);
        }
        ring.finish_commit(pending);
        assert!(ring.cached_bytes() <= 5);
        assert!(ring.len() <= 1);
    }

    #[test]
    fn miss_to_vec_once_hit_keeps_original_bytes() {
        let mut ring = BlobRing::with_cap(1024);
        let mut pending = PendingOlds::default();
        pending.record_miss(b"a".to_vec(), oid(1), b"orig".to_vec());
        pending.record_miss(b"b".to_vec(), oid(1), b"copy".to_vec());
        ring.finish_commit(pending);
        assert_eq!(ring.lookup(oid(1)), Some(&b"orig"[..]));

        finish_hit(&mut ring, b"a", oid(1));
        assert_eq!(ring.lookup(oid(1)), Some(&b"orig"[..]));
        assert_eq!(ring.len(), 1);
    }

    #[test]
    fn pending_olds_not_visible_until_finish() {
        let mut ring = BlobRing::with_cap(1024);
        let mut pending = PendingOlds::default();
        pending.record_miss(b"a".to_vec(), oid(1), b"now".to_vec());
        assert!(ring.lookup(oid(1)).is_none());
        ring.finish_commit(pending);
        assert_eq!(ring.lookup(oid(1)), Some(&b"now"[..]));
    }

    #[test]
    fn zero_oid_never_enters_map() {
        let mut ring = BlobRing::with_cap(1024);
        let mut pending = PendingOlds::default();
        pending.record_miss(b"a".to_vec(), Oid::ZERO_SHA1, b"nope".to_vec());
        pending.record_hit(b"b".to_vec(), Oid::ZERO_SHA1);
        ring.finish_commit(pending);
        assert_eq!(ring.len(), 0);
        assert!(ring.lookup(Oid::ZERO_SHA1).is_none());
        assert!(!ring.contains_path(b"a"));
        assert_eq!(ring.finish_count(), 1);
    }
}
