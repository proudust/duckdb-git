use git2::Oid;
use std::collections::{HashMap, VecDeque};

pub(crate) const K: usize = 11;

#[derive(Default)]
pub(super) struct PendingOlds {
    /// `Some(bytes)` = miss to insert; `None` = hit to gen-bump.
    olds: HashMap<Oid, Option<Vec<u8>>>,
}

impl PendingOlds {
    pub(super) fn contains(&self, oid: Oid) -> bool {
        self.olds.contains_key(&oid)
    }

    pub(super) fn record_hit(&mut self, oid: Oid) {
        if oid.is_zero() {
            return;
        }
        self.olds.entry(oid).or_insert(None);
    }

    pub(super) fn record_miss(&mut self, oid: Oid, bytes: Vec<u8>) {
        if oid.is_zero() {
            return;
        }
        self.olds.entry(oid).or_insert(Some(bytes));
    }
}

#[derive(Default)]
pub(crate) struct BlobRing {
    by_oid: HashMap<Oid, (u64, Vec<u8>)>,
    gens: VecDeque<(u64, Vec<Oid>)>,
    gen: u64,
}

impl BlobRing {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        self.gen
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_oid.len()
    }

    pub(crate) fn lookup(&self, oid: Oid) -> Option<&[u8]> {
        if oid.is_zero() {
            return None;
        }
        self.by_oid.get(&oid).map(|(_, bytes)| bytes.as_slice())
    }

    /// Insert this commit's unique olds and advance the generation.
    ///
    /// Call only after all lookups/xdiff for the commit have dropped ring
    /// borrows. Empty pending still pushes a generation (empty delta commits).
    pub(super) fn finish_commit(&mut self, pending: PendingOlds) {
        let gen = self.gen;
        let mut oid_list = Vec::with_capacity(pending.olds.len());
        for (oid, bytes) in pending.olds {
            if oid.is_zero() {
                continue;
            }
            match bytes {
                Some(bytes) => {
                    self.by_oid.insert(oid, (gen, bytes));
                }
                None => {
                    if let Some((stored_gen, _)) = self.by_oid.get_mut(&oid) {
                        *stored_gen = gen;
                    }
                }
            }
            oid_list.push(oid);
        }
        self.gens.push_back((gen, oid_list));
        if self.gens.len() > K {
            let (dropped_gen, dropped) = self.gens.pop_front().expect("len > K");
            for oid in dropped {
                if self.by_oid.get(&oid).map(|(g, _)| *g) == Some(dropped_gen) {
                    self.by_oid.remove(&oid);
                }
            }
        }
        self.gen += 1;
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

    fn finish_miss(ring: &mut BlobRing, id: Oid, bytes: &[u8]) {
        let mut pending = PendingOlds::default();
        pending.record_miss(id, bytes.to_vec());
        ring.finish_commit(pending);
    }

    fn finish_hit(ring: &mut BlobRing, id: Oid) {
        let mut pending = PendingOlds::default();
        pending.record_hit(id);
        ring.finish_commit(pending);
    }

    fn finish_empty(ring: &mut BlobRing) {
        ring.finish_commit(PendingOlds::default());
    }

    #[test]
    fn overwrite_keeps_latest_gen_bytes_and_lists_oid() {
        let mut ring = BlobRing::new();
        finish_miss(&mut ring, oid(1), b"one");
        finish_miss(&mut ring, oid(1), b"two");
        assert_eq!(ring.lookup(oid(1)), Some(&b"two"[..]));
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.generation(), 2);
        for _ in 0..K {
            finish_empty(&mut ring);
        }
        assert!(
            ring.lookup(oid(1)).is_none(),
            "oid must be on the latest gen list so eviction of that gen removes it"
        );
    }

    #[test]
    fn oldest_hits_at_k_and_misses_on_k_plus_one() {
        let mut ring = BlobRing::new();
        for i in 0..K as u8 {
            finish_miss(&mut ring, oid(i + 1), &[i]);
        }
        assert_eq!(ring.lookup(oid(1)), Some(&[0][..]));
        finish_miss(&mut ring, oid(K as u8 + 1), &[K as u8]);
        assert!(ring.lookup(oid(1)).is_none());
        assert_eq!(ring.lookup(oid(2)), Some(&[1][..]));
    }

    #[test]
    fn hit_bump_stays_until_k_gens_without_hit() {
        let mut ring = BlobRing::new();
        let hot = oid(1);
        finish_miss(&mut ring, hot, b"hot");
        for i in 0..K {
            let mut pending = PendingOlds::default();
            pending.record_hit(hot);
            pending.record_miss(oid(i as u8 + 2), vec![i as u8]);
            ring.finish_commit(pending);
        }
        assert_eq!(ring.lookup(hot), Some(&b"hot"[..]));

        for i in 0..K {
            finish_miss(&mut ring, oid(100 + i as u8), &[i as u8]);
        }
        assert!(
            ring.lookup(hot).is_none(),
            "hit-bumped oid must leave after K gens without appearing on a gen list"
        );
    }

    #[test]
    fn unique_olds_insert_once() {
        let mut ring = BlobRing::new();
        let mut pending = PendingOlds::default();
        pending.record_miss(oid(1), b"a".to_vec());
        pending.record_miss(oid(1), b"b".to_vec());
        ring.finish_commit(pending);
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.lookup(oid(1)), Some(&b"a"[..]));
    }

    #[test]
    fn current_gen_olds_not_visible_until_finish() {
        let mut ring = BlobRing::new();
        let mut pending = PendingOlds::default();
        pending.record_miss(oid(1), b"now".to_vec());
        assert!(ring.lookup(oid(1)).is_none());
        ring.finish_commit(pending);
        assert_eq!(ring.lookup(oid(1)), Some(&b"now"[..]));
    }

    #[test]
    fn zero_oid_never_enters_map() {
        let mut ring = BlobRing::new();
        let mut pending = PendingOlds::default();
        pending.record_miss(Oid::ZERO_SHA1, b"nope".to_vec());
        pending.record_hit(Oid::ZERO_SHA1);
        ring.finish_commit(pending);
        assert_eq!(ring.len(), 0);
        assert!(ring.lookup(Oid::ZERO_SHA1).is_none());
        assert_eq!(ring.generation(), 1);
    }

    #[test]
    fn empty_delta_commit_still_advances_generation() {
        let mut ring = BlobRing::new();
        finish_miss(&mut ring, oid(1), b"a");
        for _ in 0..K {
            finish_empty(&mut ring);
        }
        assert!(
            ring.lookup(oid(1)).is_none(),
            "empty gens must still occupy a slot so eviction is not delayed"
        );
    }

    #[test]
    fn hit_without_memcpy_keeps_original_bytes() {
        let mut ring = BlobRing::new();
        finish_miss(&mut ring, oid(1), b"orig");
        finish_hit(&mut ring, oid(1));
        assert_eq!(ring.lookup(oid(1)), Some(&b"orig"[..]));
        assert_eq!(ring.len(), 1);
    }
}
