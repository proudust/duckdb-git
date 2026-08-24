//! Commit walk ordered like `git log` default: committer-date priority queue.
//!
//! Equal timestamps break ties by insertion order into the queue (FIFO), matching
//! git's `prio-queue` secondary key. OID bytes are **not** used for tie-breaking.
//!
//! Walk state is separate from [`DateWalkCallbacks`] so a scanner can keep the PQ
//! across `read()` calls while borrowing a thread-local repository only during
//! `next()`. Each queued commit is inspected **once** (committer time + parents).

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::error::Error;

/// Opaque 20-byte object id used by the shared walker.
pub type OidBytes = [u8; 20];

#[derive(Debug)]
struct HeapEntry {
    seconds: i64,
    /// Monotonic counter assigned at heap push time; smaller = earlier = first out.
    insertion_ctr: u64,
    oid: OidBytes,
    /// Parents captured during the single inspect at enqueue time.
    parents: Vec<OidBytes>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.seconds == other.seconds && self.insertion_ctr == other.insertion_ctr
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.seconds
            .cmp(&other.seconds)
            .then_with(|| other.insertion_ctr.cmp(&self.insertion_ctr))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn is_interesting(interesting: Option<&HashSet<OidBytes>>, oid: OidBytes) -> bool {
    match interesting {
        Some(set) => set.contains(&oid),
        None => true,
    }
}

/// One object read per queued commit: committer seconds + parent OIDs.
pub trait DateWalkCallbacks {
    fn inspect(&mut self, id: OidBytes) -> Result<(i64, Vec<OidBytes>), Box<dyn Error>>;
}

struct FnWalkCallbacks<F> {
    inspect: F,
}

impl<F> DateWalkCallbacks for FnWalkCallbacks<F>
where
    F: FnMut(OidBytes) -> Result<(i64, Vec<OidBytes>), Box<dyn Error>>,
{
    fn inspect(&mut self, id: OidBytes) -> Result<(i64, Vec<OidBytes>), Box<dyn Error>> {
        (self.inspect)(id)
    }
}

/// Incremental committer-date PQ walk (git log default-like).
pub struct CommitDateWalk {
    heap: BinaryHeap<HeapEntry>,
    queued: HashSet<OidBytes>,
    visited: HashSet<OidBytes>,
    next_ctr: u64,
    interesting: Option<HashSet<OidBytes>>,
    max_count: Option<usize>,
    emitted: usize,
}

impl CommitDateWalk {
    pub fn new(
        tips: impl IntoIterator<Item = OidBytes>,
        interesting: Option<HashSet<OidBytes>>,
        max_count: Option<usize>,
        callbacks: &mut impl DateWalkCallbacks,
    ) -> Result<Self, Box<dyn Error>> {
        let mut walk = Self {
            heap: BinaryHeap::new(),
            queued: HashSet::new(),
            visited: HashSet::new(),
            next_ctr: 0,
            interesting,
            max_count,
            emitted: 0,
        };

        if max_count == Some(0) {
            return Ok(walk);
        }

        for tip in tips {
            walk.enqueue(tip, callbacks)?;
        }
        Ok(walk)
    }

    fn enqueue(
        &mut self,
        oid: OidBytes,
        callbacks: &mut impl DateWalkCallbacks,
    ) -> Result<(), Box<dyn Error>> {
        if !is_interesting(self.interesting.as_ref(), oid) || !self.queued.insert(oid) {
            return Ok(());
        }
        let (seconds, parents) = callbacks.inspect(oid)?;
        let insertion_ctr = self.next_ctr;
        self.next_ctr += 1;
        self.heap.push(HeapEntry {
            seconds,
            insertion_ctr,
            oid,
            parents,
        });
        Ok(())
    }

    /// Next OID in walk order, or `None` when exhausted.
    pub fn next(
        &mut self,
        callbacks: &mut impl DateWalkCallbacks,
    ) -> Result<Option<OidBytes>, Box<dyn Error>> {
        if self.max_count.is_some_and(|n| self.emitted >= n) {
            return Ok(None);
        }

        let HeapEntry { oid, parents, .. } = match self.heap.pop() {
            Some(entry) => entry,
            None => return Ok(None),
        };

        if !self.visited.insert(oid) {
            return self.next(callbacks);
        }

        self.emitted += 1;
        let out = oid;

        if self.max_count.is_some_and(|n| self.emitted >= n) {
            return Ok(Some(out));
        }

        for parent in parents {
            self.enqueue(parent, callbacks)?;
        }

        Ok(Some(out))
    }

    pub fn drain_into_vec(
        mut self,
        callbacks: &mut impl DateWalkCallbacks,
    ) -> Result<Vec<OidBytes>, Box<dyn Error>> {
        let mut out = Vec::new();
        while let Some(oid) = self.next(callbacks)? {
            out.push(oid);
        }
        Ok(out)
    }
}

/// Walk commits reachable from `tips` that are also in `interesting`, in
/// committer-date priority-queue order (git log default-like).
pub fn walk_by_commit_date(
    tips: impl IntoIterator<Item = OidBytes>,
    interesting: Option<&HashSet<OidBytes>>,
    max_count: Option<usize>,
    mut inspect: impl FnMut(OidBytes) -> Result<(i64, Vec<OidBytes>), Box<dyn Error>>,
) -> Result<Vec<OidBytes>, Box<dyn Error>> {
    let interesting_owned = interesting.cloned();
    let mut callbacks = FnWalkCallbacks {
        inspect: &mut inspect,
    };
    CommitDateWalk::new(tips, interesting_owned, max_count, &mut callbacks)?
        .drain_into_vec(&mut callbacks)
}

/// Copy a 20-byte SHA-1 into [`OidBytes`].
pub fn oid_bytes_from_slice(bytes: &[u8]) -> Result<OidBytes, Box<dyn Error>> {
    if bytes.len() != 20 {
        return Err(format!("expected 20-byte oid, got {}", bytes.len()).into());
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(bytes);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn oid(n: u8) -> OidBytes {
        let mut id = [0u8; 20];
        id[19] = n;
        id
    }

    fn inspect_maps<'a>(
        parents: &'a HashMap<OidBytes, Vec<OidBytes>>,
        times: &'a HashMap<OidBytes, i64>,
    ) -> impl FnMut(OidBytes) -> Result<(i64, Vec<OidBytes>), Box<dyn Error>> + 'a {
        move |id| {
            Ok((
                *times.get(&id).unwrap(),
                parents.get(&id).cloned().unwrap_or_default(),
            ))
        }
    }

    #[test]
    fn clock_skew_max_count_is_pq_prefix_not_global_date_top() {
        let c = oid(1);
        let p = oid(2);
        let interesting = HashSet::from([c, p]);
        let parents: HashMap<_, _> = [(c, vec![p]), (p, vec![])].into();
        let times: HashMap<_, _> = [(c, 100i64), (p, 200)].into();

        let got =
            walk_by_commit_date([c], Some(&interesting), Some(1), inspect_maps(&parents, &times))
                .unwrap();
        assert_eq!(got, vec![c]);
    }

    #[test]
    fn iterator_matches_vec_api() {
        let c = oid(1);
        let p = oid(2);
        let interesting = HashSet::from([c, p]);
        let parents: HashMap<_, _> = [(c, vec![p]), (p, vec![])].into();
        let times: HashMap<_, _> = [(c, 100i64), (p, 50)].into();

        let vec_out =
            walk_by_commit_date([c], Some(&interesting), None, inspect_maps(&parents, &times))
                .unwrap();

        let mut callbacks = FnWalkCallbacks {
            inspect: inspect_maps(&parents, &times),
        };
        let mut walk =
            CommitDateWalk::new([c], Some(interesting), None, &mut callbacks).unwrap();
        let mut iter_out = Vec::new();
        while let Some(oid) = walk.next(&mut callbacks).unwrap() {
            iter_out.push(oid);
        }
        assert_eq!(iter_out, vec_out);
    }

    #[test]
    fn max_count_zero_returns_empty() {
        let t = oid(1);
        let interesting = HashSet::from([t]);
        let got = walk_by_commit_date([t], Some(&interesting), Some(0), |_| Ok((1, vec![]))).unwrap();
        assert!(got.is_empty());
    }
}
