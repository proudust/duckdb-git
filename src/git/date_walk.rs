//! Commit walk ordered like `git log` default: committer-date priority queue.
//!
//! Equal timestamps break ties by insertion order into the queue (FIFO), matching
//! git's `prio-queue` secondary key. OID bytes are **not** used for tie-breaking.

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
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.seconds == other.seconds && self.insertion_ctr == other.insertion_ctr
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: newer committer date first; on ties, smaller insertion_ctr
        // first (FIFO). Rust BinaryHeap pops the greatest element, so invert ctr.
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

/// Walk commits reachable from `tips` in committer-date priority-queue order
/// (git log default-like).
///
/// When `interesting` is `Some`, only OIDs in that set are enqueued (hide /
/// revision-range filtering). When `None`, every parent of an emitted commit
/// is eligible (no precomputed interesting set).
///
/// `max_count` is applied as streaming `take(N)`: do not pop once `N` results
/// have been emitted (`Some(0)` yields an empty vec immediately).
///
/// `parents` should already respect `first_parent` if that mode is active.
pub fn walk_by_commit_date(
    tips: impl IntoIterator<Item = OidBytes>,
    interesting: Option<&HashSet<OidBytes>>,
    max_count: Option<usize>,
    mut parents: impl FnMut(OidBytes) -> Result<Vec<OidBytes>, Box<dyn Error>>,
    mut committer_seconds: impl FnMut(OidBytes) -> Result<i64, Box<dyn Error>>,
) -> Result<Vec<OidBytes>, Box<dyn Error>> {
    if max_count == Some(0) {
        return Ok(Vec::new());
    }

    let is_interesting = |oid: &OidBytes| interesting.map_or(true, |s| s.contains(oid));

    let mut heap = BinaryHeap::new();
    let mut queued = HashSet::new();
    let mut visited = HashSet::new();
    let mut out = Vec::new();
    let mut next_ctr: u64 = 0;

    for tip in tips {
        if !is_interesting(&tip) || !queued.insert(tip) {
            continue;
        }
        let seconds = committer_seconds(tip)?;
        let insertion_ctr = next_ctr;
        next_ctr += 1;
        heap.push(HeapEntry {
            seconds,
            insertion_ctr,
            oid: tip,
        });
    }

    while let Some(HeapEntry { oid, .. }) = heap.pop() {
        if max_count.is_some_and(|n| out.len() >= n) {
            break;
        }
        if !visited.insert(oid) {
            continue;
        }
        out.push(oid);

        if max_count.is_some_and(|n| out.len() >= n) {
            break;
        }

        for parent in parents(oid)? {
            if !is_interesting(&parent) || !queued.insert(parent) {
                continue;
            }
            let seconds = committer_seconds(parent)?;
            let insertion_ctr = next_ctr;
            next_ctr += 1;
            heap.push(HeapEntry {
                seconds,
                insertion_ctr,
                oid: parent,
            });
        }
    }

    Ok(out)
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

    /// Synthetic graph: child C (t=100) → parent P (t=200, clock skew).
    /// Global date top-1 is P; PQ from C emits C then P.
    #[test]
    fn clock_skew_max_count_is_pq_prefix_not_global_date_top() {
        let c = oid(1);
        let p = oid(2);
        let interesting = HashSet::from([c, p]);
        let parents: HashMap<_, _> = [(c, vec![p]), (p, vec![])].into();
        let times: HashMap<_, _> = [(c, 100i64), (p, 200)].into();

        let got = walk_by_commit_date(
            [c],
            Some(&interesting),
            Some(1),
            |id| Ok(parents.get(&id).cloned().unwrap_or_default()),
            |id| Ok(*times.get(&id).unwrap()),
        )
        .unwrap();
        assert_eq!(got, vec![c], "PQ must emit child before skewed-newer parent");

        let global_top = {
            let mut v = vec![c, p];
            v.sort_by(|a, b| times[b].cmp(&times[a]).then(b.cmp(a)));
            v.into_iter().take(1).collect::<Vec<_>>()
        };
        assert_eq!(global_top, vec![p]);
        assert_ne!(got, global_top);
    }

    #[test]
    fn orphan_tip_with_newer_date_emits_before_older_line() {
        // Tips O (t=300) and M (t=100); M→A (t=50). Orphan O has no parents.
        let o = oid(10);
        let m = oid(11);
        let a = oid(12);
        let interesting = HashSet::from([o, m, a]);
        let parents: HashMap<_, _> = [(o, vec![]), (m, vec![a]), (a, vec![])].into();
        let times: HashMap<_, _> = [(o, 300i64), (m, 100), (a, 50)].into();

        let got = walk_by_commit_date(
            [m, o],
            Some(&interesting),
            None,
            |id| Ok(parents.get(&id).cloned().unwrap_or_default()),
            |id| Ok(*times.get(&id).unwrap()),
        )
        .unwrap();
        assert_eq!(got, vec![o, m, a]);
    }

    #[test]
    fn equal_seconds_fifo_earlier_tip_first() {
        let lo = oid(1);
        let hi = oid(2);
        assert!(hi > lo);
        let interesting = HashSet::from([lo, hi]);
        let parents: HashMap<_, _> = [(lo, vec![]), (hi, vec![])].into();
        let times: HashMap<_, _> = [(lo, 50i64), (hi, 50)].into();

        // Seed lo before hi: FIFO must emit lo first despite smaller OID bytes.
        let got = walk_by_commit_date(
            [lo, hi],
            Some(&interesting),
            None,
            |id| Ok(parents.get(&id).cloned().unwrap_or_default()),
            |id| Ok(*times.get(&id).unwrap()),
        )
        .unwrap();
        assert_eq!(got, vec![lo, hi]);

        let got_rev = walk_by_commit_date(
            [hi, lo],
            Some(&interesting),
            None,
            |id| Ok(parents.get(&id).cloned().unwrap_or_default()),
            |id| Ok(*times.get(&id).unwrap()),
        )
        .unwrap();
        assert_eq!(got_rev, vec![hi, lo]);
    }

    #[test]
    fn equal_seconds_parents_pushed_in_parent_index_order() {
        // Child C (t=100) has two parents P0, P1 at the same second (t=50).
        // After C is popped, parents are enqueued in parent(0).. order; FIFO
        // must emit P0 before P1 even when P1 has a larger OID.
        let c = oid(10);
        let p0 = oid(1);
        let p1 = oid(2);
        assert!(p1 > p0);
        let interesting = HashSet::from([c, p0, p1]);
        let parents: HashMap<_, _> = [(c, vec![p0, p1]), (p0, vec![]), (p1, vec![])].into();
        let times: HashMap<_, _> = [(c, 100i64), (p0, 50), (p1, 50)].into();

        let got = walk_by_commit_date(
            [c],
            Some(&interesting),
            None,
            |id| Ok(parents.get(&id).cloned().unwrap_or_default()),
            |id| Ok(*times.get(&id).unwrap()),
        )
        .unwrap();
        assert_eq!(got, vec![c, p0, p1]);
    }

    #[test]
    fn max_count_zero_returns_empty() {
        let t = oid(1);
        let interesting = HashSet::from([t]);
        let got = walk_by_commit_date(
            [t],
            Some(&interesting),
            Some(0),
            |_| Ok(vec![]),
            |_| Ok(1),
        )
        .unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn skips_tips_outside_interesting() {
        let in_set = oid(1);
        let out_set = oid(2);
        let interesting = HashSet::from([in_set]);
        let got = walk_by_commit_date(
            [out_set, in_set],
            Some(&interesting),
            None,
            |_| Ok(vec![]),
            |_| Ok(10),
        )
        .unwrap();
        assert_eq!(got, vec![in_set]);
    }

    #[test]
    fn none_interesting_walks_tip_to_parent() {
        let c = oid(1);
        let p = oid(2);
        let parents: HashMap<_, _> = [(c, vec![p]), (p, vec![])].into();
        let times: HashMap<_, _> = [(c, 100i64), (p, 50)].into();

        let got = walk_by_commit_date(
            [c],
            None,
            None,
            |id| Ok(parents.get(&id).cloned().unwrap_or_default()),
            |id| Ok(*times.get(&id).unwrap()),
        )
        .unwrap();
        assert_eq!(got, vec![c, p]);
    }

    #[test]
    fn heap_entry_eq_matches_ord_keys() {
        // Eq/Ord must use the same keys (seconds + insertion_ctr), not oid.
        let a = HeapEntry {
            seconds: 1,
            insertion_ctr: 0,
            oid: oid(1),
        };
        let b = HeapEntry {
            seconds: 1,
            insertion_ctr: 0,
            oid: oid(2),
        };
        assert_eq!(a, b);
        assert_eq!(a.cmp(&b), Ordering::Equal);
    }
}
