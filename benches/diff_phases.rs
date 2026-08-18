//! One-shot breakdown of libgit `with_diff` work on `BENCH_REPO`.
//!
//! Splits each non-merge commit into tree-diff, find_similar, blob inflate,
//! and xdiff/numstat so we can see what dominates on large histories.
//!
//! After warmup, runs probe passes (blob-cache env is ignored — always off):
//! - pass 0 (T0): plain `find_blob`
//! - pass 1 (T1): one-generation `HashMap` (lookup previous olds; insert this
//!   commit's olds into a separate map; swap at end). `copy_tax` is memcpy of
//!   **all** olds into the next map, hits included.
//! - pass 2: always `find_blob`; distance histogram (1, 2, … and 2049+).
//!   Insert old only, lookup old and new. Do not insert the current generation
//!   until lookups finish. Same OID keeps the latest insert generation.
//! - pass 3: production path-LRU `BlobRing` vs a bench-local K=11 generation
//!   replica, same revwalk oid-index batches as `git_log`.
//!
//! ```bash
//! BENCH_REPO=/path/to/repo cargo bench --bench diff_phases \
//!   --no-default-features --features bundled,libgit-backend
//! ```
//!
//! Optional:
//! - `DIFF_PHASES_BLOB_CACHE=1` — unused by the probe (libgit2 blob cache stays off)
//! - `DIFF_PHASES_SKIP_SIMILAR=1` — skip `find_similar` (rename/copy)
//! - `DIFF_PHASES_SKIP_NUMSTAT=1` — tree-diff only (name-status); skips T1/T2

use duckdb_git::microbench::{BlobRing, PendingOlds};
use git2::{Oid, Repository};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

fn repo_path() -> String {
    std::env::var("BENCH_REPO").unwrap_or_else(|_| ".".to_string())
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes")
    )
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn print_pcts(label: &str, mut xs: Vec<u64>) {
    xs.sort_unstable();
    println!(
        "{label}: p50={} p99={} max={}  (n={})",
        percentile(&xs, 0.50),
        percentile(&xs, 0.99),
        xs.last().copied().unwrap_or(0),
        xs.len()
    );
}

#[derive(Default)]
struct Acc {
    commits: u64,
    deltas: u64,
    added: u64,
    deleted: u64,
    modified: u64,
    other: u64,
    blob_bytes: u64,
    tree: Duration,
    similar: Duration,
    blob: Duration,
    numstat: Duration,
}

impl Acc {
    fn add_timed(dst: &mut Duration, start: Instant) {
        *dst += start.elapsed();
    }

    fn print(&self, label: &str, wall: Duration) {
        let wall_ms = ms(wall);
        let pct = |d: Duration| {
            if wall_ms == 0.0 {
                0.0
            } else {
                100.0 * ms(d) / wall_ms
            }
        };
        println!("=== diff_phases ({label}) ===");
        println!(
            "commits={}  deltas={}  A={} D={} M={} other={}",
            self.commits, self.deltas, self.added, self.deleted, self.modified, self.other
        );
        println!("blob_bytes={:.1} MiB", mib(self.blob_bytes));
        println!("wall:          {wall_ms:9.1} ms");
        println!(
            "  tree-diff:   {:9.1} ms  ({:5.1}%)",
            ms(self.tree),
            pct(self.tree)
        );
        println!(
            "  find_similar:{:9.1} ms  ({:5.1}%)",
            ms(self.similar),
            pct(self.similar)
        );
        println!(
            "  blob-read:   {:9.1} ms  ({:5.1}%)   T0",
            ms(self.blob),
            pct(self.blob)
        );
        println!(
            "  numstat:     {:9.1} ms  ({:5.1}%)",
            ms(self.numstat),
            pct(self.numstat)
        );
    }
}

fn find_blob<'a>(repo: &'a Repository, oid: git2::Oid) -> Result<git2::Blob<'a>, git2::Error> {
    repo.find_blob(oid)
}

struct DeltaIds {
    old_id: Oid,
    new_id: Oid,
    /// Rename/copy uses `old_file` path; otherwise new (else old). `None` skips
    /// path-LRU `note_old` only.
    cache_path: Option<Vec<u8>>,
}

fn delta_ids(diff: &git2::Diff<'_>) -> Vec<DeltaIds> {
    let n = diff.deltas().len();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let delta = diff.get_delta(i).unwrap();
        let is_gitlink = delta.new_file().mode() == git2::FileMode::Commit
            || delta.old_file().mode() == git2::FileMode::Commit;
        if is_gitlink || delta.status() == git2::Delta::Typechange {
            continue;
        }
        let cache_path = match delta.status() {
            git2::Delta::Renamed | git2::Delta::Copied => {
                delta.old_file().path_bytes().map(|p| p.to_vec())
            }
            _ => delta
                .new_file()
                .path_bytes()
                .or_else(|| delta.old_file().path_bytes())
                .map(|p| p.to_vec()),
        };
        out.push(DeltaIds {
            old_id: delta.old_file().id(),
            new_id: delta.new_file().id(),
            cache_path,
        });
    }
    out
}

fn walk_non_merge(
    repo: &Repository,
    skip_similar: bool,
    mut on_commit: impl FnMut(Oid, Oid, &[DeltaIds]) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    for oid in revwalk {
        let oid = oid?;
        let commit = repo.find_commit(oid)?;
        if commit.parent_count() != 1 {
            continue;
        }
        let parent = commit.parent(0)?;
        let parent_oid = parent.id();
        let parent_tree = parent.tree()?;
        let current_tree = commit.tree()?;

        let mut diff_options = git2::DiffOptions::new();
        diff_options.include_typechange(true);
        let mut diff = repo.diff_tree_to_tree(
            Some(&parent_tree),
            Some(&current_tree),
            Some(&mut diff_options),
        )?;
        if !skip_similar {
            let mut find_opts = git2::DiffFindOptions::new();
            find_opts.renames(true).rename_threshold(50);
            diff.find_similar(Some(&mut find_opts))?;
        }
        let ids = delta_ids(&diff);
        on_commit(oid, parent_oid, &ids)?;
    }
    Ok(())
}

fn run_t0(
    path: &str,
    skip_similar: bool,
    skip_numstat: bool,
) -> Result<Acc, Box<dyn std::error::Error>> {
    let repo = Repository::open(path)?;
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;

    let mut acc = Acc::default();
    for oid in revwalk {
        let oid = oid?;
        let commit = repo.find_commit(oid)?;
        if commit.parent_count() != 1 {
            continue;
        }
        acc.commits += 1;

        let parent = commit.parent(0)?;
        let parent_tree = parent.tree()?;
        let current_tree = commit.tree()?;

        let mut diff_options = git2::DiffOptions::new();
        diff_options.include_typechange(true);

        let t0 = Instant::now();
        let mut diff = repo.diff_tree_to_tree(
            Some(&parent_tree),
            Some(&current_tree),
            Some(&mut diff_options),
        )?;
        Acc::add_timed(&mut acc.tree, t0);

        if !skip_similar {
            let t1 = Instant::now();
            let mut find_opts = git2::DiffFindOptions::new();
            find_opts.renames(true).rename_threshold(50);
            diff.find_similar(Some(&mut find_opts))?;
            Acc::add_timed(&mut acc.similar, t1);
        }

        let n = diff.deltas().len();
        acc.deltas += n as u64;

        if skip_numstat {
            for i in 0..n {
                match diff.get_delta(i).unwrap().status() {
                    git2::Delta::Added => acc.added += 1,
                    git2::Delta::Deleted => acc.deleted += 1,
                    git2::Delta::Modified => acc.modified += 1,
                    _ => acc.other += 1,
                }
            }
            continue;
        }

        for i in 0..n {
            let delta = diff.get_delta(i).unwrap();
            match delta.status() {
                git2::Delta::Added => acc.added += 1,
                git2::Delta::Deleted => acc.deleted += 1,
                git2::Delta::Modified => acc.modified += 1,
                _ => acc.other += 1,
            }

            let is_gitlink = delta.new_file().mode() == git2::FileMode::Commit
                || delta.old_file().mode() == git2::FileMode::Commit;
            if is_gitlink || delta.status() == git2::Delta::Typechange {
                continue;
            }

            let old_id = delta.old_file().id();
            let new_id = delta.new_file().id();

            let t2 = Instant::now();
            let old_blob = if old_id.is_zero() {
                None
            } else {
                Some(find_blob(&repo, old_id)?)
            };
            let new_blob = if new_id.is_zero() {
                None
            } else {
                Some(find_blob(&repo, new_id)?)
            };
            Acc::add_timed(&mut acc.blob, t2);

            let old_content = old_blob.as_ref().map(|b| b.content()).unwrap_or(&[]);
            let new_content = new_blob.as_ref().map(|b| b.content()).unwrap_or(&[]);
            acc.blob_bytes += (old_content.len() + new_content.len()) as u64;

            let t3 = Instant::now();
            let _ = duckdb_git::microbench::diff_line_counts(old_content, new_content, false)?;
            Acc::add_timed(&mut acc.numstat, t3);
        }
    }

    Ok(acc)
}

struct T1 {
    blob: Duration,
    copy_tax: Duration,
    blob_sizes: Vec<u64>,
    gen_insert_bytes: Vec<u64>,
}

fn run_t1(path: &str, skip_similar: bool) -> Result<T1, Box<dyn std::error::Error>> {
    let repo = Repository::open(path)?;
    let mut prev: HashMap<Oid, Vec<u8>> = HashMap::new();
    let mut next: HashMap<Oid, Vec<u8>> = HashMap::new();
    let mut blob = Duration::ZERO;
    let mut copy_tax = Duration::ZERO;
    let mut blob_sizes = Vec::new();
    let mut gen_insert_bytes = Vec::new();

    walk_non_merge(&repo, skip_similar, |_, _, deltas| {
        next.clear();
        for d in deltas {
            let mut keep: Vec<git2::Blob<'_>> = Vec::new();

            let old_hit = if d.old_id.is_zero() {
                None
            } else {
                prev.get(&d.old_id).map(|v| v.as_slice())
            };
            if old_hit.is_none() && !d.old_id.is_zero() {
                let t = Instant::now();
                keep.push(find_blob(&repo, d.old_id)?);
                blob += t.elapsed();
            }

            let new_hit = if d.new_id.is_zero() {
                None
            } else {
                prev.get(&d.new_id).map(|v| v.as_slice())
            };
            if new_hit.is_none() && !d.new_id.is_zero() {
                let t = Instant::now();
                keep.push(find_blob(&repo, d.new_id)?);
                blob += t.elapsed();
            }

            let old_s: &[u8] = if d.old_id.is_zero() {
                &[]
            } else if let Some(s) = old_hit {
                blob_sizes.push(s.len() as u64);
                s
            } else {
                let s = keep[0].content();
                blob_sizes.push(s.len() as u64);
                s
            };
            let new_s: &[u8] = if d.new_id.is_zero() {
                &[]
            } else if let Some(s) = new_hit {
                blob_sizes.push(s.len() as u64);
                s
            } else {
                let idx = keep.len() - 1;
                let s = keep[idx].content();
                blob_sizes.push(s.len() as u64);
                s
            };

            let _ = duckdb_git::microbench::diff_line_counts(old_s, new_s, false)?;

            if !d.old_id.is_zero() && !next.contains_key(&d.old_id) {
                let t = Instant::now();
                next.insert(d.old_id, old_s.to_vec());
                copy_tax += t.elapsed();
            }
        }

        let gen_bytes: u64 = next.values().map(|v| v.len() as u64).sum();
        gen_insert_bytes.push(gen_bytes);
        std::mem::swap(&mut prev, &mut next);
        next.clear();
        Ok(())
    })?;

    Ok(T1 {
        blob,
        copy_tax,
        blob_sizes,
        gen_insert_bytes,
    })
}

struct Dist {
    lookups: u64,
    lookup_bytes: u64,
    hits: u64,
    hit_bytes: u64,
    u_time: Duration,
    /// index 0 unused; 1..=2048; 2049+ in overflow.
    dist_count: [u64; 2049],
    dist_bytes: [u64; 2049],
    dist_fp_count: [u64; 2049],
    dist_fp_bytes: [u64; 2049],
    overflow_count: u64,
    overflow_bytes: u64,
    overflow_fp_count: u64,
    overflow_fp_bytes: u64,
    commits: u64,
    fp_adjacent: u64,
}

impl Default for Dist {
    fn default() -> Self {
        Self {
            lookups: 0,
            lookup_bytes: 0,
            hits: 0,
            hit_bytes: 0,
            u_time: Duration::ZERO,
            dist_count: [0; 2049],
            dist_bytes: [0; 2049],
            dist_fp_count: [0; 2049],
            dist_fp_bytes: [0; 2049],
            overflow_count: 0,
            overflow_bytes: 0,
            overflow_fp_count: 0,
            overflow_fp_bytes: 0,
            commits: 0,
            fp_adjacent: 0,
        }
    }
}

fn bin_hit(dst: &mut Dist, dist: u64, bytes: u64, first_parent: bool) {
    if dist == 0 {
        return;
    }
    if dist >= 2049 {
        dst.overflow_count += 1;
        dst.overflow_bytes += bytes;
        if first_parent {
            dst.overflow_fp_count += 1;
            dst.overflow_fp_bytes += bytes;
        }
        return;
    }
    let i = dist as usize;
    dst.dist_count[i] += 1;
    dst.dist_bytes[i] += bytes;
    if first_parent {
        dst.dist_fp_count[i] += 1;
        dst.dist_fp_bytes[i] += bytes;
    }
}

fn run_dist(path: &str, skip_similar: bool) -> Result<Dist, Box<dyn std::error::Error>> {
    let repo = Repository::open(path)?;
    let mut inserted: HashMap<Oid, u64> = HashMap::new();
    let mut gen: u64 = 0;
    let mut prev_first_parent: Option<Oid> = None;
    let mut dst = Dist::default();

    walk_non_merge(&repo, skip_similar, |commit_oid, parent_oid, deltas| {
        let first_parent = prev_first_parent == Some(commit_oid);
        dst.commits += 1;
        if first_parent {
            dst.fp_adjacent += 1;
        }

        let mut this_olds: Vec<Oid> = Vec::new();
        for d in deltas {
            for (id, is_old) in [(d.old_id, true), (d.new_id, false)] {
                if id.is_zero() {
                    continue;
                }
                let t = Instant::now();
                let blob = find_blob(&repo, id)?;
                let find_dt = t.elapsed();
                let bytes = blob.size() as u64;
                dst.lookups += 1;
                dst.lookup_bytes += bytes;
                if let Some(&insert_gen) = inserted.get(&id) {
                    let dist = gen.saturating_sub(insert_gen);
                    dst.hits += 1;
                    dst.hit_bytes += bytes;
                    dst.u_time += find_dt;
                    bin_hit(&mut dst, dist, bytes, first_parent);
                }
                if is_old {
                    this_olds.push(id);
                }
            }
        }

        for old_id in this_olds {
            inserted.insert(old_id, gen);
        }
        gen += 1;
        prev_first_parent = Some(parent_oid);
        Ok(())
    })?;

    Ok(dst)
}

fn print_t1(t0_blob: Duration, t1: &T1) {
    let t0 = t0_blob;
    let saved = t0.saturating_sub(t1.blob);
    println!("=== probe pass1 T1 (one-gen HashMap) ===");
    println!("  blob-read T1: {:9.1} ms", ms(t1.blob));
    println!(
        "  copy_tax:     {:9.1} ms  (all olds into next, hits included)",
        ms(t1.copy_tax)
    );
    println!("  T0:           {:9.1} ms", ms(t0));
    println!("  T0-T1:        {:9.1} ms", ms(saved));
    println!(
        "  (T0-T1) > copy_tax: {}",
        if saved > t1.copy_tax { "yes" } else { "no" }
    );
    print_pcts("  blob bytes", t1.blob_sizes.clone());
    print_pcts("  gen insert bytes", t1.gen_insert_bytes.clone());
    let mut gen = t1.gen_insert_bytes.clone();
    gen.sort_unstable();
    let p99 = percentile(&gen, 0.99);
    println!(
        "  gen_insert p99 × 11 = {:.1} MiB  (K=11 bench replica)",
        mib(p99.saturating_mul(11))
    );
}

fn print_dist(d: &Dist) {
    println!("=== probe pass2 distance (always find_blob) ===");
    println!(
        "commits={}  first-parent-adjacent={} ({:.1}%)",
        d.commits,
        d.fp_adjacent,
        if d.commits == 0 {
            0.0
        } else {
            100.0 * d.fp_adjacent as f64 / d.commits as f64
        }
    );
    println!(
        "lookups={}  lookup_bytes={:.1} MiB",
        d.lookups,
        mib(d.lookup_bytes)
    );
    let u = if d.lookup_bytes == 0 {
        0.0
    } else {
        d.hit_bytes as f64 / d.lookup_bytes as f64
    };
    println!(
        "hits={}  hit_bytes={:.1} MiB  u={:.3}  u_time={:.1} ms",
        d.hits,
        mib(d.hit_bytes),
        u,
        ms(d.u_time)
    );

    let pct_all = |bytes: u64| {
        if d.lookup_bytes == 0 {
            0.0
        } else {
            100.0 * bytes as f64 / d.lookup_bytes as f64
        }
    };

    println!("distance bins (count, bytes, % of all lookup bytes; fp = first-parent adjacent):");
    let mut cum_bytes = 0u64;
    for dist in 1..=64u64 {
        let i = dist as usize;
        let bytes = d.dist_bytes[i];
        cum_bytes += bytes;
        if d.dist_count[i] == 0 {
            continue;
        }
        println!(
            "  d={:<4}  n={:<8}  {:>8.1} MiB  {:>5.1}%   cum={:>5.1}%   fp_n={} fp_bytes={:.1} MiB",
            dist,
            d.dist_count[i],
            mib(bytes),
            pct_all(bytes),
            pct_all(cum_bytes),
            d.dist_fp_count[i],
            mib(d.dist_fp_bytes[i])
        );
    }
    let mid: u64 = (65..=2048).map(|i| d.dist_bytes[i as usize]).sum();
    let mid_n: u64 = (65..=2048).map(|i| d.dist_count[i as usize]).sum();
    println!(
        "  d=65..2048  n={:<8}  {:>8.1} MiB  {:>5.1}%  (flag, not K)",
        mid_n,
        mib(mid),
        pct_all(mid)
    );
    println!(
        "  d=2049+     n={:<8}  {:>8.1} MiB  {:>5.1}%  (TLS would be needed)",
        d.overflow_count,
        mib(d.overflow_bytes),
        pct_all(d.overflow_bytes)
    );
    println!(
        "  d1 / all lookup bytes = {:.3}",
        pct_all(d.dist_bytes[1]) / 100.0
    );
    let d2k: u64 = (2..=64).map(|i| d.dist_bytes[i]).sum();
    println!("  d2..64 / all lookup bytes = {:.3}", pct_all(d2k) / 100.0);
}

/// Bench-local copy of the former production K=11 generation ring. Independent
/// of `BlobRing`. Empty pending still consumes a generation slot.
const K11: usize = 11;

struct GenPending {
    olds: HashMap<Oid, Option<Vec<u8>>>,
}

impl GenPending {
    fn contains(&self, oid: Oid) -> bool {
        self.olds.contains_key(&oid)
    }

    fn record_hit(&mut self, oid: Oid) {
        if oid.is_zero() {
            return;
        }
        self.olds.entry(oid).or_insert(None);
    }

    fn record_miss(&mut self, oid: Oid, bytes: Vec<u8>) {
        if oid.is_zero() {
            return;
        }
        self.olds.entry(oid).or_insert(Some(bytes));
    }
}

struct GenRing {
    by_oid: HashMap<Oid, (u64, Vec<u8>)>,
    gens: VecDeque<(u64, Vec<Oid>)>,
    gen: u64,
}

impl GenRing {
    fn new() -> Self {
        Self {
            by_oid: HashMap::new(),
            gens: VecDeque::new(),
            gen: 0,
        }
    }

    fn lookup(&self, oid: Oid) -> Option<&[u8]> {
        if oid.is_zero() {
            return None;
        }
        self.by_oid.get(&oid).map(|(_, bytes)| bytes.as_slice())
    }

    fn finish_commit(&mut self, pending: GenPending) {
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
        if self.gens.len() > K11 {
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

#[derive(Default, Clone)]
struct HitSide {
    lookups: u64,
    hits: u64,
    bytes: u64,
    hit_bytes: u64,
}

impl HitSide {
    fn record(&mut self, hit: bool, n: usize) {
        self.lookups += 1;
        self.bytes += n as u64;
        if hit {
            self.hits += 1;
            self.hit_bytes += n as u64;
        }
    }
}

#[derive(Default, Clone)]
struct HitAcc {
    old: HitSide,
    new: HitSide,
}

fn cpu_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(4)
}

fn prod_batch_size(commit_count: usize) -> usize {
    (commit_count / cpu_cores()).clamp(1, 2048)
}

fn collect_commit_oids(repo: &Repository) -> Result<Vec<Oid>, Box<dyn std::error::Error>> {
    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    let mut oids = Vec::new();
    for oid in revwalk {
        oids.push(oid?);
    }
    Ok(oids)
}

fn file_change_deltas(
    repo: &Repository,
    oid: Oid,
    skip_similar: bool,
) -> Result<Option<Vec<DeltaIds>>, Box<dyn std::error::Error>> {
    let commit = repo.find_commit(oid)?;
    let parent_count = commit.parent_count();
    if parent_count > 1 {
        return Ok(None);
    }
    if parent_count == 0 {
        return Ok(Some(Vec::new()));
    }

    let parent = commit.parent(0)?;
    let parent_tree = parent.tree()?;
    let current_tree = commit.tree()?;
    let mut diff_options = git2::DiffOptions::new();
    diff_options.include_typechange(true);
    let mut diff = repo.diff_tree_to_tree(
        Some(&parent_tree),
        Some(&current_tree),
        Some(&mut diff_options),
    )?;
    if !skip_similar {
        let mut find_opts = git2::DiffFindOptions::new();
        find_opts.renames(true).rename_threshold(50);
        diff.find_similar(Some(&mut find_opts))?;
    }
    Ok(Some(delta_ids(&diff)))
}

fn print_hit_side(indent: &str, label: &str, s: &HitSide) {
    println!(
        "{indent}{label:<12} lookups={:<10} hits={:<10} {:>5.1}%   bytes={:>8.1} MiB  hit={:>8.1} MiB  {:>5.1}%",
        s.lookups,
        s.hits,
        if s.lookups == 0 {
            0.0
        } else {
            100.0 * s.hits as f64 / s.lookups as f64
        },
        mib(s.bytes),
        mib(s.hit_bytes),
        if s.bytes == 0 {
            0.0
        } else {
            100.0 * s.hit_bytes as f64 / s.bytes as f64
        }
    );
}

fn print_hit_acc(label: &str, acc: &HitAcc) {
    println!("  {label}");
    print_hit_side("    ", "new", &acc.new);
    print_hit_side("    ", "old", &acc.old);
}

fn run_cache_probe(
    path: &str,
    skip_similar: bool,
    batch: usize,
    caps: &[usize],
    with_k11: bool,
) -> Result<(Option<HitAcc>, Vec<(usize, HitAcc)>), Box<dyn std::error::Error>> {
    let repo = Repository::open(path)?;
    let oids = collect_commit_oids(&repo)?;
    let batch = batch.max(1);

    let mut k11_acc = HitAcc::default();
    let mut path_accs: Vec<HitAcc> = caps.iter().map(|_| HitAcc::default()).collect();

    for chunk in oids.chunks(batch) {
        let mut k11 = GenRing::new();
        let mut rings: Vec<BlobRing> = caps.iter().map(|&cap| BlobRing::with_cap(cap)).collect();

        for &oid in chunk {
            let Some(deltas) = file_change_deltas(&repo, oid, skip_similar)? else {
                continue;
            };
            if deltas.is_empty() {
                if with_k11 {
                    k11.finish_commit(GenPending {
                        olds: HashMap::new(),
                    });
                }
                for ring in &mut rings {
                    ring.finish_commit(PendingOlds::default());
                }
                continue;
            }

            let mut k11_pending = GenPending {
                olds: HashMap::new(),
            };
            let mut path_pendings: Vec<PendingOlds> =
                caps.iter().map(|_| PendingOlds::default()).collect();

            for d in &deltas {
                let k11_old = if with_k11 {
                    k11.lookup(d.old_id).map(|b| b.len())
                } else {
                    None
                };
                let k11_new = if with_k11 {
                    k11.lookup(d.new_id).map(|b| b.len())
                } else {
                    None
                };
                let path_old: Vec<Option<usize>> = rings
                    .iter()
                    .map(|r| r.lookup(d.old_id).map(|b| b.len()))
                    .collect();
                let path_new: Vec<Option<usize>> = rings
                    .iter()
                    .map(|r| r.lookup(d.new_id).map(|b| b.len()))
                    .collect();

                let load_old = !d.old_id.is_zero()
                    && ((with_k11 && k11_old.is_none()) || path_old.iter().any(|l| l.is_none()));
                let load_new = !d.new_id.is_zero()
                    && ((with_k11 && k11_new.is_none()) || path_new.iter().any(|l| l.is_none()));
                let old_blob = if load_old {
                    Some(find_blob(&repo, d.old_id)?)
                } else {
                    None
                };
                let new_blob = if load_new {
                    Some(find_blob(&repo, d.new_id)?)
                } else {
                    None
                };
                let old_loaded = old_blob.as_ref().map(|b| b.size());
                let new_loaded = new_blob.as_ref().map(|b| b.size());

                if with_k11 && !d.old_id.is_zero() {
                    let n = k11_old.or(old_loaded).expect("old size");
                    k11_acc.old.record(k11_old.is_some(), n);
                }
                if with_k11 && !d.new_id.is_zero() {
                    let n = k11_new.or(new_loaded).expect("new size");
                    k11_acc.new.record(k11_new.is_some(), n);
                }
                for i in 0..caps.len() {
                    if !d.old_id.is_zero() {
                        let n = path_old[i].or(old_loaded).expect("old size");
                        path_accs[i].old.record(path_old[i].is_some(), n);
                    }
                    if !d.new_id.is_zero() {
                        let n = path_new[i].or(new_loaded).expect("new size");
                        path_accs[i].new.record(path_new[i].is_some(), n);
                    }
                }

                if with_k11 && !d.old_id.is_zero() && !k11_pending.contains(d.old_id) {
                    if k11_old.is_some() {
                        k11_pending.record_hit(d.old_id);
                    } else if let Some(blob) = &old_blob {
                        k11_pending.record_miss(d.old_id, blob.content().to_vec());
                    }
                }
                if let Some(cache_path) = &d.cache_path {
                    if !d.old_id.is_zero() {
                        for i in 0..caps.len() {
                            if path_old[i].is_some() {
                                path_pendings[i].record_hit(cache_path.clone(), d.old_id);
                            } else if let Some(blob) = &old_blob {
                                path_pendings[i].record_miss(
                                    cache_path.clone(),
                                    d.old_id,
                                    blob.content().to_vec(),
                                );
                            }
                        }
                    }
                }
            }

            if with_k11 {
                k11.finish_commit(k11_pending);
            }
            for (ring, pending) in rings.iter_mut().zip(path_pendings) {
                ring.finish_commit(pending);
            }
        }
    }

    let k11_out = if with_k11 { Some(k11_acc) } else { None };
    let path_out = caps.iter().copied().zip(path_accs).collect();
    Ok((k11_out, path_out))
}

fn print_cache_probe(batch: usize, cores: usize, k11: Option<&HitAcc>, path: &[(usize, HitAcc)]) {
    println!("=== probe BlobRing K=11 vs path last-old (batch={batch}, cores={cores}) ===");
    println!("  (merge: walk but skip file_changes+finish; root: empty finish)");
    if let Some(acc) = k11 {
        print_hit_acc("K=11 (empty gens consume a slot)", acc);
    }
    for (cap, acc) in path {
        print_hit_acc(&format!("cap={} MiB", cap / (1024 * 1024)), acc);
    }
}

fn main() {
    let path = repo_path();
    let skip_similar = env_flag("DIFF_PHASES_SKIP_SIMILAR");
    let skip_numstat = env_flag("DIFF_PHASES_SKIP_NUMSTAT");
    if env_flag("DIFF_PHASES_BLOB_CACHE") {
        eprintln!("note: DIFF_PHASES_BLOB_CACHE is ignored; probe keeps libgit2 blob cache off");
    }

    let mut label = String::from(&path);
    if skip_similar {
        label.push_str(" no_similar");
    }
    if skip_numstat {
        label.push_str(" no_numstat");
    }

    let _ = run_t0(&path, skip_similar, skip_numstat).expect("warmup");

    let start = Instant::now();
    let acc = run_t0(&path, skip_similar, skip_numstat).expect("T0");
    acc.print(&label, start.elapsed());

    if skip_numstat {
        println!("skip T1/T2 (DIFF_PHASES_SKIP_NUMSTAT)");
    } else {
        let t1 = run_t1(&path, skip_similar).expect("T1");
        print_t1(acc.blob, &t1);

        let dist = run_dist(&path, skip_similar).expect("distance");
        print_dist(&dist);
    }

    let mib = 1024 * 1024;
    let cores = cpu_cores();
    let n_commits = {
        let repo = Repository::open(&path).expect("repo");
        collect_commit_oids(&repo).expect("oids").len()
    };
    let batch = prod_batch_size(n_commits);
    let (k11, path_hits) = run_cache_probe(
        &path,
        skip_similar,
        batch,
        &[8 * mib, 16 * mib, 32 * mib, 64 * mib],
        true,
    )
    .expect("cache probe");
    print_cache_probe(batch, cores, k11.as_ref(), &path_hits);

    for extra in [1usize, 2048] {
        if extra == batch {
            continue;
        }
        let (k11, path_hits) =
            run_cache_probe(&path, skip_similar, extra, &[32 * mib], true).expect("cache extra");
        print_cache_probe(extra, cores, k11.as_ref(), &path_hits);
    }
}
