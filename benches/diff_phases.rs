//! One-shot breakdown of libgit `with_diff` work on `BENCH_REPO`.
//!
//! Splits each non-merge commit into tree-diff, find_similar, blob inflate,
//! and xdiff/numstat so we can see what dominates on large histories.
//!
//! After warmup, runs three probe passes (blob-cache env is ignored — always off):
//! - pass 0 (T0): plain `find_blob`
//! - pass 1 (T1): one-generation `HashMap` (lookup previous olds; insert this
//!   commit's olds into a separate map; swap at end). `copy_tax` is memcpy of
//!   **all** olds into the next map, hits included.
//! - pass 2: always `find_blob`; distance histogram (1, 2, … and 2049+).
//!   Insert old only, lookup old and new. Do not insert the current generation
//!   until lookups finish. Same OID keeps the latest insert generation.
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

use git2::{Oid, Repository};
use std::collections::HashMap;
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
        out.push(DeltaIds {
            old_id: delta.old_file().id(),
            new_id: delta.new_file().id(),
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
        "  gen_insert p99 × 11 = {:.1} MiB  (production BlobRing K)",
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
        return;
    }

    let t1 = run_t1(&path, skip_similar).expect("T1");
    print_t1(acc.blob, &t1);

    let dist = run_dist(&path, skip_similar).expect("distance");
    print_dist(&dist);
}
