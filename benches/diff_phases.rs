//! One-shot breakdown of libgit `with_diff` work on `BENCH_REPO`.
//!
//! Splits each non-merge commit into tree-diff, find_similar, blob inflate,
//! and xdiff/numstat so we can see what dominates on large histories.
//!
//! ```bash
//! BENCH_REPO=/path/to/repo cargo bench --bench diff_phases \
//!   --no-default-features --features bundled,libgit-backend
//! ```
//!
//! Optional:
//! - `DIFF_PHASES_BLOB_CACHE=1` — enable libgit2 blob object cache (64 MiB)
//! - `DIFF_PHASES_SKIP_SIMILAR=1` — skip `find_similar` (rename/copy)
//! - `DIFF_PHASES_SKIP_NUMSTAT=1` — tree-diff only (name-status)

use git2::Repository;
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
        let ms = |d: Duration| d.as_secs_f64() * 1000.0;
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
        println!(
            "blob_bytes={:.1} MiB",
            self.blob_bytes as f64 / (1024.0 * 1024.0)
        );
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
            "  blob-read:   {:9.1} ms  ({:5.1}%)",
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

fn find_blob<'a>(
    repo: &'a Repository,
    oid: git2::Oid,
) -> Result<git2::Blob<'a>, git2::Error> {
    repo.find_blob(oid)
}

fn run(path: &str, blob_cache: bool, skip_similar: bool, skip_numstat: bool) -> Result<Acc, Box<dyn std::error::Error>> {
    if blob_cache {
        // Default is 0 for blobs (never cached). Allow blobs up to 2 MiB.
        unsafe {
            git2::opts::set_cache_object_limit(git2::ObjectType::Blob, 2 * 1024 * 1024)?;
        }
    }

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

fn main() {
    let path = repo_path();
    let blob_cache = env_flag("DIFF_PHASES_BLOB_CACHE");
    let skip_similar = env_flag("DIFF_PHASES_SKIP_SIMILAR");
    let skip_numstat = env_flag("DIFF_PHASES_SKIP_NUMSTAT");

    let mut label = String::from(&path);
    if blob_cache {
        label.push_str(" blob_cache");
    }
    if skip_similar {
        label.push_str(" no_similar");
    }
    if skip_numstat {
        label.push_str(" no_numstat");
    }

    // Warm pack windows / page cache, then measure.
    let _ = run(&path, blob_cache, skip_similar, skip_numstat).expect("warmup");
    let start = Instant::now();
    let acc = run(&path, blob_cache, skip_similar, skip_numstat).expect("diff_phases");
    acc.print(&label, start.elapsed());
}
