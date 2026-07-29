use crate::git_log::params::{
    unresolved_revision_error, DecorateFormat, DiffMerges, GitLogParameter, RevisionTerm,
};
use crate::git_log::ref_index::{ContainedIndex, RefBits};
use crate::git_log::schema;
use crate::git_log::types::{gitlink_numstat, CommitData, FileChange};
use crate::git_log::vector::VectorInserter;
use crate::git_log::{GitLogReadPlanner, GitLogReader};
use duckdb::core::DataChunkHandle;
use git2::Repository;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, Repository)>> = const { RefCell::new(None) };
}

/// Holds at most one repository per thread: dropping a handle installs its
/// repository, evicting whatever was cached for a different path. A query
/// alternating between repositories on the same worker thread therefore
/// misses every time.
struct CachedRepo {
    repo: Option<Repository>,
    repo_path: String,
}

impl CachedRepo {
    fn open(repo_path: &str) -> Result<Self, Box<dyn Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1),
            _ => Repository::open(repo_path),
        })?;
        Ok(CachedRepo {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
        })
    }

    fn repo(&self) -> &Repository {
        self.repo.as_ref().expect("repo is present outside Drop")
    }
}

impl Drop for CachedRepo {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), repo));
            });
        }
    }
}

fn walk_commit_oids(
    repo: &Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
) -> Result<Vec<git2::Oid>, Box<dyn Error>> {
    let mut revwalk = repo.revwalk()?;

    match revision {
        Some(terms) => {
            for term in terms {
                let obj = repo
                    .revparse_single(&term.spec)
                    .map_err(|_| -> Box<dyn Error> {
                        unresolved_revision_error(&term.origin).into()
                    })?;
                if term.negate {
                    revwalk.hide(obj.id())?;
                } else {
                    revwalk.push(obj.id())?;
                }
            }
        }
        None => {
            revwalk.push_head()?;
        }
    }

    let revwalk_iter: Box<dyn Iterator<Item = _>> = match max_count {
        Some(count) => Box::new(revwalk.take(count)),
        None => Box::new(revwalk),
    };

    let mut commit_oids = Vec::new();
    for oid in revwalk_iter {
        commit_oids.push(oid?);
    }

    Ok(commit_oids)
}

fn read_commit(
    repo: &Repository,
    oid: git2::Oid,
    ignore_all_space: bool,
    skip_file_changes: bool,
    diff_merges: DiffMerges,
) -> Result<CommitData, Box<dyn Error>> {
    let commit = repo.find_commit(oid)?;
    let author = commit.author();
    let committer = commit.committer();

    let skip = skip_file_changes || (diff_merges == DiffMerges::Off && commit.parent_count() > 1);
    let file_changes = if skip {
        Vec::new()
    } else {
        collect_file_changes(repo, &commit, ignore_all_space)?
    };

    Ok(CommitData {
        author_name: author.name_bytes().to_vec(),
        author_email: author.email_bytes().to_vec(),
        author_timestamp: author.when().seconds(),
        committer_name: committer.name_bytes().to_vec(),
        committer_email: committer.email_bytes().to_vec(),
        committer_timestamp: committer.when().seconds(),
        message: commit.message_bytes().to_vec(),
        parents: (0..commit.parent_count())
            .map(|i| commit.parent_id(i).unwrap().to_string())
            .collect(),
        file_changes,
    })
}

fn collect_refs(
    repo: &Repository,
    format: DecorateFormat,
) -> Result<HashMap<git2::Oid, Vec<String>>, Box<dyn Error>> {
    let mut refs_map: HashMap<git2::Oid, Vec<String>> = HashMap::new();
    for reference in repo.references()? {
        let reference = reference?;
        let name = match format {
            DecorateFormat::Short => reference.shorthand().unwrap_or("").to_string(),
            DecorateFormat::Full => reference.name().unwrap_or("").to_string(),
        };
        if name.is_empty() {
            continue;
        }
        if let Ok(commit) = reference.peel_to_commit() {
            refs_map.entry(commit.id()).or_default().push(name);
        }
    }
    Ok(refs_map)
}

fn build_contained_index(
    repo: &Repository,
    format: DecorateFormat,
    need_branches: bool,
    need_tags: bool,
    wanted: &HashSet<git2::Oid>,
) -> Result<ContainedIndex<git2::Oid>, Box<dyn Error>> {
    let mut branch_refs = Vec::new();
    if need_branches {
        for branch in repo.branches(None)? {
            let (branch, _branch_type) = branch?;
            let reference = branch.get();
            if reference.kind() != Some(git2::ReferenceType::Direct) {
                continue;
            }
            let name = match format {
                DecorateFormat::Short => reference.shorthand().unwrap_or("").to_string(),
                DecorateFormat::Full => reference.name().unwrap_or("").to_string(),
            };
            if name.is_empty() {
                continue;
            }
            if let Ok(tip) = reference.peel_to_commit() {
                branch_refs.push((tip.id(), name));
            }
        }
    }

    let mut tag_refs = Vec::new();
    if need_tags {
        for reference in repo.references()? {
            let reference = reference?;
            if !reference.is_tag() {
                continue;
            }
            let name = match format {
                DecorateFormat::Short => reference.shorthand().unwrap_or("").to_string(),
                DecorateFormat::Full => reference.name().unwrap_or("").to_string(),
            };
            if name.is_empty() {
                continue;
            }
            if let Ok(tip) = reference.peel_to_commit() {
                tag_refs.push((tip.id(), name));
            }
        }
    }

    if branch_refs.is_empty() && tag_refs.is_empty() {
        return Ok(ContainedIndex::empty());
    }

    let mut branch_names: Vec<String> = branch_refs.iter().map(|(_, name)| name.clone()).collect();
    branch_names.sort_unstable();
    branch_names.dedup();

    let mut tag_names: Vec<String> = tag_refs.iter().map(|(_, name)| name.clone()).collect();
    tag_names.sort_unstable();
    tag_names.dedup();

    let branch_words = branch_names.len().div_ceil(64);
    let tag_words = tag_names.len().div_ceil(64);
    let total_words = branch_words + tag_words;

    if wanted.is_empty() {
        return Ok(ContainedIndex {
            branch_names,
            tag_names,
            branch_words,
            bits: HashMap::new(),
        });
    }

    let mut revwalk = repo.revwalk()?;
    revwalk.set_sorting(git2::Sort::TOPOLOGICAL)?;

    let mut pending: HashMap<git2::Oid, RefBits> = HashMap::new();
    for (tip, name) in &branch_refs {
        let bit = branch_names
            .binary_search(name)
            .expect("name collected above");
        pending
            .entry(*tip)
            .or_insert_with(|| RefBits::new(total_words))
            .set(bit);
        revwalk.push(*tip)?;
    }
    for (tip, name) in &tag_refs {
        let bit = branch_words * 64 + tag_names.binary_search(name).expect("name collected above");
        pending
            .entry(*tip)
            .or_insert_with(|| RefBits::new(total_words))
            .set(bit);
        revwalk.push(*tip)?;
    }

    let mut bits: HashMap<git2::Oid, RefBits> = HashMap::new();
    for oid in revwalk {
        let oid = oid?;
        let Some(cur_bits) = pending.remove(&oid) else {
            continue;
        };
        let commit = repo.find_commit(oid)?;
        for parent in commit.parent_ids() {
            pending
                .entry(parent)
                .or_insert_with(|| RefBits::new(total_words))
                .or_assign(&cur_bits);
        }
        if wanted.contains(&oid) {
            bits.insert(oid, cur_bits);
            if bits.len() == wanted.len() {
                break;
            }
        }
    }

    Ok(ContainedIndex {
        branch_names,
        tag_names,
        branch_words,
        bits,
    })
}

fn collect_file_changes(
    repo: &Repository,
    commit: &git2::Commit,
    ignore_all_space: bool,
) -> Result<Vec<FileChange>, git2::Error> {
    let mut file_changes = Vec::new();

    if commit.parent_count() == 0 {
        let tree = commit.tree()?;
        tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
            if entry.kind() == Some(git2::ObjectType::Tree) {
                return git2::TreeWalkResult::Ok;
            }
            if let Some(name) = entry.name() {
                let oid = entry.id();
                if entry.kind() == Some(git2::ObjectType::Commit) {
                    let (add_lines, del_lines) = gitlink_numstat("A");
                    file_changes.push(FileChange {
                        path: format!("{}{}", root, name),
                        old_path: None,
                        status: "A",
                        blob_id: oid.to_string(),
                        file_size: None,
                        add_lines,
                        del_lines,
                    });
                } else if let Ok(blob) = repo.find_blob(oid) {
                    let content = std::str::from_utf8(blob.content());
                    let add_lines = if let Ok(text) = content {
                        text.lines().count() as i32
                    } else {
                        0
                    };
                    file_changes.push(FileChange {
                        path: format!("{}{}", root, name),
                        old_path: None,
                        status: "A",
                        blob_id: oid.to_string(),
                        file_size: Some(blob.size() as i64),
                        add_lines,
                        del_lines: 0,
                    });
                }
            }
            git2::TreeWalkResult::Ok
        })?;
        return Ok(file_changes);
    }

    let parent = commit.parent(0)?;
    let parent_tree = parent.tree()?;
    let current_tree = commit.tree()?;

    let mut diff_options = git2::DiffOptions::new();
    if ignore_all_space {
        diff_options.ignore_whitespace(true);
    }

    let mut diff = repo.diff_tree_to_tree(
        Some(&parent_tree),
        Some(&current_tree),
        Some(&mut diff_options),
    )?;

    let mut find_opts = git2::DiffFindOptions::new();
    find_opts
        .renames(true)
        .rename_threshold(50)
        .ignore_whitespace(ignore_all_space);

    diff.find_similar(Some(&mut find_opts))?;

    file_changes.reserve(diff.deltas().len());

    for i in 0..diff.deltas().len() {
        let delta = diff.get_delta(i).unwrap();

        let status = match delta.status() {
            git2::Delta::Added => "A",
            git2::Delta::Deleted => "D",
            git2::Delta::Modified => "M",
            git2::Delta::Renamed => "R",
            git2::Delta::Copied => "C",
            git2::Delta::Ignored => "I",
            git2::Delta::Untracked => "?",
            git2::Delta::Typechange => "T",
            _ => "U",
        };

        let file_path = if let Some(new_file) = delta.new_file().path() {
            new_file.to_string_lossy().to_string()
        } else if let Some(old_file) = delta.old_file().path() {
            old_file.to_string_lossy().to_string()
        } else {
            "unknown".to_string()
        };

        let old_path = match delta.status() {
            git2::Delta::Renamed | git2::Delta::Copied => delta
                .old_file()
                .path()
                .map(|p| p.to_string_lossy().to_string()),
            _ => None,
        };

        let is_gitlink = delta.new_file().mode() == git2::FileMode::Commit
            || delta.old_file().mode() == git2::FileMode::Commit;

        let (blob_id, file_size, add_lines, del_lines) = if is_gitlink {
            let id = if delta.new_file().path().is_some() {
                delta.new_file().id().to_string()
            } else if delta.old_file().path().is_some() {
                delta.old_file().id().to_string()
            } else {
                "unknown".to_string()
            };
            let (add, del) = gitlink_numstat(status);
            (id, None, add, del)
        } else {
            let (blob_id, file_size) = if delta.new_file().path().is_some() {
                (
                    delta.new_file().id().to_string(),
                    Some(delta.new_file().size() as i64),
                )
            } else if delta.old_file().path().is_some() {
                (
                    delta.old_file().id().to_string(),
                    Some(delta.old_file().size() as i64),
                )
            } else {
                ("unknown".to_string(), Some(0))
            };

            let (add, del) = {
                let old_id = delta.old_file().id();
                let new_id = delta.new_file().id();
                let old_blob;
                let old_content: &[u8] = if old_id.is_zero() {
                    &[]
                } else {
                    old_blob = repo.find_blob(old_id)?;
                    old_blob.content()
                };
                let new_blob;
                let new_content: &[u8] = if new_id.is_zero() {
                    &[]
                } else {
                    new_blob = repo.find_blob(new_id)?;
                    new_blob.content()
                };
                super::xdiff::diff_line_counts(old_content, new_content, ignore_all_space)
                    .map_err(|e| git2::Error::from_str(&e.to_string()))?
            };

            (blob_id, file_size, add, del)
        };

        file_changes.push(FileChange {
            path: file_path,
            old_path,
            status,
            blob_id,
            file_size,
            add_lines,
            del_lines,
        });
    }

    Ok(file_changes)
}

struct LibGitLogReadPlannerInner {
    commit_oids: Vec<git2::Oid>,
    decorations: HashMap<git2::Oid, Vec<String>>,
    contained: ContainedIndex<git2::Oid>,
    current_index: AtomicUsize,
    batch_size: usize,
    max_threads: u64,
    repo_path: String,
}

pub struct LibGitLogReadPlanner {
    inner: Arc<LibGitLogReadPlannerInner>,
}

impl LibGitLogReadPlanner {
    pub fn open(
        repo_path: &str,
        params: &GitLogParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        let handle = CachedRepo::open(repo_path)?;
        let repo = handle.repo();

        let commit_oids = walk_commit_oids(repo, params.revision.as_deref(), params.max_count)?;
        let decorations = if schema::needs_refs(column_indices) {
            collect_refs(repo, params.decorate)?
        } else {
            HashMap::new()
        };
        let need_branches = schema::needs_contained_branches(column_indices);
        let need_tags = schema::needs_contained_tags(column_indices);
        let contained = if need_branches || need_tags {
            let wanted: HashSet<git2::Oid> = commit_oids.iter().copied().collect();
            build_contained_index(repo, params.decorate, need_branches, need_tags, &wanted)?
        } else {
            ContainedIndex::empty()
        };

        let (max_threads, batch_size) = compute_parallelism(commit_oids.len());

        Ok(LibGitLogReadPlanner {
            inner: Arc::new(LibGitLogReadPlannerInner {
                commit_oids,
                decorations,
                contained,
                current_index: AtomicUsize::new(0),
                batch_size,
                max_threads,
                repo_path: repo_path.to_string(),
            }),
        })
    }
}

impl GitLogReadPlanner for LibGitLogReadPlanner {
    fn max_threads(&self) -> u64 {
        self.inner.max_threads
    }

    fn new_reader(&self, params: &GitLogParameter) -> Box<dyn GitLogReader> {
        Box::new(LibGitLogReader {
            inner: Arc::clone(&self.inner),
            ignore_all_space: params.ignore_all_space,
            diff_merges: params.diff_merges,
        })
    }
}

struct LibGitLogReader {
    inner: Arc<LibGitLogReadPlannerInner>,
    ignore_all_space: bool,
    diff_merges: DiffMerges,
}

impl GitLogReader for LibGitLogReader {
    fn read(
        &mut self,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        let start_index = self
            .inner
            .current_index
            .fetch_add(self.inner.batch_size, Ordering::Relaxed);

        if start_index >= self.inner.commit_oids.len() {
            return Ok(0);
        }

        let end_index = std::cmp::min(
            start_index + self.inner.batch_size,
            self.inner.commit_oids.len(),
        );

        let handle = CachedRepo::open(&self.inner.repo_path)?;
        let repo = handle.repo();

        let mut writer = VectorInserter::new(output, column_indices);

        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        let oids = &self.inner.commit_oids[start_index..end_index];
        for (batch_idx, oid) in oids.iter().enumerate() {
            let commit = read_commit(
                repo,
                *oid,
                self.ignore_all_space,
                skip_file_changes,
                self.diff_merges,
            )?;
            let refs = self.inner.decorations.get(oid).unwrap_or(&empty_refs);
            let branches: Vec<&str> = self.inner.contained.branches_of(oid).collect();
            let tags: Vec<&str> = self.inner.contained.tags_of(oid).collect();
            writer.push(batch_idx, &oid.to_string(), &commit, refs, &branches, &tags);
        }

        writer.finish();
        Ok(oids.len() as u32)
    }
}

const MAX_LIBGIT_THREADS: usize = 4;

fn compute_parallelism(commit_count: usize) -> (u64, usize) {
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(MAX_LIBGIT_THREADS);
    let max_threads = std::cmp::min(commit_count, cpu_cores) as u64;
    let batch_size = (commit_count / cpu_cores).clamp(1, 2048);
    (max_threads, batch_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

    /// The only non-obvious behaviour in this module: drop installs into a
    /// one-slot thread cache, a matching path is taken back out, and a
    /// mismatching one is evicted only once the new handle drops.
    #[test]
    fn cached_repo_round_trips_through_thread_cache() {
        // --test-threads=1 runs tests on the shared main thread, so start clean.
        CACHED_REPO.with_borrow_mut(|cached| *cached = None);

        {
            let handle = CachedRepo::open(".").unwrap();
            assert!(handle.repo().path().exists());
        }
        CACHED_REPO.with_borrow(|cached| {
            assert!(
                matches!(cached, Some((path, _)) if path == "."),
                "drop installs"
            );
        });

        {
            let _handle = CachedRepo::open(".").unwrap();
            CACHED_REPO.with_borrow(|cached| {
                assert!(cached.is_none(), "a matching path is taken from the cache");
            });
        }

        {
            let _handle = CachedRepo::open("./.git").unwrap();
            CACHED_REPO.with_borrow(|cached| {
                assert!(
                    matches!(cached, Some((path, _)) if path == "."),
                    "a miss leaves the cached entry alone"
                );
            });
        }
        CACHED_REPO.with_borrow(|cached| {
            assert!(
                matches!(cached, Some((path, _)) if path == "./.git"),
                "drop evicts the previous entry"
            );
        });

        CACHED_REPO.with_borrow_mut(|cached| *cached = None);
    }

    #[test]
    fn read_commit_honors_skip_file_changes() {
        let repo = Repository::open(".").unwrap();
        let oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();

        let skipped = read_commit(&repo, oid, false, true, DiffMerges::FirstParent).unwrap();
        assert!(skipped.file_changes.is_empty());

        let kept = read_commit(&repo, oid, false, false, DiffMerges::FirstParent).unwrap();
        assert!(!kept.file_changes.is_empty());
    }

    #[test]
    fn collect_refs_peels_annotated_tag_to_commit() {
        let repo = Repository::open(".").unwrap();
        let tagged_oid = git2::Oid::from_str(TAGGED_COMMIT).unwrap();
        let second_oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();

        for (format, expected) in [
            (DecorateFormat::Short, "v0.1.1"),
            (DecorateFormat::Full, "refs/tags/v0.1.1"),
        ] {
            let refs = collect_refs(&repo, format).unwrap();
            let names = refs
                .get(&tagged_oid)
                .expect("tagged commit should have refs");
            assert!(names.iter().any(|n| n == expected), "{format:?}");
            // A commit that is not a ref tip gets no entry at all.
            assert!(!refs.contains_key(&second_oid), "{format:?}");
        }
    }

    #[test]
    fn contained_index_tags_marks_all_descendant_tags() {
        let repo = Repository::open(".").unwrap();
        let tagged_oid = git2::Oid::from_str(TAGGED_COMMIT).unwrap();
        let wanted: HashSet<git2::Oid> = [tagged_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, false, true, &wanted).unwrap();
        let names: Vec<&str> = index.tags_of(&tagged_oid).collect();
        assert!(
            names.windows(2).all(|w| w[0] <= w[1]),
            "tags must be sorted, got {names:?}"
        );
        // Every release tag descends from v0.1.1's commit. Membership only, so a
        // new release tag does not break this; exclusion is covered exactly by
        // contained_index_is_exact_for_merge_fan_in.
        for tag in ["v0.1.1", "v0.1.2", "v0.2.0", "v0.3.0", "v0.4.0"] {
            assert!(names.contains(&tag), "{tag} missing from {names:?}");
        }
    }

    #[test]
    fn contained_index_tags_full_format() {
        let repo = Repository::open(".").unwrap();
        let tagged_oid = git2::Oid::from_str(TAGGED_COMMIT).unwrap();
        let wanted: HashSet<git2::Oid> = [tagged_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Full, false, true, &wanted).unwrap();
        assert!(index.tags_of(&tagged_oid).any(|n| n == "refs/tags/v0.1.1"));
    }

    #[test]
    fn contained_index_branches_marks_ancestor() {
        let repo = Repository::open(".").unwrap();
        let head = repo.head().unwrap();
        let head_name = head.shorthand().unwrap().to_string();
        let second_oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();
        let wanted: HashSet<git2::Oid> = [second_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &wanted).unwrap();
        assert!(index.branches_of(&second_oid).any(|n| n == head_name));
        // Remote-tracking branches are included too.
        assert!(index.branches_of(&second_oid).any(|n| n == "origin/main"));
    }

    #[test]
    fn contained_index_branches_skips_symbolic_head_alias() {
        let repo = Repository::open(".").unwrap();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &HashSet::new())
                .unwrap();
        assert!(!index.branch_names.iter().any(|n| n == "origin/HEAD"));
    }

    #[test]
    fn contained_index_branches_self_inclusive() {
        let repo = Repository::open(".").unwrap();
        let head = repo.head().unwrap();
        let head_oid = head.peel_to_commit().unwrap().id();
        let head_name = head.shorthand().unwrap().to_string();
        let wanted: HashSet<git2::Oid> = [head_oid].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, false, &wanted).unwrap();
        assert!(index.branches_of(&head_oid).any(|n| n == head_name));
    }

    fn init_fan_in_repo() -> (tempfile::TempDir, [git2::Oid; 4]) {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();

        let a_id = repo.commit(None, &sig, &sig, "A", &tree, &[]).unwrap();
        let a = repo.find_commit(a_id).unwrap();
        let b_id = repo.commit(None, &sig, &sig, "B", &tree, &[&a]).unwrap();
        let b = repo.find_commit(b_id).unwrap();
        let c_id = repo.commit(None, &sig, &sig, "C", &tree, &[&a]).unwrap();
        let c = repo.find_commit(c_id).unwrap();
        let d_id = repo
            .commit(None, &sig, &sig, "D", &tree, &[&b, &c])
            .unwrap();
        let d = repo.find_commit(d_id).unwrap();

        repo.tag_lightweight("left", b.as_object(), false).unwrap();
        repo.tag_lightweight("right", c.as_object(), false).unwrap();
        repo.tag_lightweight("merged", d.as_object(), false)
            .unwrap();

        (dir, [a_id, b_id, c_id, d_id])
    }

    #[test]
    fn contained_index_is_exact_for_merge_fan_in() {
        let (dir, [a_id, b_id, c_id, d_id]) = init_fan_in_repo();
        let repo = Repository::open(dir.path()).unwrap();

        let expected = [
            (d_id, vec!["merged"]),
            (b_id, vec!["left", "merged"]),
            (c_id, vec!["merged", "right"]),
            (a_id, vec!["left", "merged", "right"]),
        ];

        let all: HashSet<git2::Oid> = [a_id, b_id, c_id, d_id].into_iter().collect();
        let index = build_contained_index(&repo, DecorateFormat::Short, false, true, &all).unwrap();
        for (oid, want) in &expected {
            assert_eq!(index.tags_of(oid).collect::<Vec<_>>(), *want);
        }

        // Asking for one commit at a time lets the walk stop early; the answer
        // must not change.
        for (oid, want) in &expected {
            let wanted: HashSet<git2::Oid> = [*oid].into_iter().collect();
            let index =
                build_contained_index(&repo, DecorateFormat::Short, false, true, &wanted).unwrap();
            assert_eq!(index.tags_of(oid).collect::<Vec<_>>(), *want);
        }

        let index =
            build_contained_index(&repo, DecorateFormat::Short, false, true, &HashSet::new())
                .unwrap();
        assert_eq!(index.tag_names, vec!["left", "merged", "right"]);
        assert_eq!(index.tags_of(&d_id).count(), 0);
    }

    const MULTI_WORD_N: usize = 70;

    fn ref_names(prefix: char, step: usize) -> Vec<String> {
        (0..MULTI_WORD_N)
            .step_by(step)
            .map(|i| format!("{prefix}{i:03}"))
            .collect()
    }

    fn init_multi_word_repo() -> (tempfile::TempDir, [git2::Oid; 2]) {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = repo.index().unwrap().write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();

        let root_id = repo.commit(None, &sig, &sig, "root", &tree, &[]).unwrap();
        let root = repo.find_commit(root_id).unwrap();
        let tip_id = repo
            .commit(None, &sig, &sig, "tip", &tree, &[&root])
            .unwrap();
        let tip = repo.find_commit(tip_id).unwrap();

        for i in 0..MULTI_WORD_N {
            let target = if i % 2 == 0 { &tip } else { &root };
            repo.branch(&format!("b{i:03}"), target, false).unwrap();
            repo.tag_lightweight(&format!("t{i:03}"), target.as_object(), false)
                .unwrap();
        }

        (dir, [root_id, tip_id])
    }

    #[test]
    fn contained_index_multi_word_refbits() {
        let (dir, [root_id, tip_id]) = init_multi_word_repo();
        let repo = Repository::open(dir.path()).unwrap();
        let wanted: HashSet<git2::Oid> = [root_id, tip_id].into_iter().collect();
        let index =
            build_contained_index(&repo, DecorateFormat::Short, true, true, &wanted).unwrap();

        assert!(
            index.branch_words > 1,
            "test must exercise RefBits::Words, not Inline"
        );

        // Even-numbered refs point at the tip, odd ones at the root, so the root
        // (an ancestor of both) is contained in all of them.
        for (label, actual, expected) in [
            (
                "tip branches",
                index
                    .branches_of(&tip_id)
                    .map(String::from)
                    .collect::<Vec<_>>(),
                ref_names('b', 2),
            ),
            (
                "tip tags",
                index.tags_of(&tip_id).map(String::from).collect::<Vec<_>>(),
                ref_names('t', 2),
            ),
            (
                "root branches",
                index
                    .branches_of(&root_id)
                    .map(String::from)
                    .collect::<Vec<_>>(),
                ref_names('b', 1),
            ),
            (
                "root tags",
                index
                    .tags_of(&root_id)
                    .map(String::from)
                    .collect::<Vec<_>>(),
                ref_names('t', 1),
            ),
        ] {
            assert_eq!(actual, expected, "{label}");
        }
    }
}
