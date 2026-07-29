use crate::git_log::ref_index::{ContainedIndex, RefBits};
use crate::git_log::params::{
    unresolved_revision_error, DecorateFormat, DiffMerges, GitLogParameter, RevisionTerm,
};
use crate::git_log::schema;
use crate::git_log::types::{gitlink_numstat, CommitData, FileChange};
use crate::git_log::vector::VectorInserter;
use crate::git_log::{GitLogReadPlanner, GitLogReader};
use duckdb::core::DataChunkHandle;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, gix::ThreadSafeRepository)>> = const { RefCell::new(None) };
}

struct GixRepo {
    repo: Option<gix::Repository>,
    repo_path: String,
}

impl GixRepo {
    fn open(repo_path: &str) -> Result<Self, Box<dyn Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1.to_thread_local()),
            _ => gix::open(repo_path).map_err(|e| -> Box<dyn Error> { Box::new(e) }),
        })?;
        Ok(GixRepo {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
        })
    }

    fn repo(&self) -> &gix::Repository {
        self.repo.as_ref().unwrap()
    }

    fn get_commit_oids(
        &self,
        revision: Option<&[RevisionTerm]>,
        max_count: Option<usize>,
    ) -> Result<Vec<gix::ObjectId>, Box<dyn Error>> {
        let (tips, hidden) = match revision {
            Some(terms) => {
                let mut tips = Vec::new();
                let mut hidden = Vec::new();
                for term in terms {
                    let id = self
                        .repo()
                        .rev_parse_single(term.spec.as_str())
                        .map_err(|_| -> Box<dyn Error> {
                            unresolved_revision_error(&term.origin).into()
                        })?
                        .detach();
                    if term.negate {
                        hidden.push(id);
                    } else {
                        tips.push(id);
                    }
                }
                (tips, hidden)
            }
            None => (vec![self.repo().head_id()?.detach()], Vec::new()),
        };

        let walk = self.repo().rev_walk(tips).with_hidden(hidden);
        let all = walk.all()?;

        let oids: Result<Vec<gix::ObjectId>, Box<dyn Error>> = match max_count {
            Some(count) => all.take(count).map(|info| Ok(info?.id)).collect(),
            None => all.map(|info| Ok(info?.id)).collect(),
        };

        oids
    }

    fn get_commit(
        &self,
        oid: gix::ObjectId,
        // TODO: gix does not yet support ignore_all_space option for diffs
        _ignore_all_space: bool,
        skip_file_changes: bool,
        diff_merges: DiffMerges,
    ) -> Result<CommitData, Box<dyn Error>> {
        let commit = self.repo().find_commit(oid)?;

        let author_name = commit.author().map(|a| a.name.to_vec()).unwrap_or_default();
        let author_email = commit
            .author()
            .map(|a| a.email.to_vec())
            .unwrap_or_default();
        let author_timestamp = commit
            .author()
            .ok()
            .and_then(|a| a.time().ok())
            .map(|t| t.seconds)
            .unwrap_or(0);

        let committer_name = commit
            .committer()
            .map(|a| a.name.to_vec())
            .unwrap_or_default();
        let committer_email = commit
            .committer()
            .map(|a| a.email.to_vec())
            .unwrap_or_default();
        let committer_timestamp = commit
            .committer()
            .ok()
            .and_then(|a| a.time().ok())
            .map(|t| t.seconds)
            .unwrap_or(0);

        let message = commit.message_raw_sloppy().to_vec();
        let parents = commit.parent_ids().map(|id| id.to_string()).collect();

        let skip = skip_file_changes
            || (diff_merges == DiffMerges::Off && commit.parent_ids().count() > 1);
        let file_changes = if skip {
            Vec::new()
        } else {
            self.get_file_changes(&commit)?
        };

        Ok(CommitData {
            author_name,
            author_email,
            author_timestamp,
            committer_name,
            committer_email,
            committer_timestamp,
            message,
            parents,
            file_changes,
        })
    }

    fn get_refs(
        &self,
        format: DecorateFormat,
    ) -> Result<HashMap<gix::ObjectId, Vec<String>>, Box<dyn Error>> {
        let mut refs_map: HashMap<gix::ObjectId, Vec<String>> = HashMap::new();

        let platform = self.repo().references()?;
        for reference in platform.all()? {
            let mut reference = reference.map_err(|e| e.to_string())?;
            let name = match format {
                DecorateFormat::Short => reference.name().shorten().to_string(),
                DecorateFormat::Full => reference.name().as_bstr().to_string(),
            };
            if name.is_empty() {
                continue;
            }
            if let Ok(commit) = reference.peel_to_commit() {
                refs_map.entry(commit.id).or_default().push(name);
            }
        }
        Ok(refs_map)
    }

    fn get_contained(
        &self,
        format: DecorateFormat,
        need_branches: bool,
        need_tags: bool,
        wanted: &HashSet<gix::ObjectId>,
    ) -> Result<ContainedIndex<gix::ObjectId>, Box<dyn Error>> {
        let platform = self.repo().references()?;

        let collect = |iter: gix::reference::iter::Iter<'_, '_>,
                       out: &mut Vec<(gix::ObjectId, String)>|
         -> Result<(), Box<dyn Error>> {
            for reference in iter {
                let mut reference = reference.map_err(|e| e.to_string())?;
                if matches!(reference.inner.target, gix::refs::Target::Symbolic(_)) {
                    continue;
                }
                let name = match format {
                    DecorateFormat::Short => reference.name().shorten().to_string(),
                    DecorateFormat::Full => reference.name().as_bstr().to_string(),
                };
                if name.is_empty() {
                    continue;
                }
                if let Ok(commit) = reference.peel_to_commit() {
                    out.push((commit.id, name));
                }
            }
            Ok(())
        };

        let mut branch_refs = Vec::new();
        if need_branches {
            collect(platform.local_branches()?, &mut branch_refs)?;
            collect(platform.remote_branches()?, &mut branch_refs)?;
        }

        let mut tag_refs = Vec::new();
        if need_tags {
            collect(platform.tags()?, &mut tag_refs)?;
        }

        if branch_refs.is_empty() && tag_refs.is_empty() {
            return Ok(ContainedIndex::empty());
        }

        let mut branch_names: Vec<String> =
            branch_refs.iter().map(|(_, name)| name.clone()).collect();
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

        let mut pending: HashMap<gix::ObjectId, RefBits> = HashMap::new();
        for (tip, name) in &branch_refs {
            let bit = branch_names
                .binary_search(name)
                .expect("name collected above");
            pending
                .entry(*tip)
                .or_insert_with(|| RefBits::new(total_words))
                .set(bit);
        }
        for (tip, name) in &tag_refs {
            let bit =
                branch_words * 64 + tag_names.binary_search(name).expect("name collected above");
            pending
                .entry(*tip)
                .or_insert_with(|| RefBits::new(total_words))
                .set(bit);
        }

        let tips: Vec<gix::ObjectId> = pending.keys().copied().collect();
        let mut parents_of: HashMap<gix::ObjectId, gix::traverse::commit::ParentIds> =
            HashMap::new();
        let mut child_count: HashMap<gix::ObjectId, usize> = HashMap::new();
        for info in self.repo().rev_walk(tips).all()? {
            let info = info?;
            child_count.entry(info.id).or_insert(0);
            for parent in &info.parent_ids {
                *child_count.entry(*parent).or_insert(0) += 1;
            }
            parents_of.insert(info.id, info.parent_ids);
        }

        let mut queue: Vec<gix::ObjectId> = child_count
            .iter()
            .filter(|(_, count)| **count == 0)
            .map(|(id, _)| *id)
            .collect();

        let mut bits: HashMap<gix::ObjectId, RefBits> = HashMap::new();
        while let Some(id) = queue.pop() {
            let cur_bits = pending.remove(&id);
            for parent in parents_of
                .get(&id)
                .map(|p| p.as_slice())
                .unwrap_or_default()
            {
                if let Some(cur_bits) = cur_bits.as_ref() {
                    pending
                        .entry(*parent)
                        .or_insert_with(|| RefBits::new(total_words))
                        .or_assign(cur_bits);
                }
                // A parent becomes ready once every child has propagated into it.
                if let Some(count) = child_count.get_mut(parent) {
                    *count -= 1;
                    if *count == 0 {
                        queue.push(*parent);
                    }
                }
            }
            let Some(cur_bits) = cur_bits else {
                continue;
            };
            if wanted.contains(&id) {
                bits.insert(id, cur_bits);
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

    fn get_file_changes(
        &self,
        commit: &gix::Commit,
    ) -> Result<Vec<FileChange>, Box<dyn std::error::Error>> {
        let mut file_changes = Vec::new();
        let current_tree = commit.tree()?;

        let parent_tree = if commit.parent_ids().count() == 0 {
            self.repo().empty_tree()
        } else {
            let parent_id = commit.parent_ids().next().unwrap().detach();
            self.repo().find_commit(parent_id)?.tree()?
        };

        let mut resource_cache = self.repo().diff_resource_cache_for_tree_diff()?;

        parent_tree
            .changes()?
            .for_each_to_obtain_tree(&current_tree, |change| {
                use gix::object::tree::diff::Change;

                // Directories are reported as their own Addition/Deletion/Modification
                // entries alongside their recursed-into children; skip them so only
                // blob-level changes are returned, matching the git2 backend.
                let entry_mode = match &change {
                    Change::Addition { entry_mode, .. } => *entry_mode,
                    Change::Deletion { entry_mode, .. } => *entry_mode,
                    Change::Modification { entry_mode, .. } => *entry_mode,
                    Change::Rewrite { entry_mode, .. } => *entry_mode,
                };
                if entry_mode.is_tree() {
                    return Ok::<_, std::convert::Infallible>(
                        gix::object::tree::diff::Action::Continue(()),
                    );
                }

                let location = change.location().to_string();
                let (status, old_path): (&'static str, Option<String>) = match &change {
                    Change::Addition { .. } => ("A", None),
                    Change::Deletion { .. } => ("D", None),
                    Change::Modification { .. } => ("M", None),
                    Change::Rewrite {
                        copy: true,
                        source_location,
                        ..
                    } => (
                        "C",
                        Some(String::from_utf8_lossy(source_location).into_owned()),
                    ),
                    Change::Rewrite {
                        copy: false,
                        source_location,
                        ..
                    } => (
                        "R",
                        Some(String::from_utf8_lossy(source_location).into_owned()),
                    ),
                };

                let id = change.id();
                let is_gitlink = entry_mode.is_commit();

                let file_size = if is_gitlink {
                    None
                } else {
                    id.try_header().ok().flatten().map(|h| h.size() as i64)
                };

                let (add_lines, del_lines) = if is_gitlink {
                    gitlink_numstat(status)
                } else {
                    change
                        .diff(&mut resource_cache)
                        .ok()
                        .and_then(|mut platform| platform.line_counts().ok())
                        .flatten()
                        .map(|counts| (counts.insertions as i32, counts.removals as i32))
                        .unwrap_or((0, 0))
                };

                resource_cache.clear_resource_cache_keep_allocation();

                file_changes.push(FileChange {
                    path: location,
                    old_path,
                    status,
                    blob_id: id.to_string(),
                    file_size,
                    add_lines,
                    del_lines,
                });

                Ok::<_, std::convert::Infallible>(gix::object::tree::diff::Action::Continue(()))
            })?;

        Ok(file_changes)
    }
}

impl Drop for GixRepo {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            let sync_repo = repo.into_sync();
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), sync_repo));
            });
        }
    }
}

struct GixLogReadPlannerInner {
    commit_oids: Vec<gix::ObjectId>,
    decorations: HashMap<gix::ObjectId, Vec<String>>,
    contained: ContainedIndex<gix::ObjectId>,
    current_index: AtomicUsize,
    batch_size: usize,
    max_threads: u64,
    repo_path: String,
}

pub struct GixLogReadPlanner {
    inner: Arc<GixLogReadPlannerInner>,
}

impl GixLogReadPlanner {
    pub fn open(
        repo_path: &str,
        params: &GitLogParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        let repo = GixRepo::open(repo_path)?;

        let commit_oids = repo.get_commit_oids(params.revision.as_deref(), params.max_count)?;
        let decorations = if schema::needs_refs(column_indices) {
            repo.get_refs(params.decorate)?
        } else {
            HashMap::new()
        };
        let need_branches = schema::needs_contained_branches(column_indices);
        let need_tags = schema::needs_contained_tags(column_indices);
        let contained = if need_branches || need_tags {
            let wanted: HashSet<gix::ObjectId> = commit_oids.iter().copied().collect();
            repo.get_contained(params.decorate, need_branches, need_tags, &wanted)?
        } else {
            ContainedIndex::empty()
        };

        let (max_threads, batch_size) = compute_parallelism(commit_oids.len());

        Ok(GixLogReadPlanner {
            inner: Arc::new(GixLogReadPlannerInner {
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

impl GitLogReadPlanner for GixLogReadPlanner {
    fn max_threads(&self) -> u64 {
        self.inner.max_threads
    }

    fn new_reader(&self, params: &GitLogParameter) -> Box<dyn GitLogReader> {
        Box::new(GixLogReader {
            inner: Arc::clone(&self.inner),
            ignore_all_space: params.ignore_all_space,
            diff_merges: params.diff_merges,
        })
    }
}

struct GixLogReader {
    inner: Arc<GixLogReadPlannerInner>,
    ignore_all_space: bool,
    diff_merges: DiffMerges,
}

impl GitLogReader for GixLogReader {
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

        let repo = GixRepo::open(&self.inner.repo_path)?;

        let mut writer = VectorInserter::new(output, column_indices);

        let empty_refs: Vec<String> = Vec::new();
        let skip_file_changes = !schema::needs_file_changes(column_indices);
        let oids = &self.inner.commit_oids[start_index..end_index];
        for (batch_idx, oid) in oids.iter().enumerate() {
            let commit = repo.get_commit(
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

fn compute_parallelism(commit_count: usize) -> (u64, usize) {
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let max_threads = std::cmp::min(commit_count, cpu_cores) as u64;
    let batch_size = (commit_count / cpu_cores).clamp(1, 2048);
    (max_threads, batch_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

    #[test]
    fn get_commit_honors_skip_file_changes() {
        let repo = GixRepo::open(".").unwrap();
        let oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();

        let skipped = repo
            .get_commit(oid, false, true, DiffMerges::FirstParent)
            .unwrap();
        assert!(skipped.file_changes.is_empty());

        let kept = repo
            .get_commit(oid, false, false, DiffMerges::FirstParent)
            .unwrap();
        assert!(!kept.file_changes.is_empty());
    }

    #[test]
    fn get_refs_peels_annotated_tag_to_commit() {
        let repo = GixRepo::open(".").unwrap();
        let tagged_oid = gix::ObjectId::from_hex(TAGGED_COMMIT.as_bytes()).unwrap();
        let second_oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();

        for (format, expected) in [
            (DecorateFormat::Short, "v0.1.1"),
            (DecorateFormat::Full, "refs/tags/v0.1.1"),
        ] {
            let refs = repo.get_refs(format).unwrap();
            let names = refs
                .get(&tagged_oid)
                .expect("tagged commit should have refs");
            assert!(names.iter().any(|n| n == expected), "{format:?}");
            // A commit that is not a ref tip gets no entry at all.
            assert!(!refs.contains_key(&second_oid), "{format:?}");
        }
    }

    #[test]
    fn get_contained_tags_marks_all_descendant_tags() {
        let repo = GixRepo::open(".").unwrap();
        let tagged_oid = gix::ObjectId::from_hex(TAGGED_COMMIT.as_bytes()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [tagged_oid].into_iter().collect();
        let index = repo
            .get_contained(DecorateFormat::Short, false, true, &wanted)
            .unwrap();
        let names: Vec<&str> = index.tags_of(&tagged_oid).collect();
        assert!(
            names.windows(2).all(|w| w[0] <= w[1]),
            "tags must be sorted, got {names:?}"
        );
        // Every release tag descends from v0.1.1's commit. Membership only, so a
        // new release tag does not break this; exclusion is covered exactly by
        // get_contained_is_exact_for_merge_fan_in.
        for tag in ["v0.1.1", "v0.1.2", "v0.2.0", "v0.3.0", "v0.4.0"] {
            assert!(names.contains(&tag), "{tag} missing from {names:?}");
        }
    }

    #[test]
    fn get_contained_tags_full_format() {
        let repo = GixRepo::open(".").unwrap();
        let tagged_oid = gix::ObjectId::from_hex(TAGGED_COMMIT.as_bytes()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [tagged_oid].into_iter().collect();
        let index = repo
            .get_contained(DecorateFormat::Full, false, true, &wanted)
            .unwrap();
        assert!(index.tags_of(&tagged_oid).any(|n| n == "refs/tags/v0.1.1"));
    }

    #[test]
    fn get_contained_branches_marks_ancestor() {
        let repo = GixRepo::open(".").unwrap();
        let head_name = repo.repo().head_name().unwrap().unwrap();
        let head_name = head_name.shorten().to_string();
        let second_oid = gix::ObjectId::from_hex(SECOND_COMMIT.as_bytes()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [second_oid].into_iter().collect();
        let index = repo
            .get_contained(DecorateFormat::Short, true, false, &wanted)
            .unwrap();
        assert!(index.branches_of(&second_oid).any(|n| n == head_name));
        // Remote-tracking branches are included too.
        assert!(index.branches_of(&second_oid).any(|n| n == "origin/main"));
    }

    #[test]
    fn get_contained_branches_skips_symbolic_head_alias() {
        let repo = GixRepo::open(".").unwrap();
        let index = repo
            .get_contained(DecorateFormat::Short, true, false, &HashSet::new())
            .unwrap();
        assert!(!index.branch_names.iter().any(|n| n == "origin/HEAD"));
    }

    #[test]
    fn get_contained_branches_self_inclusive() {
        let repo = GixRepo::open(".").unwrap();
        let head_oid = repo.repo().head_id().unwrap().detach();
        let head_name = repo.repo().head_name().unwrap().unwrap();
        let head_name = head_name.shorten().to_string();
        let wanted: HashSet<gix::ObjectId> = [head_oid].into_iter().collect();
        let index = repo
            .get_contained(DecorateFormat::Short, true, false, &wanted)
            .unwrap();
        assert!(index.branches_of(&head_oid).any(|n| n == head_name));
    }

    fn init_fan_in_repo() -> (tempfile::TempDir, [gix::ObjectId; 4]) {
        let dir = tempfile::tempdir().unwrap();
        let repo = gix::init(dir.path()).unwrap();
        let sig = gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(0, 0),
        };
        let tree = repo
            .write_object(gix::objs::Tree::empty())
            .unwrap()
            .detach();

        // Written via write_object rather than commit_as: the latter requires the
        // updated ref to already point at the first parent, which C (branching off A
        // while the ref is at B) would violate.
        let commit = |message: &str, parents: Vec<gix::ObjectId>| {
            repo.write_object(&gix::objs::Commit {
                tree,
                parents: parents.into(),
                author: sig.clone(),
                committer: sig.clone(),
                encoding: None,
                message: message.into(),
                extra_headers: Vec::new(),
            })
            .unwrap()
            .detach()
        };

        let a_id = commit("A", vec![]);
        let b_id = commit("B", vec![a_id]);
        let c_id = commit("C", vec![a_id]);
        let d_id = commit("D", vec![b_id, c_id]);

        use gix::refs::transaction::PreviousValue;
        repo.tag_reference("left", b_id, PreviousValue::Any)
            .unwrap();
        repo.tag_reference("right", c_id, PreviousValue::Any)
            .unwrap();
        repo.tag_reference("merged", d_id, PreviousValue::Any)
            .unwrap();

        (dir, [a_id, b_id, c_id, d_id])
    }

    #[test]
    fn get_contained_is_exact_for_merge_fan_in() {
        let (dir, [a_id, b_id, c_id, d_id]) = init_fan_in_repo();
        let gr = GixRepo::open(dir.path().to_str().unwrap()).unwrap();

        let expected = [
            (d_id, vec!["merged"]),
            (b_id, vec!["left", "merged"]),
            (c_id, vec!["merged", "right"]),
            (a_id, vec!["left", "merged", "right"]),
        ];

        let all: HashSet<gix::ObjectId> = [a_id, b_id, c_id, d_id].into_iter().collect();
        let index = gr
            .get_contained(DecorateFormat::Short, false, true, &all)
            .unwrap();
        for (oid, want) in &expected {
            assert_eq!(index.tags_of(oid).collect::<Vec<_>>(), *want);
        }

        // Asking for one commit at a time lets the walk stop early; the answer
        // must not change.
        for (oid, want) in &expected {
            let wanted: HashSet<gix::ObjectId> = [*oid].into_iter().collect();
            let index = gr
                .get_contained(DecorateFormat::Short, false, true, &wanted)
                .unwrap();
            assert_eq!(index.tags_of(oid).collect::<Vec<_>>(), *want);
        }

        let index = gr
            .get_contained(DecorateFormat::Short, false, true, &HashSet::new())
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

    fn init_multi_word_repo() -> (tempfile::TempDir, [gix::ObjectId; 2]) {
        let dir = tempfile::tempdir().unwrap();
        let mut repo = gix::init(dir.path()).unwrap();
        // Creating refs/heads/* writes a reflog, which needs a committer identity.
        // CI has no global git config, so set one on the test repo itself.
        let mut config = repo.config_snapshot_mut();
        config
            .set_raw_value(gix::config::tree::User::NAME, "Test")
            .unwrap();
        config
            .set_raw_value(gix::config::tree::User::EMAIL, "test@example.com")
            .unwrap();
        drop(config);
        let sig = gix::actor::Signature {
            name: "Test".into(),
            email: "test@example.com".into(),
            time: gix::date::Time::new(0, 0),
        };
        let tree = repo
            .write_object(gix::objs::Tree::empty())
            .unwrap()
            .detach();

        let commit = |message: &str, parents: Vec<gix::ObjectId>| {
            repo.write_object(&gix::objs::Commit {
                tree,
                parents: parents.into(),
                author: sig.clone(),
                committer: sig.clone(),
                encoding: None,
                message: message.into(),
                extra_headers: Vec::new(),
            })
            .unwrap()
            .detach()
        };

        let root_id = commit("root", vec![]);
        let tip_id = commit("tip", vec![root_id]);

        use gix::refs::transaction::PreviousValue;
        for i in 0..MULTI_WORD_N {
            let target = if i % 2 == 0 { tip_id } else { root_id };
            repo.reference(
                format!("refs/heads/b{i:03}"),
                target,
                PreviousValue::Any,
                "test",
            )
            .unwrap();
            repo.tag_reference(format!("t{i:03}"), target, PreviousValue::Any)
                .unwrap();
        }

        (dir, [root_id, tip_id])
    }

    #[test]
    fn get_contained_multi_word_refbits() {
        let (dir, [root_id, tip_id]) = init_multi_word_repo();
        let gr = GixRepo::open(dir.path().to_str().unwrap()).unwrap();
        let wanted: HashSet<gix::ObjectId> = [root_id, tip_id].into_iter().collect();
        let index = gr
            .get_contained(DecorateFormat::Short, true, true, &wanted)
            .unwrap();

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
