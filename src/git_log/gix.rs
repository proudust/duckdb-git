use crate::git_log::params::{DecorateFormat, DiffMerges, GitLogParameter};
use crate::git_log::schema;
use crate::git_log::types::{CommitData, FileChange};
use crate::git_log::vector::VectorInserter;
use crate::git_log::{GitLogReadPlanner, GitLogReader};
use duckdb::core::DataChunkHandle;
use std::cell::RefCell;
use std::collections::HashMap;
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
            _ => gix::open(repo_path),
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
        revision: Option<&str>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let tip = match revision {
            Some(rev) => self.repo().rev_parse_single(rev)?.detach(),
            None => self.repo().head_id()?.detach(),
        };

        let walk = self.repo().rev_walk([tip]);
        let all = walk.all()?;

        let oids: Result<Vec<String>, Box<dyn Error>> = match max_count {
            Some(count) => all
                .take(count)
                .map(|info| Ok(info?.id.to_string()))
                .collect(),
            None => all.map(|info| Ok(info?.id.to_string())).collect(),
        };

        oids
    }

    fn get_commit(
        &self,
        oid: &str,
        // TODO: gix does not yet support ignore_all_space option for diffs
        _ignore_all_space: bool,
        skip_file_changes: bool,
        diff_merges: DiffMerges,
    ) -> Result<CommitData, Box<dyn Error>> {
        let oid = gix::ObjectId::from_hex(oid.as_bytes())?;
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
    ) -> Result<HashMap<String, Vec<String>>, Box<dyn Error>> {
        let mut refs_map: HashMap<String, Vec<String>> = HashMap::new();

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
                refs_map
                    .entry(commit.id.to_string())
                    .or_default()
                    .push(name);
            }
        }
        Ok(refs_map)
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
                        gix::object::tree::diff::Action::Continue,
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
                let file_size = self
                    .repo()
                    .find_object(id)
                    .map(|o| o.data.len() as i64)
                    .unwrap_or(0);

                let (add_lines, del_lines) = change
                    .diff(&mut resource_cache)
                    .ok()
                    .and_then(|mut platform| platform.line_counts().ok())
                    .flatten()
                    .map(|counts| (counts.insertions as i32, counts.removals as i32))
                    .unwrap_or((0, 0));

                resource_cache.clear_resource_cache_keep_allocation();

                file_changes.push(FileChange {
                    path: location,
                    old_path,
                    status,
                    blob_id: id.to_string(),
                    file_size: Some(file_size),
                    add_lines,
                    del_lines,
                });

                Ok::<_, std::convert::Infallible>(gix::object::tree::diff::Action::Continue)
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
    commit_oids: Vec<String>,
    decorations: HashMap<String, Vec<String>>,
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

        let (max_threads, batch_size) = compute_parallelism(commit_oids.len());

        Ok(GixLogReadPlanner {
            inner: Arc::new(GixLogReadPlannerInner {
                commit_oids,
                decorations,
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
    fn read<'a>(
        &mut self,
        output: &'a mut DataChunkHandle,
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
            let commit =
                repo.get_commit(oid, self.ignore_all_space, skip_file_changes, self.diff_merges)?;
            let refs = self.inner.decorations.get(oid).unwrap_or(&empty_refs);
            writer.push(batch_idx, oid, &commit, refs);
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
    fn skip_file_changes_returns_empty() {
        let repo = GixRepo::open(".").unwrap();
        let commit = repo
            .get_commit(SECOND_COMMIT, false, true, DiffMerges::FirstParent)
            .unwrap();
        assert!(commit.file_changes.is_empty());
    }

    #[test]
    fn no_skip_returns_file_changes() {
        let repo = GixRepo::open(".").unwrap();
        let commit = repo
            .get_commit(SECOND_COMMIT, false, false, DiffMerges::FirstParent)
            .unwrap();
        assert!(!commit.file_changes.is_empty());
    }

    #[test]
    fn get_refs_peels_annotated_tag_to_commit() {
        let repo = GixRepo::open(".").unwrap();
        let refs = repo.get_refs(DecorateFormat::Short).unwrap();
        let names = refs
            .get(TAGGED_COMMIT)
            .expect("tagged commit should have refs");
        assert!(names.iter().any(|n| n == "v0.1.1"));
    }

    #[test]
    fn get_refs_full_returns_full_ref_path() {
        let repo = GixRepo::open(".").unwrap();
        let refs = repo.get_refs(DecorateFormat::Full).unwrap();
        let names = refs
            .get(TAGGED_COMMIT)
            .expect("tagged commit should have refs");
        assert!(names.iter().any(|n| n == "refs/tags/v0.1.1"));
    }

    #[test]
    fn get_refs_returns_empty_for_commit_without_refs() {
        let repo = GixRepo::open(".").unwrap();
        let refs = repo.get_refs(DecorateFormat::Short).unwrap();
        assert!(!refs.contains_key(SECOND_COMMIT));
    }
}
