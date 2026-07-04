use super::GitBackend;
use crate::types::{CommitData, DiffMerges, FileChange};
use std::cell::RefCell;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, gix::ThreadSafeRepository)>> = const { RefCell::new(None) };
}

pub struct GixBackend {
    repo: Option<gix::Repository>,
    repo_path: String,
    // TODO: gix does not yet support ignore_all_space option for diffs
    _ignore_all_space: bool,
    diff_merges: DiffMerges,
}

impl GixBackend {
    pub fn new(
        repo_path: &str,
        ignore_all_space: bool,
        diff_merges: DiffMerges,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => {
                Ok(cached.take().unwrap().1.to_thread_local())
            }
            _ => gix::open(repo_path),
        })?;
        Ok(GixBackend {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
            _ignore_all_space: ignore_all_space,
            diff_merges,
        })
    }

    fn repo(&self) -> &gix::Repository {
        self.repo.as_ref().unwrap()
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
                let status: &'static str = match &change {
                    Change::Addition { .. } => "A",
                    Change::Deletion { .. } => "D",
                    Change::Modification { .. } => "M",
                    Change::Rewrite { copy: true, .. } => "C",
                    Change::Rewrite { copy: false, .. } => "R",
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
                    status,
                    blob_id: id.to_string(),
                    file_size,
                    add_lines,
                    del_lines,
                });

                Ok::<_, std::convert::Infallible>(gix::object::tree::diff::Action::Continue)
            })?;

        Ok(file_changes)
    }
}

impl GitBackend for GixBackend {
    fn get_commit_oids(
        &self,
        revision: Option<&str>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let tip = match revision {
            Some(rev) => self.repo().rev_parse_single(rev)?.detach(),
            None => self.repo().head_id()?.detach(),
        };

        let walk = self.repo().rev_walk([tip]);
        let all = walk.all()?;

        let oids: Result<Vec<String>, Box<dyn std::error::Error>> = match max_count {
            Some(count) => all
                .take(count)
                .map(|info| Ok(info?.id.to_string()))
                .collect(),
            None => all.map(|info| Ok(info?.id.to_string())).collect(),
        };

        oids
    }

    fn get_commit(&self, oid: &str) -> Result<CommitData, Box<dyn std::error::Error>> {
        let oid = gix::ObjectId::from_hex(oid.as_bytes())?;
        let commit = self.repo().find_commit(oid)?;

        let author_name = commit
            .author()
            .map(|a| a.name.to_vec())
            .unwrap_or_default();
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

        let file_changes = if self.diff_merges.should_skip_file_changes() {
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
}

impl Drop for GixBackend {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            let sync_repo = repo.into_sync();
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), sync_repo));
            });
        }
    }
}
