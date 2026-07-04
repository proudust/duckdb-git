use super::GitBackend;
use crate::types::{CommitData, FileChange};
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, gix::ThreadSafeRepository)>> = const { RefCell::new(None) };
}

pub struct GixBackend {
    repo: Option<gix::Repository>,
    repo_path: String,
}

impl GixBackend {
    pub fn new(repo_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1.to_thread_local()),
            _ => gix::open(repo_path),
        })?;
        Ok(GixBackend {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
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

    fn get_commit(
        &self,
        oid: &str,
        // TODO: gix does not yet support ignore_all_space option for diffs
        _ignore_all_space: bool,
        skip_file_changes: bool,
    ) -> Result<CommitData, Box<dyn std::error::Error>> {
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

        let file_changes = if skip_file_changes {
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
    fn get_refs(&self) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>> {
        let mut refs_map: HashMap<String, Vec<String>> = HashMap::new();

        let platform = self.repo().references()?;
        for reference in platform.all()? {
            let mut reference = reference.map_err(|e| e.to_string())?;
            let name = reference.name().shorten().to_string();
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
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

    #[test]
    fn skip_file_changes_returns_empty() {
        let backend = GixBackend::new(".").unwrap();
        let commit = backend.get_commit(SECOND_COMMIT, false, true).unwrap();
        assert!(commit.file_changes.is_empty());
    }

    #[test]
    fn no_skip_returns_file_changes() {
        let backend = GixBackend::new(".").unwrap();
        let commit = backend.get_commit(SECOND_COMMIT, false, false).unwrap();
        assert!(!commit.file_changes.is_empty());
    }

    #[test]
    fn get_refs_peels_annotated_tag_to_commit() {
        let backend = GixBackend::new(".").unwrap();
        let refs = backend.get_refs().unwrap();
        let names = refs
            .get(TAGGED_COMMIT)
            .expect("tagged commit should have refs");
        assert!(names.iter().any(|n| n == "v0.1.1"));
    }

    #[test]
    fn get_refs_returns_empty_for_commit_without_refs() {
        let backend = GixBackend::new(".").unwrap();
        let refs = backend.get_refs().unwrap();
        assert!(!refs.contains_key(SECOND_COMMIT));
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
