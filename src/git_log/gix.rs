use super::{DiffMerges, FileChange};
use std::cell::RefCell;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, gix::ThreadSafeRepository)>> = const { RefCell::new(None) };
}

pub struct Commit<'a> {
    ctx: &'a GitContext,
    commit: gix::Commit<'a>,
}

impl<'a> Commit<'a> {
    fn new(ctx: &'a GitContext, oid: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let oid = gix::ObjectId::from_hex(oid.as_bytes())?;
        let commit = ctx.repo().find_commit(oid)?;
        Ok(Commit { ctx, commit })
    }

    pub fn author_name(&self) -> &[u8] {
        self.commit.author().map(|a| a.name.as_ref()).unwrap_or(b"")
    }

    pub fn author_email(&self) -> &[u8] {
        self.commit
            .author()
            .map(|a| a.email.as_ref())
            .unwrap_or(b"")
    }

    pub fn author_timestamp(&self) -> i64 {
        self.commit
            .author()
            .ok()
            .and_then(|a| a.time().ok())
            .map(|t| t.seconds)
            .unwrap_or(0)
    }

    pub fn committer_name(&self) -> &[u8] {
        self.commit
            .committer()
            .map(|a| a.name.as_ref())
            .unwrap_or(b"")
    }

    pub fn committer_email(&self) -> &[u8] {
        self.commit
            .committer()
            .map(|a| a.email.as_ref())
            .unwrap_or(b"")
    }

    pub fn committer_timestamp(&self) -> i64 {
        self.commit
            .committer()
            .ok()
            .and_then(|a| a.time().ok())
            .map(|t| t.seconds)
            .unwrap_or(0)
    }

    pub fn message(&self) -> &[u8] {
        self.commit.message_raw_sloppy().as_ref()
    }

    pub fn parents(&self) -> Vec<String> {
        self.commit.parent_ids().map(|id| id.to_string()).collect()
    }

    pub fn file_changes(&self) -> Result<Vec<FileChange>, Box<dyn std::error::Error>> {
        if self.ctx.diff_merges.should_skip_file_changes() {
            return Ok(Vec::new());
        }
        self.ctx.get_file_changes(&self.commit)
    }
}

pub struct GitContext {
    repo: Option<gix::Repository>,
    pub repo_path: String,
    // TODO: gix does not yet support ignore_all_space option for diffs
    pub ignore_all_space: bool,
    pub diff_merges: DiffMerges,
}

impl GitContext {
    pub fn new(
        repo_path: &str,
        ignore_all_space: bool,
        diff_merges: DiffMerges,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1.to_thread_local()),
            _ => gix::open(repo_path),
        })?;
        Ok(GitContext {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
            ignore_all_space,
            diff_merges,
        })
    }

    fn repo(&self) -> &gix::Repository {
        self.repo.as_ref().unwrap()
    }

    pub fn get_commit_oids(
        &self,
        revision: Option<&String>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let tip = match revision {
            Some(rev) => self.repo().rev_parse_single(rev.as_str())?.detach(),
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

    pub fn get_commit(&self, oid: &str) -> Result<Commit<'_>, Box<dyn std::error::Error>> {
        Commit::new(self, oid)
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
                let status = match &change {
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

impl Drop for GitContext {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            let sync_repo = repo.into_sync();
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), sync_repo));
            });
        }
    }
}
