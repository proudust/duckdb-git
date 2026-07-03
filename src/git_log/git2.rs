use super::{DiffMerges, FileChange};
use ::git2::Repository;
use std::cell::RefCell;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, Repository)>> = const { RefCell::new(None) };
}

pub struct Commit<'a> {
    ctx: &'a GitContext,
    commit: ::git2::Commit<'a>,
    author: ::git2::Signature<'a>,
    committer: ::git2::Signature<'a>,
}

impl<'a> Commit<'a> {
    fn new(ctx: &'a GitContext, oid: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let oid = ::git2::Oid::from_str(oid)?;
        let commit = ctx.repo().find_commit(oid)?;
        let author = commit.author().to_owned();
        let committer = commit.committer().to_owned();
        Ok(Commit {
            ctx,
            commit,
            author,
            committer,
        })
    }

    pub fn author_name(&self) -> &[u8] {
        self.author.name_bytes()
    }

    pub fn author_email(&self) -> &[u8] {
        self.author.email_bytes()
    }

    pub fn author_timestamp(&self) -> i64 {
        self.commit.time().seconds()
    }

    pub fn committer_name(&self) -> &[u8] {
        self.committer.name_bytes()
    }

    pub fn committer_email(&self) -> &[u8] {
        self.committer.email_bytes()
    }

    pub fn committer_timestamp(&self) -> i64 {
        self.committer.when().seconds()
    }

    pub fn message(&self) -> &[u8] {
        self.commit.message_bytes()
    }

    pub fn parents(&self) -> Vec<String> {
        (0..self.commit.parent_count())
            .map(|i| self.commit.parent_id(i).unwrap().to_string())
            .collect()
    }

    pub fn file_changes(&self) -> Result<Vec<FileChange>, Box<dyn std::error::Error>> {
        if self.ctx.diff_merges.should_skip_file_changes() {
            return Ok(Vec::new());
        }
        Ok(self.ctx.get_file_changes(&self.commit)?)
    }
}

pub struct GitContext {
    repo: Option<Repository>,
    pub repo_path: String,
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
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1),
            _ => Repository::open(repo_path),
        })?;
        Ok(GitContext {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
            ignore_all_space,
            diff_merges,
        })
    }

    fn repo(&self) -> &Repository {
        self.repo.as_ref().unwrap()
    }

    pub fn get_commit_oids(
        &self,
        revision: Option<&String>,
        max_count: Option<usize>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut revwalk = self.repo().revwalk()?;

        match revision {
            Some(rev) => {
                let obj = self.repo().revparse_single(rev)?;
                revwalk.push(obj.id())?;
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
            commit_oids.push(oid?.to_string());
        }

        Ok(commit_oids)
    }

    pub fn get_commit(&self, oid: &str) -> Result<Commit<'_>, Box<dyn std::error::Error>> {
        Commit::new(self, oid)
    }

    fn get_file_changes(&self, commit: &::git2::Commit) -> Result<Vec<FileChange>, ::git2::Error> {
        let mut file_changes = Vec::new();
        let parent_count = commit.parent_count();

        if parent_count == 0 {
            let tree = commit.tree()?;
            tree.walk(::git2::TreeWalkMode::PreOrder, |root, entry| {
                if entry.kind() == Some(::git2::ObjectType::Tree) {
                    return ::git2::TreeWalkResult::Ok;
                }
                if let Some(name) = entry.name() {
                    let oid = entry.id();
                    let (file_size, add_lines) = if let Ok(blob) = self.repo().find_blob(oid) {
                        let content = std::str::from_utf8(blob.content());
                        let lines = if let Ok(text) = content {
                            text.lines().count() as i32
                        } else {
                            0
                        };
                        (blob.size() as i64, lines)
                    } else {
                        (0, 0)
                    };

                    file_changes.push(FileChange {
                        path: format!("{}{}", root, name),
                        status: "A",
                        blob_id: oid.to_string(),
                        file_size,
                        add_lines,
                        del_lines: 0,
                    });
                }
                ::git2::TreeWalkResult::Ok
            })?;
        } else {
            let parent = commit.parent(0)?;
            let parent_tree = parent.tree()?;
            let current_tree = commit.tree()?;

            let mut diff_options = ::git2::DiffOptions::new();
            if self.ignore_all_space {
                diff_options.ignore_whitespace(true);
            }

            let diff = self.repo().diff_tree_to_tree(
                Some(&parent_tree),
                Some(&current_tree),
                Some(&mut diff_options),
            )?;

            let file_changes = RefCell::new(&mut file_changes);
            diff.foreach(
                &mut |delta, _progress| {
                    let status = match delta.status() {
                        ::git2::Delta::Added => "A",
                        ::git2::Delta::Deleted => "D",
                        ::git2::Delta::Modified => "M",
                        ::git2::Delta::Renamed => "R",
                        ::git2::Delta::Copied => "C",
                        ::git2::Delta::Ignored => "I",
                        ::git2::Delta::Untracked => "?",
                        ::git2::Delta::Typechange => "T",
                        _ => "U",
                    };

                    let file_path = if let Some(new_file) = delta.new_file().path() {
                        new_file.to_string_lossy().to_string()
                    } else if let Some(old_file) = delta.old_file().path() {
                        old_file.to_string_lossy().to_string()
                    } else {
                        "unknown".to_string()
                    };

                    let (blob_id, file_size) = if delta.new_file().path().is_some() {
                        (
                            delta.new_file().id().to_string(),
                            delta.new_file().size() as i64,
                        )
                    } else if delta.old_file().path().is_some() {
                        (
                            delta.old_file().id().to_string(),
                            delta.old_file().size() as i64,
                        )
                    } else {
                        ("unknown".to_string(), 0)
                    };

                    file_changes.borrow_mut().push(FileChange {
                        path: file_path,
                        status,
                        blob_id,
                        file_size,
                        add_lines: 0,
                        del_lines: 0,
                    });

                    true
                },
                None,
                None,
                Some(&mut |_delta, _hunk, line| {
                    let mut fc = file_changes.borrow_mut();
                    if let Some(last) = fc.last_mut() {
                        match line.origin() {
                            '+' => last.add_lines += 1,
                            '-' => last.del_lines += 1,
                            _ => {}
                        }
                    }
                    true
                }),
            )?;
            let _ = file_changes;
        }

        Ok(file_changes)
    }
}

impl Drop for GitContext {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), repo));
            });
        }
    }
}
