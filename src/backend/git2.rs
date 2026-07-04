use super::GitBackend;
use crate::types::{CommitData, DiffMerges, FileChange};
use ::git2::Repository;
use std::cell::RefCell;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, Repository)>> = const { RefCell::new(None) };
}

pub struct Git2Backend {
    repo: Option<Repository>,
    repo_path: String,
    ignore_all_space: bool,
    diff_merges: DiffMerges,
}

impl Git2Backend {
    pub fn new(
        repo_path: &str,
        ignore_all_space: bool,
        diff_merges: DiffMerges,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1),
            _ => Repository::open(repo_path),
        })?;
        Ok(Git2Backend {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
            ignore_all_space,
            diff_merges,
        })
    }

    fn repo(&self) -> &Repository {
        self.repo.as_ref().unwrap()
    }

    fn get_file_changes(
        &self,
        commit: &::git2::Commit,
    ) -> Result<Vec<FileChange>, ::git2::Error> {
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

impl GitBackend for Git2Backend {
    fn get_commit_oids(
        &self,
        revision: Option<&str>,
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

    fn get_commit(&self, oid: &str) -> Result<CommitData, Box<dyn std::error::Error>> {
        let oid = ::git2::Oid::from_str(oid)?;
        let commit = self.repo().find_commit(oid)?;
        let author = commit.author();
        let committer = commit.committer();

        let file_changes = if self.diff_merges.should_skip_file_changes() {
            Vec::new()
        } else {
            self.get_file_changes(&commit)?
        };

        Ok(CommitData {
            author_name: author.name_bytes().to_vec(),
            author_email: author.email_bytes().to_vec(),
            author_timestamp: commit.time().seconds(),
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
}

impl Drop for Git2Backend {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), repo));
            });
        }
    }
}
