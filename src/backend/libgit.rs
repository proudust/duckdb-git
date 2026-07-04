use super::GitBackend;
use crate::types::{CommitData, FileChange};
use ::git2::Repository;
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, Repository)>> = const { RefCell::new(None) };
}

pub struct LibGitBackend {
    repo: Option<Repository>,
    repo_path: String,
}

impl LibGitBackend {
    pub fn new(repo_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1),
            _ => Repository::open(repo_path),
        })?;
        Ok(LibGitBackend {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
        })
    }

    fn repo(&self) -> &Repository {
        self.repo.as_ref().unwrap()
    }

    fn get_file_changes(
        &self,
        commit: &::git2::Commit,
        ignore_all_space: bool,
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
            if ignore_all_space {
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

impl GitBackend for LibGitBackend {
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

    fn get_commit(
        &self,
        oid: &str,
        ignore_all_space: bool,
        skip_file_changes: bool,
    ) -> Result<CommitData, Box<dyn std::error::Error>> {
        let oid = ::git2::Oid::from_str(oid)?;
        let commit = self.repo().find_commit(oid)?;
        let author = commit.author();
        let committer = commit.committer();

        let file_changes = if skip_file_changes {
            Vec::new()
        } else {
            self.get_file_changes(&commit, ignore_all_space)?
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

    fn get_refs(&self) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>> {
        let mut refs_map: HashMap<String, Vec<String>> = HashMap::new();
        for reference in self.repo().references()? {
            let reference = reference?;
            let name = reference.shorthand().unwrap_or("").to_string();
            if name.is_empty() {
                continue;
            }
            if let Ok(commit) = reference.peel_to_commit() {
                refs_map
                    .entry(commit.id().to_string())
                    .or_default()
                    .push(name);
            }
        }
        Ok(refs_map)
    }
}

impl Drop for LibGitBackend {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), repo));
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const TAGGED_COMMIT: &str = "295db8704f2b2e12fe71a1f433b8b17906fedf25"; // v0.1.1 (annotated tag)

    #[test]
    fn skip_file_changes_returns_empty() {
        let backend = LibGitBackend::new(".").unwrap();
        let commit = backend.get_commit(SECOND_COMMIT, false, true).unwrap();
        assert!(commit.file_changes.is_empty());
    }

    #[test]
    fn no_skip_returns_file_changes() {
        let backend = LibGitBackend::new(".").unwrap();
        let commit = backend.get_commit(SECOND_COMMIT, false, false).unwrap();
        assert!(!commit.file_changes.is_empty());
    }

    #[test]
    fn get_refs_peels_annotated_tag_to_commit() {
        let backend = LibGitBackend::new(".").unwrap();
        let refs = backend.get_refs().unwrap();
        let names = refs
            .get(TAGGED_COMMIT)
            .expect("tagged commit should have refs");
        assert!(names.iter().any(|n| n == "v0.1.1"));
    }

    #[test]
    fn get_refs_returns_empty_for_commit_without_refs() {
        let backend = LibGitBackend::new(".").unwrap();
        let refs = backend.get_refs().unwrap();
        assert!(!refs.contains_key(SECOND_COMMIT));
    }
}
