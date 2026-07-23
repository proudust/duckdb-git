use crate::git_log::params::{
    unresolved_revision_error, DecorateFormat, DiffMerges, GitLogParameter, RevisionTerm,
};
use crate::git_log::schema;
use crate::git_log::types::{gitlink_numstat, CommitData, FileChange};
use crate::git_log::vector::VectorInserter;
use crate::git_log::{GitLogReadPlanner, GitLogReader};
use duckdb::core::DataChunkHandle;
use git2::Repository;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

thread_local! {
    static CACHED_REPO: RefCell<Option<(String, Repository)>> = const { RefCell::new(None) };
}

struct LibGitRepo {
    repo: Option<Repository>,
    repo_path: String,
}

impl LibGitRepo {
    fn open(repo_path: &str) -> Result<Self, Box<dyn Error>> {
        let repo = CACHED_REPO.with_borrow_mut(|cached| match cached {
            Some((path, _)) if path == repo_path => Ok(cached.take().unwrap().1),
            _ => Repository::open(repo_path),
        })?;
        Ok(LibGitRepo {
            repo: Some(repo),
            repo_path: repo_path.to_string(),
        })
    }

    fn repo(&self) -> &Repository {
        self.repo.as_ref().unwrap()
    }

    fn get_commit_oids(
        &self,
        revision: Option<&[RevisionTerm]>,
        max_count: Option<usize>,
    ) -> Result<Vec<git2::Oid>, Box<dyn Error>> {
        let mut revwalk = self.repo().revwalk()?;

        match revision {
            Some(terms) => {
                for term in terms {
                    let obj = self
                        .repo()
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

    fn get_commit(
        &self,
        oid: git2::Oid,
        ignore_all_space: bool,
        skip_file_changes: bool,
        diff_merges: DiffMerges,
    ) -> Result<CommitData, Box<dyn Error>> {
        let commit = self.repo().find_commit(oid)?;
        let author = commit.author();
        let committer = commit.committer();

        let skip =
            skip_file_changes || (diff_merges == DiffMerges::Off && commit.parent_count() > 1);
        let file_changes = if skip {
            Vec::new()
        } else {
            self.get_file_changes(&commit, ignore_all_space)?
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

    fn get_refs(
        &self,
        format: DecorateFormat,
    ) -> Result<HashMap<git2::Oid, Vec<String>>, Box<dyn Error>> {
        let mut refs_map: HashMap<git2::Oid, Vec<String>> = HashMap::new();
        for reference in self.repo().references()? {
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

    fn get_file_changes(
        &self,
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
                    } else if let Ok(blob) = self.repo().find_blob(oid) {
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

        let mut diff = self.repo().diff_tree_to_tree(
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
                        old_blob = self.repo().find_blob(old_id)?;
                        old_blob.content()
                    };
                    let new_blob;
                    let new_content: &[u8] = if new_id.is_zero() {
                        &[]
                    } else {
                        new_blob = self.repo().find_blob(new_id)?;
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
}

impl Drop for LibGitRepo {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            CACHED_REPO.with_borrow_mut(|cached| {
                *cached = Some((std::mem::take(&mut self.repo_path), repo));
            });
        }
    }
}

struct LibGitLogReadPlannerInner {
    commit_oids: Vec<git2::Oid>,
    decorations: HashMap<git2::Oid, Vec<String>>,
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
        let repo = LibGitRepo::open(repo_path)?;

        let commit_oids = repo.get_commit_oids(params.revision.as_deref(), params.max_count)?;
        let decorations = if schema::needs_refs(column_indices) {
            repo.get_refs(params.decorate)?
        } else {
            HashMap::new()
        };

        let (max_threads, batch_size) = compute_parallelism(commit_oids.len());

        Ok(LibGitLogReadPlanner {
            inner: Arc::new(LibGitLogReadPlannerInner {
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

        let repo = LibGitRepo::open(&self.inner.repo_path)?;

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
            writer.push(batch_idx, &oid.to_string(), &commit, refs);
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
        let repo = LibGitRepo::open(".").unwrap();
        let oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();
        let commit = repo
            .get_commit(oid, false, true, DiffMerges::FirstParent)
            .unwrap();
        assert!(commit.file_changes.is_empty());
    }

    #[test]
    fn no_skip_returns_file_changes() {
        let repo = LibGitRepo::open(".").unwrap();
        let oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();
        let commit = repo
            .get_commit(oid, false, false, DiffMerges::FirstParent)
            .unwrap();
        assert!(!commit.file_changes.is_empty());
    }

    #[test]
    fn get_refs_peels_annotated_tag_to_commit() {
        let repo = LibGitRepo::open(".").unwrap();
        let refs = repo.get_refs(DecorateFormat::Short).unwrap();
        let tagged_oid = git2::Oid::from_str(TAGGED_COMMIT).unwrap();
        let names = refs
            .get(&tagged_oid)
            .expect("tagged commit should have refs");
        assert!(names.iter().any(|n| n == "v0.1.1"));
    }

    #[test]
    fn get_refs_full_returns_full_ref_path() {
        let repo = LibGitRepo::open(".").unwrap();
        let refs = repo.get_refs(DecorateFormat::Full).unwrap();
        let tagged_oid = git2::Oid::from_str(TAGGED_COMMIT).unwrap();
        let names = refs
            .get(&tagged_oid)
            .expect("tagged commit should have refs");
        assert!(names.iter().any(|n| n == "refs/tags/v0.1.1"));
    }

    #[test]
    fn get_refs_returns_empty_for_commit_without_refs() {
        let repo = LibGitRepo::open(".").unwrap();
        let refs = repo.get_refs(DecorateFormat::Short).unwrap();
        let second_oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();
        assert!(!refs.contains_key(&second_oid));
    }
}
