use crate::git::model::{gitlink_numstat, FileChange};
use git2::Repository;

pub(super) fn collect_file_changes(
    repo: &Repository,
    commit: &git2::Commit,
    ignore_all_space: bool,
) -> Result<Vec<FileChange>, git2::Error> {
    let mut file_changes = Vec::new();

    if commit.parent_count() == 0 {
        let tree = commit.tree()?;
        let mut walk_err: Option<git2::Error> = None;
        tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
            if walk_err.is_some() {
                return git2::TreeWalkResult::Abort;
            }
            if entry.kind() == Some(git2::ObjectType::Tree) {
                return git2::TreeWalkResult::Ok;
            }
            let Some(name) = entry.name() else {
                return git2::TreeWalkResult::Ok;
            };
            let oid = entry.id();
            if entry.kind() == Some(git2::ObjectType::Commit) {
                let (add_lines, del_lines) = gitlink_numstat("A");
                file_changes.push(FileChange {
                    path: format!("{}{}", root, name),
                    old_path: None,
                    status: "A",
                    blob_id: oid.to_string(),
                    file_size: None,
                    add_lines: Some(add_lines),
                    del_lines: Some(del_lines),
                });
            } else {
                match repo.find_blob(oid) {
                    Ok(blob) => {
                        let content = blob.content();
                        let (add_lines, del_lines) = if super::xdiff::is_binary_content(content) {
                            (None, None)
                        } else {
                            match std::str::from_utf8(content) {
                                Ok(text) => (Some(text.lines().count() as i32), Some(0)),
                                Err(_) => (None, None),
                            }
                        };
                        file_changes.push(FileChange {
                            path: format!("{}{}", root, name),
                            old_path: None,
                            status: "A",
                            blob_id: oid.to_string(),
                            file_size: Some(blob.size() as i64),
                            add_lines,
                            del_lines,
                        });
                    }
                    Err(e) => {
                        walk_err = Some(e);
                        return git2::TreeWalkResult::Abort;
                    }
                }
            }
            git2::TreeWalkResult::Ok
        })?;
        if let Some(e) = walk_err {
            return Err(e);
        }
        return Ok(file_changes);
    }

    let parent = commit.parent(0)?;
    let parent_tree = parent.tree()?;
    let current_tree = commit.tree()?;

    let mut diff_options = git2::DiffOptions::new();
    diff_options.include_typechange(true);
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
            git2::Delta::Typechange => "T",
            other => {
                return Err(git2::Error::from_str(&format!(
                    "unexpected diff delta status in commit history: {other:?}"
                )));
            }
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
        let is_typechange = delta.status() == git2::Delta::Typechange;

        let (blob_id, file_size, add_lines, del_lines) = if is_gitlink || is_typechange {
            let id = if delta.new_file().path().is_some() {
                delta.new_file().id().to_string()
            } else if delta.old_file().path().is_some() {
                delta.old_file().id().to_string()
            } else {
                "unknown".to_string()
            };
            let file_size = if is_gitlink {
                None
            } else if delta.new_file().path().is_some() {
                Some(delta.new_file().size() as i64)
            } else if delta.old_file().path().is_some() {
                Some(delta.old_file().size() as i64)
            } else {
                None
            };
            let (add, del) = gitlink_numstat(status);
            (id, file_size, Some(add), Some(del))
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

            let line_counts = {
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
            let (add, del) = match line_counts {
                Some((a, d)) => (Some(a), Some(d)),
                None => (None, None),
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
