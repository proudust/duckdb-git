use git2::Repository;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum DiffMerges {
    Off,
    FirstParent,
}

impl DiffMerges {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "off" => DiffMerges::Off,
            "none" => DiffMerges::Off,
            "first-parent" => DiffMerges::FirstParent,
            _ => DiffMerges::FirstParent, // デフォルト
        }
    }

    pub fn should_skip_file_changes(&self) -> bool {
        matches!(self, DiffMerges::Off)
    }
}

#[derive(Clone)]
pub struct Commit {
    pub commit_id: String,
    pub author: String,
    pub author_email: String,
    pub committer: String,
    pub committer_email: String,
    pub message: String,
    pub author_timestamp: i64,
    pub committer_timestamp: i64,
    pub parents: Vec<String>,
    pub file_changes: Vec<FileChange>,
}

#[derive(Clone)]
pub struct FileChange {
    pub path: String,
    pub status: String,
    pub blob_id: String,
    pub file_size: i64,
    pub add_lines: i32,
    pub del_lines: i32,
}

pub struct GitContext {
    pub repo: Repository,
    pub ignore_all_space: bool,
    pub diff_merges: DiffMerges,
}

impl GitContext {
    pub fn new(
        repo_path: &str,
        ignore_all_space: bool,
        diff_merges: DiffMerges,
    ) -> Result<Self, git2::Error> {
        let repo = Repository::open(repo_path)?;
        Ok(GitContext {
            repo,
            ignore_all_space,
            diff_merges,
        })
    }

    pub fn get_commit_oids(
        &self,
        revision: Option<&String>,
        max_count: Option<usize>,
    ) -> Result<Vec<git2::Oid>, git2::Error> {
        let mut revwalk = self.repo.revwalk()?;

        // リビジョンが指定されている場合はそれを使用、そうでなければHEADを使用
        match revision {
            Some(rev) => {
                // ブランチ名やコミットハッシュを解決
                let obj = self.repo.revparse_single(rev)?;
                revwalk.push(obj.id())?;
            }
            None => {
                revwalk.push_head()?;
            }
        }

        // max_countが指定されていればその値を使用、そうでなければデフォルトで全件を取得
        let revwalk_iter: Box<dyn Iterator<Item = _>> = match max_count {
            Some(count) => Box::new(revwalk.take(count)),
            None => Box::new(revwalk),
        };

        // 全てのコミットOIDを収集
        let mut commit_oids = Vec::new();
        for oid in revwalk_iter {
            commit_oids.push(oid?);
        }

        Ok(commit_oids)
    }

    pub fn get_commit(&self, oid: git2::Oid) -> Result<Commit, git2::Error> {
        let commit = self.repo.find_commit(oid)?;

        // diff_mergesの設定に基づいてファイル変更を取得するかどうか決定
        let file_changes = if self.diff_merges.should_skip_file_changes() {
            Vec::new() // ファイル変更解析をスキップ
        } else {
            self.get_file_changes(&commit)?
        };

        // 親コミットIDを取得
        let parents: Vec<String> = (0..commit.parent_count())
            .map(|i| commit.parent_id(i).unwrap().to_string())
            .collect();

        // コミット情報を事前に取得
        let author_name = commit.author().name().unwrap_or("Unknown").to_string();
        let author_email = commit.author().email().unwrap_or("Unknown").to_string();
        let committer_name = commit.committer().name().unwrap_or("Unknown").to_string();
        let committer_email = commit.committer().email().unwrap_or("Unknown").to_string();
        let message = commit.message().unwrap_or("No message").to_string();
        let author_timestamp = commit.time().seconds();
        let committer_timestamp = commit.committer().when().seconds();

        Ok(Commit {
            commit_id: oid.to_string(),
            author: author_name,
            author_email,
            committer: committer_name,
            committer_email,
            message,
            author_timestamp,
            committer_timestamp,
            parents,
            file_changes,
        })
    }

    pub fn get_file_changes(&self, commit: &git2::Commit) -> Result<Vec<FileChange>, git2::Error> {
        let mut file_changes = Vec::new();

        // 各コミットについて変更されたファイルを取得
        let parent_count = commit.parent_count();

        if parent_count == 0 {
            // 初回コミットの場合、全てのファイルを新規追加として扱う
            let tree = commit.tree()?;
            tree.walk(git2::TreeWalkMode::PreOrder, |_root, entry| {
                if let Some(name) = entry.name() {
                    let oid = entry.id();
                    let (file_size, add_lines) = if let Ok(blob) = self.repo.find_blob(oid) {
                        let content = std::str::from_utf8(blob.content());
                        let lines = if let Ok(text) = content {
                            text.lines().count() as i32
                        } else {
                            0 // バイナリファイルの場合は0行とする
                        };
                        (blob.size() as i64, lines)
                    } else {
                        (0, 0) // ディレクトリや取得できない場合は0
                    };

                    file_changes.push(FileChange {
                        path: name.to_string(),
                        status: "A".to_string(), // Added
                        blob_id: oid.to_string(),
                        file_size,
                        add_lines,
                        del_lines: 0, // 初回コミットなので削除行は0
                    });
                }
                git2::TreeWalkResult::Ok
            })?;
        } else {
            // 親コミットとの差分を取得
            let parent = commit.parent(0)?;
            let parent_tree = parent.tree()?;
            let current_tree = commit.tree()?;

            let mut diff_options = git2::DiffOptions::new();
            if self.ignore_all_space {
                diff_options.ignore_whitespace(true);
            }

            let diff = self.repo.diff_tree_to_tree(
                Some(&parent_tree),
                Some(&current_tree),
                Some(&mut diff_options),
            )?;

            // 各ファイルの統計情報を格納するマップ
            let mut file_stats = HashMap::new();

            // まず統計情報を収集
            diff.print(git2::DiffFormat::Patch, |delta, _hunk, line| {
                let file_path = if let Some(new_file) = delta.new_file().path() {
                    new_file.to_string_lossy().to_string()
                } else if let Some(old_file) = delta.old_file().path() {
                    old_file.to_string_lossy().to_string()
                } else {
                    "unknown".to_string()
                };

                let entry = file_stats.entry(file_path).or_insert((0i32, 0i32));

                match line.origin() {
                    '+' => entry.0 += 1, // add_lines
                    '-' => entry.1 += 1, // del_lines
                    _ => {}
                }
                true
            })?;

            diff.foreach(
                &mut |delta, _progress| {
                    let status = match delta.status() {
                        git2::Delta::Added => "A",
                        git2::Delta::Deleted => "D",
                        git2::Delta::Modified => "M",
                        git2::Delta::Renamed => "R",
                        git2::Delta::Copied => "C",
                        git2::Delta::Ignored => "I",
                        git2::Delta::Untracked => "?",
                        git2::Delta::Typechange => "T",
                        _ => "U", // Unknown/Unmodified
                    };

                    let file_path = if let Some(new_file) = delta.new_file().path() {
                        new_file.to_string_lossy().to_string()
                    } else if let Some(old_file) = delta.old_file().path() {
                        old_file.to_string_lossy().to_string()
                    } else {
                        "unknown".to_string()
                    };

                    // blob_idとファイルサイズの取得
                    let (blob_id, file_size) = if delta.new_file().path().is_some() {
                        let new_oid = delta.new_file().id();
                        let size = if let Ok(blob) = self.repo.find_blob(new_oid) {
                            blob.size() as i64
                        } else {
                            0 // ディレクトリや削除されたファイルは0
                        };
                        (new_oid.to_string(), size)
                    } else if delta.old_file().path().is_some() {
                        let old_oid = delta.old_file().id();
                        let size = if let Ok(blob) = self.repo.find_blob(old_oid) {
                            blob.size() as i64
                        } else {
                            0
                        };
                        (old_oid.to_string(), size)
                    } else {
                        ("unknown".to_string(), 0)
                    };

                    // 統計情報から行数を取得
                    let (add_lines, del_lines) = file_stats.get(&file_path).unwrap_or(&(0, 0));

                    file_changes.push(FileChange {
                        path: file_path,
                        status: status.to_string(),
                        blob_id,
                        file_size,
                        add_lines: *add_lines,
                        del_lines: *del_lines,
                    });

                    true
                },
                None,
                None,
                None,
            )?;
        }

        Ok(file_changes)
    }
}
