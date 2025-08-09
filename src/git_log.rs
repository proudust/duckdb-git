use crate::types::{DecorateMode, FileStatus, GitCommit, GitFileChange, GitLogParameter};
use git2::Repository;
use internment::Arena;
use std::collections::HashMap;

pub struct LibGitCommit<'a> {
    ctx: &'a LibGitContext,
    commit: git2::Commit<'a>,
    author: git2::Signature<'a>,
    committer: git2::Signature<'a>,
}

impl<'a> LibGitCommit<'a> {
    pub fn new(ctx: &'a LibGitContext, oid: git2::Oid) -> Result<Self, git2::Error> {
        let commit = ctx.repo.find_commit(oid)?;
        let author = commit.author().to_owned();
        let committer = commit.committer().to_owned();
        Ok(LibGitCommit {
            ctx,
            commit,
            author,
            committer,
        })
    }
}

impl<'a> GitCommit for LibGitCommit<'a> {
    type FileChange = LibGitFileChange;

    fn commit_id(&self) -> &[u8] {
        self.ctx
            .arena
            .intern_ref(self.commit.id().as_bytes())
            .into_ref()
    }

    fn author_name(&self) -> &[u8] {
        self.author.name_bytes()
    }

    fn author_email(&self) -> &[u8] {
        self.author.email_bytes()
    }

    fn author_timestamp(&self) -> i64 {
        self.commit.time().seconds()
    }

    fn committer_name(&self) -> &[u8] {
        self.committer.name_bytes()
    }

    fn committer_email(&self) -> &[u8] {
        self.committer.email_bytes()
    }

    fn committer_timestamp(&self) -> i64 {
        self.committer.when().seconds()
    }

    fn message(&self) -> &[u8] {
        self.commit.message_bytes()
    }

    fn parents(&self) -> Vec<&[u8]> {
        (0..self.commit.parent_count())
            .map(|i| {
                self.ctx
                    .arena
                    .intern_ref(self.commit.parent_id(i).unwrap().as_bytes())
                    .into_ref()
            })
            .collect()
    }

    fn file_changes(&self) -> Vec<LibGitFileChange> {
        if !self.ctx.status {
            return Vec::new();
        }

        // コミットを直接参照
        let commit = &self.commit;
        self.ctx.get_file_changes(commit).unwrap()
    }

    fn refs(&self) -> Vec<&[u8]> {
        if self.ctx.decorate == DecorateMode::No {
            return Vec::new();
        }

        let commit_id = self.commit.id();
        let arena = &self.ctx.arena;
        let mut refs = Vec::new();

        // ブランチの取得
        let branches = self.ctx.repo.branches(None).unwrap();
        for branch_result in branches {
            let (branch, branch_type) = branch_result.unwrap();
            if let Some(target_oid) = branch.get().target() {
                if target_oid == commit_id {
                    if let Some(name) = branch.name().unwrap() {
                        let ref_name = match &self.ctx.decorate {
                            DecorateMode::Short => arena.intern_ref(name.as_bytes()),
                            DecorateMode::Full => {
                                // full形式: 完全なref名
                                match branch_type {
                                    git2::BranchType::Local => {
                                        arena.intern_ref(format!("refs/heads/{}", name).as_bytes())
                                    }
                                    git2::BranchType::Remote => arena
                                        .intern_ref(format!("refs/remotes/{}", name).as_bytes()),
                                }
                            }
                            DecorateMode::No => unreachable!(),
                        };
                        refs.push(ref_name.into_ref());
                    }
                }
            }
        }

        // タグの取得
        self.ctx
            .repo
            .tag_foreach(|tag_oid, name| {
                // タグが指すコミットを解決
                if let Ok(tag_obj) = self.ctx.repo.find_object(tag_oid, None) {
                    let target_oid = match tag_obj.kind() {
                        Some(git2::ObjectType::Tag) => {
                            // annotated tag の場合、target を取得
                            if let Some(tag) = tag_obj.as_tag() {
                                tag.target_id()
                            } else {
                                tag_oid
                            }
                        }
                        _ => tag_oid, // lightweight tag の場合
                    };

                    if target_oid == commit_id {
                        if let Ok(tag_name) = std::str::from_utf8(name) {
                            let ref_name = match &self.ctx.decorate {
                                DecorateMode::Short => {
                                    // short形式: refs/tags/v1.0.0 -> v1.0.0
                                    tag_name
                                        .strip_prefix("refs/tags/")
                                        .unwrap_or(tag_name)
                                        .as_bytes()
                                }
                                DecorateMode::Full => {
                                    // full形式: 完全なref名
                                    tag_name.as_bytes()
                                }
                                DecorateMode::No => unreachable!(),
                            };
                            refs.push(arena.intern_ref(ref_name).into_ref());
                        }
                    }
                }
                true
            })
            .unwrap();

        refs
    }
}

#[derive(Clone)]
pub struct LibGitFileChange {
    path: String,
    status: FileStatus,
    blob_id: String,
    file_size: i64,
    add_lines: i32,
    del_lines: i32,
}

impl GitFileChange for LibGitFileChange {
    fn blob_id(&self) -> &[u8] {
        self.blob_id.as_bytes()
    }
    fn path(&self) -> &[u8] {
        self.path.as_bytes()
    }
    fn status(&self) -> FileStatus {
        self.status.clone()
    }
    fn add_lines(&self) -> i32 {
        self.add_lines
    }
    fn del_lines(&self) -> i32 {
        self.del_lines
    }
    fn file_size(&self) -> i64 {
        self.file_size
    }
}

pub struct LibGitContext {
    arena: Arena<[u8]>,
    repo: Repository,
    ignore_all_space: bool,
    status: bool,
    decorate: DecorateMode,
}

impl LibGitContext {
    pub fn new(parameters: &GitLogParameter) -> Result<Self, git2::Error> {
        let arena = Arena::new();
        let repo = Repository::open(&parameters.repo_path)?;
        Ok(LibGitContext {
            arena,
            repo,
            ignore_all_space: parameters.ignore_all_space,
            status: parameters.stat || parameters.name_only || parameters.name_status,
            decorate: parameters.decorate.clone(),
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

    pub fn get_commit(&'_ self, oid: git2::Oid) -> Result<LibGitCommit<'_>, git2::Error> {
        LibGitCommit::new(self, oid)
    }

    pub fn get_file_changes(
        &self,
        commit: &git2::Commit,
    ) -> Result<Vec<LibGitFileChange>, git2::Error> {
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

                    file_changes.push(LibGitFileChange {
                        path: name.to_string(),
                        status: FileStatus::Added,
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
                    let status = FileStatus::from_delta(delta.status());

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

                    file_changes.push(LibGitFileChange {
                        path: file_path,
                        status,
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
