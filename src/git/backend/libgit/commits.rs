use super::blob_ring::BlobRing;
use super::diff::{emit_file_changes, emit_file_changes_trees};
use crate::git::date_walk::{oid_bytes_from_slice, CommitDateWalk, DateWalkCallbacks, OidBytes};
use crate::git::ident::{commit_header, parse_ident};
use crate::git::meta_proj::MetaProjection;
use crate::git::options::DiffMerges;
use crate::git::revision::{unresolved_revision_error, RevisionTerm};
use crate::git::sink::{oid_hex, CommitSink};
use git2::{Oid, Repository};
use std::collections::{HashMap, HashSet};
use std::error::Error;

fn oid_to_bytes(oid: Oid) -> OidBytes {
    oid_bytes_from_slice(oid.as_bytes()).expect("git2 oid is 20 bytes")
}

fn bytes_to_oid(bytes: OidBytes) -> Oid {
    Oid::from_bytes(&bytes).expect("20-byte sha1")
}

/// Collect all_refs tip OIDs in `git log --all` seed order: raw refname bytes, then HEAD.
///
/// Does not touch a revwalk; callers that need hide/interesting push these tips themselves.
fn collect_all_tips(repo: &Repository) -> Result<Vec<Oid>, Box<dyn Error>> {
    // Sort key is raw bytes (git `for_each_ref`), not lossy UTF-8, so libgit/gix agree
    // even when a ref name is not valid UTF-8.
    let mut named: Vec<(Vec<u8>, Oid)> = Vec::new();
    for reference in repo.references()? {
        let reference = reference?;
        let name = reference.name_bytes().to_vec();
        if !crate::git::all_refs::is_log_all_ref_bytes(&name) {
            continue;
        }
        // Non-commit tips (and unpeeled failures): skip. Symbolic refs that peel
        // to a commit (e.g. refs/remotes/origin/HEAD) are included, like git.
        if let Ok(commit) = reference.peel_to_commit() {
            named.push((name, commit.id()));
        }
    }
    named.sort_by(|a, b| a.0.cmp(&b.0));

    let mut tips: Vec<Oid> = named.into_iter().map(|(_, id)| id).collect();

    // Unborn HEAD: skip (like `git log --all`). Duplicate of a named ref: skip via caller queued set.
    if let Ok(head) = repo.head() {
        if let Ok(commit) = head.peel_to_commit() {
            tips.push(commit.id());
        }
    }
    Ok(tips)
}

fn resolve_revision_tips(
    repo: &Repository,
    revision: Option<&[RevisionTerm]>,
    all_refs: bool,
) -> Result<(Vec<Oid>, Vec<Oid>), Box<dyn Error>> {
    let mut tips: Vec<Oid> = Vec::new();
    let mut hidden: Vec<Oid> = Vec::new();

    if all_refs {
        tips = collect_all_tips(repo)?;
    } else if revision.is_none() {
        tips.push(repo.head()?.peel_to_commit()?.id());
    }

    if let Some(terms) = revision {
        for term in terms {
            let obj = repo
                .revparse_single(&term.spec)
                .map_err(|_| -> Box<dyn Error> {
                    unresolved_revision_error(&term.origin).into()
                })?;
            // Peel annotated tags (and anything else) to a commit, like `git log <rev>`.
            let id = obj
                .peel_to_commit()
                .map(|c| c.id())
                .map_err(|e| -> Box<dyn Error> {
                    format!(
                        "revision '{}' does not resolve to a commit: {e}",
                        term.origin
                    )
                    .into()
                })?;
            if term.negate {
                hidden.push(id);
            } else {
                tips.push(id);
            }
        }
    }

    Ok((tips, hidden))
}

/// Inputs for a date-ordered commit walk (tips + optional hide filter).
#[derive(Clone)]
pub(crate) struct WalkPrepared {
    pub tips: Vec<OidBytes>,
    pub interesting: Option<HashSet<OidBytes>>,
    pub first_parent: bool,
    pub max_count: Option<usize>,
}

pub(crate) fn prepare_walk(
    repo: &Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
    first_parent: bool,
    all_refs: bool,
) -> Result<WalkPrepared, Box<dyn Error>> {
    let has_hide = revision.is_some_and(|t| t.iter().any(|x| x.negate));
    let (tips, hidden) = resolve_revision_tips(repo, revision, all_refs)?;

    let tip_bytes: Vec<OidBytes> = tips.into_iter().map(oid_to_bytes).collect();

    let interesting = if has_hide {
        let mut revwalk = repo.revwalk()?;
        for id in tip_bytes.iter().copied().map(bytes_to_oid) {
            revwalk.push(id)?;
        }
        for id in hidden {
            revwalk.hide(id)?;
        }
        if first_parent {
            revwalk.simplify_first_parent()?;
        }
        Some(
            revwalk
                .map(|oid| oid.map(oid_to_bytes))
                .collect::<Result<HashSet<_>, _>>()?,
        )
    } else {
        drop(hidden);
        None
    };

    Ok(WalkPrepared {
        tips: tip_bytes,
        interesting,
        first_parent,
        max_count,
    })
}

pub(crate) struct LibgitWalkCallbacks<'a> {
    repo: &'a Repository,
    first_parent: bool,
    /// Which emit fields to retain; walk always needs committer seconds + parents.
    proj: MetaProjection,
    /// Inline path: keep emit payload from the single inspect at enqueue.
    emit_cache: Option<&'a mut HashMap<OidBytes, InspectedCommit>>,
}

/// Metadata captured during walk inspect so emit can skip a second `find_commit`.
pub(crate) struct InspectedCommit {
    pub author_name: Vec<u8>,
    pub author_email: Vec<u8>,
    pub author_seconds: i64,
    pub committer_name: Vec<u8>,
    pub committer_email: Vec<u8>,
    pub committer_seconds: i64,
    pub message: Vec<u8>,
    pub parent_oids: Vec<Oid>,
}

/// Prefetch ring payload: inspect stash moved to the ring at yield time.
pub(crate) struct PrefetchItem {
    pub oid: OidBytes,
    pub meta: InspectedCommit,
    pub tree_id: OidBytes,
    /// Root only for trees API: `None`. Merge-skipped commits also leave this
/// unset but must not call `emit_file_changes_trees`.
    pub parent_tree_id: Option<OidBytes>,
    /// Real parent count (projection-independent) for `DiffMerges::Off` merge skip.
    pub parent_count: u32,
}

struct PrefetchWalkCallbacks<'a> {
    repo: &'a Repository,
    first_parent: bool,
    proj: MetaProjection,
    needs_file_changes: bool,
    diff_merges: DiffMerges,
    stash: &'a mut HashMap<OidBytes, PrefetchItem>,
}

fn build_inspected_meta(
    commit: &git2::Commit<'_>,
    header: &[u8],
    committer: crate::git::ident::ParsedIdent<'_>,
    proj: MetaProjection,
    parent_oids: Vec<Oid>,
) -> Result<InspectedCommit, Box<dyn Error>> {
    let (author_name, author_email, author_seconds) = if proj.author {
        let author = parse_ident(header, b"author")?;
        (
            author.name.to_vec(),
            author.email.to_vec(),
            author.seconds,
        )
    } else {
        (Vec::new(), Vec::new(), 0)
    };
    Ok(InspectedCommit {
        author_name,
        author_email,
        author_seconds,
        committer_name: if proj.committer {
            committer.name.to_vec()
        } else {
            Vec::new()
        },
        committer_email: if proj.committer {
            committer.email.to_vec()
        } else {
            Vec::new()
        },
        committer_seconds: committer.seconds,
        message: if proj.message {
            commit.message_raw_bytes().to_vec()
        } else {
            Vec::new()
        },
        parent_oids,
    })
}

impl DateWalkCallbacks for PrefetchWalkCallbacks<'_> {
    fn inspect(&mut self, id: OidBytes) -> Result<(i64, Vec<OidBytes>), Box<dyn Error>> {
        #[cfg(feature = "git-log-stats")]
        crate::git::diag::record_walker_find_commit();
        let commit = self.repo.find_commit(bytes_to_oid(id))?;
        let header = commit_header(commit.raw_header_bytes());
        let committer = parse_ident(header, b"committer")?;
        let seconds = committer.seconds;
        let parent_count = commit.parent_count();
        let walk_n = if self.first_parent {
            parent_count.min(1)
        } else {
            parent_count
        };
        let emit_n = if self.proj.parents {
            parent_count
        } else {
            0
        };
        let mut parent_bytes = Vec::with_capacity(walk_n);
        let mut parent_oids = Vec::with_capacity(emit_n);
        for i in 0..walk_n.max(emit_n) {
            let pid = commit.parent_id(i)?;
            if i < walk_n {
                parent_bytes.push(oid_to_bytes(pid));
            }
            if i < emit_n {
                parent_oids.push(pid);
            }
        }

        let merge_skip =
            self.needs_file_changes && self.diff_merges == DiffMerges::Off && parent_count > 1;
        let need_parent_tree = self.needs_file_changes && parent_count >= 1 && !merge_skip;
        let parent_tree_id = if need_parent_tree {
            let pid = commit.parent_id(0)?;
            // tree_id() only — do not inflate the tree object.
            let tid = self.repo.find_commit(pid)?.tree_id();
            Some(oid_to_bytes(tid))
        } else {
            None
        };

        let meta = build_inspected_meta(&commit, header, committer, self.proj, parent_oids)?;
        self.stash.insert(
            id,
            PrefetchItem {
                oid: id,
                meta,
                tree_id: oid_to_bytes(commit.tree_id()),
                parent_tree_id,
                parent_count: parent_count as u32,
            },
        );
        Ok((seconds, parent_bytes))
    }
}

impl DateWalkCallbacks for LibgitWalkCallbacks<'_> {
    fn inspect(&mut self, id: OidBytes) -> Result<(i64, Vec<OidBytes>), Box<dyn Error>> {
        #[cfg(feature = "git-log-stats")]
        crate::git::diag::record_walker_find_commit();
        let commit = self.repo.find_commit(bytes_to_oid(id))?;
        let header = commit_header(commit.raw_header_bytes());
        let committer = parse_ident(header, b"committer")?;
        let parent_count = commit.parent_count();
        let walk_n = if self.first_parent {
            parent_count.min(1)
        } else {
            parent_count
        };
        // Emit lists every parent; first_parent only limits which edges the walk follows.
        let emit_n = if self.proj.parents {
            parent_count
        } else {
            0
        };
        let mut parent_bytes = Vec::with_capacity(walk_n);
        let mut parent_oids = Vec::with_capacity(emit_n);
        for i in 0..walk_n.max(emit_n) {
            let pid = commit.parent_id(i)?;
            if i < walk_n {
                parent_bytes.push(oid_to_bytes(pid));
            }
            if i < emit_n {
                parent_oids.push(pid);
            }
        }
        let seconds = committer.seconds;
        if let Some(cache) = self.emit_cache.as_mut() {
            let (author_name, author_email, author_seconds) = if self.proj.author {
                let author = parse_ident(header, b"author")?;
                (
                    author.name.to_vec(),
                    author.email.to_vec(),
                    author.seconds,
                )
            } else {
                (Vec::new(), Vec::new(), 0)
            };
            cache.insert(
                id,
                InspectedCommit {
                    author_name,
                    author_email,
                    author_seconds,
                    committer_name: if self.proj.committer {
                        committer.name.to_vec()
                    } else {
                        Vec::new()
                    },
                    committer_email: if self.proj.committer {
                        committer.email.to_vec()
                    } else {
                        Vec::new()
                    },
                    committer_seconds: committer.seconds,
                    message: if self.proj.message {
                        commit.message_raw_bytes().to_vec()
                    } else {
                        Vec::new()
                    },
                    parent_oids,
                },
            );
        }
        Ok((seconds, parent_bytes))
    }
}

pub(crate) fn emit_inspected_commit(
    oid: Oid,
    meta: &InspectedCommit,
    sink: &mut impl CommitSink,
) {
    let hex = oid_hex(oid.as_bytes());
    sink.commit_id(&hex);
    sink.author(&meta.author_name, &meta.author_email, meta.author_seconds);
    sink.committer(
        &meta.committer_name,
        &meta.committer_email,
        meta.committer_seconds,
    );
    sink.message(&meta.message);
    sink.begin_parents(meta.parent_oids.len());
    for parent in &meta.parent_oids {
        let parent_hex = oid_hex(parent.as_bytes());
        sink.parent(&parent_hex);
    }
}

/// Walk in date order; stash PrefetchItem at inspect; push at yield (same order as OID walk).
pub(crate) fn run_prefetch_commit_walk(
    repo: &Repository,
    prep: WalkPrepared,
    proj: MetaProjection,
    needs_file_changes: bool,
    diff_merges: DiffMerges,
    mut on_item: impl FnMut(PrefetchItem) -> Result<bool, String>,
) -> Result<(), String> {
    let mut stash = HashMap::new();
    let mut callbacks = PrefetchWalkCallbacks {
        repo,
        first_parent: prep.first_parent,
        proj,
        needs_file_changes,
        diff_merges,
        stash: &mut stash,
    };
    let mut walk = CommitDateWalk::new(
        prep.tips,
        prep.interesting,
        prep.max_count,
        &mut callbacks,
    )
    .map_err(|e| e.to_string())?;

    while let Some(oid) = walk.next(&mut callbacks).map_err(|e| e.to_string())? {
        let item = callbacks
            .stash
            .remove(&oid)
            .ok_or_else(|| format!("prefetch stash miss for {}", bytes_to_oid(oid)))?;
        if !on_item(item)? {
            break;
        }
    }
    Ok(())
}

pub(crate) fn emit_prefetch_item(
    repo: &Repository,
    item: &PrefetchItem,
    opts: &EmitOpts,
    sink: &mut impl CommitSink,
    ring: &mut BlobRing,
) -> Result<(), Box<dyn Error>> {
    let oid = bytes_to_oid(item.oid);
    emit_inspected_commit(oid, &item.meta, sink);
    let skip = opts.skip_file_changes
        || (opts.diff_merges == DiffMerges::Off && item.parent_count > 1);
    if !skip {
        let tree_id = bytes_to_oid(item.tree_id);
        let parent_tree_id = item.parent_tree_id.map(bytes_to_oid);
        debug_assert!(
            item.parent_count == 0 || parent_tree_id.is_some() || opts.diff_merges == DiffMerges::Off,
            "non-root diffing commit must carry parent_tree_id"
        );
        emit_file_changes_trees(
            repo,
            tree_id,
            parent_tree_id,
            opts.ignore_all_space,
            opts.rename_threshold,
            sink,
            ring,
        )?;
    }
    Ok(())
}

#[allow(dead_code)] // retained for OID-only walk helpers / diagnostics
pub(crate) fn run_commit_date_walk(
    repo: &Repository,
    prep: WalkPrepared,
    mut on_oid: impl FnMut(OidBytes) -> Result<bool, String>,
) -> Result<(), String> {
    let mut callbacks = LibgitWalkCallbacks {
        repo,
        first_parent: prep.first_parent,
        proj: MetaProjection::default(),
        emit_cache: None,
    };
    let mut walk = CommitDateWalk::new(
        prep.tips,
        prep.interesting,
        prep.max_count,
        &mut callbacks,
    )
    .map_err(|e| e.to_string())?;

    while let Some(oid) = walk.next(&mut callbacks).map_err(|e| e.to_string())? {
        if !on_oid(oid)? {
            break;
        }
    }
    Ok(())
}

/// Start a date walk that can be resumed across calls with a fresh repo borrow.
pub(crate) fn start_commit_date_walk(
    repo: &Repository,
    prep: WalkPrepared,
    proj: MetaProjection,
    emit_cache: Option<&mut HashMap<OidBytes, InspectedCommit>>,
) -> Result<CommitDateWalk, Box<dyn Error>> {
    let mut callbacks = LibgitWalkCallbacks {
        repo,
        first_parent: prep.first_parent,
        proj,
        emit_cache,
    };
    CommitDateWalk::new(
        prep.tips,
        prep.interesting,
        prep.max_count,
        &mut callbacks,
    )
}

pub(crate) fn walk_next_oid(
    repo: &Repository,
    first_parent: bool,
    proj: MetaProjection,
    walk: &mut CommitDateWalk,
    emit_cache: Option<&mut HashMap<OidBytes, InspectedCommit>>,
) -> Result<Option<OidBytes>, Box<dyn Error>> {
    let mut callbacks = LibgitWalkCallbacks {
        repo,
        first_parent,
        proj,
        emit_cache,
    };
    walk.next(&mut callbacks)
}

pub(crate) fn walk_commit_oids(
    repo: &Repository,
    revision: Option<&[RevisionTerm]>,
    max_count: Option<usize>,
    first_parent: bool,
    all_refs: bool,
) -> Result<Vec<Oid>, Box<dyn Error>> {
    let prep = prepare_walk(repo, revision, max_count, first_parent, all_refs)?;
    let first_parent = prep.first_parent;
    let mut callbacks = LibgitWalkCallbacks {
        repo,
        first_parent,
        proj: MetaProjection::default(),
        emit_cache: None,
    };
    let ordered = CommitDateWalk::new(
        prep.tips,
        prep.interesting,
        prep.max_count,
        &mut callbacks,
    )?
    .drain_into_vec(&mut callbacks)?;

    Ok(ordered.into_iter().map(bytes_to_oid).collect())
}

pub(crate) struct EmitOpts {
    pub ignore_all_space: bool,
    pub skip_file_changes: bool,
    pub diff_merges: DiffMerges,
    pub rename_threshold: Option<u16>,
}

pub(crate) fn emit_commit(
    repo: &Repository,
    oid: git2::Oid,
    opts: &EmitOpts,
    sink: &mut impl CommitSink,
    ring: &mut BlobRing,
) -> Result<(), Box<dyn Error>> {
    #[cfg(feature = "git-log-stats")]
    crate::git::diag::record_emit_find_commit();
    let commit = repo.find_commit(oid)?;
    let header = commit_header(commit.raw_header_bytes());
    let author = parse_ident(header, b"author")?;
    let committer = parse_ident(header, b"committer")?;

    let hex = oid_hex(oid.as_bytes());
    sink.commit_id(&hex);
    sink.author(author.name, author.email, author.seconds);
    sink.committer(committer.name, committer.email, committer.seconds);
    sink.message(commit.message_raw_bytes());

    let parent_count = commit.parent_count();
    sink.begin_parents(parent_count);
    for i in 0..parent_count {
        let parent_hex = oid_hex(commit.parent_id(i)?.as_bytes());
        sink.parent(&parent_hex);
    }

    let skip = opts.skip_file_changes || (opts.diff_merges == DiffMerges::Off && parent_count > 1);
    if !skip {
        emit_file_changes(
            repo,
            &commit,
            opts.ignore_all_space,
            opts.rename_threshold,
            sink,
            ring,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::sink::CollectingSink;

    const SECOND_COMMIT: &str = "2e6d5e79dafd8ff8c09152ac35e32cd26e65efe5";
    const PARITY: &str = "test/fixtures/parity.git";
    const MISSING_BLOB: &str = "test/fixtures/missing-blob.git";

    fn peel_commit(repo: &Repository, spec: &str) -> git2::Oid {
        repo.revparse_single(spec)
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .id()
    }

    fn emit(
        repo: &Repository,
        oid: git2::Oid,
        skip_file_changes: bool,
        diff_merges: DiffMerges,
        ring: &mut BlobRing,
    ) -> Result<(), Box<dyn Error>> {
        let mut sink = CollectingSink::default();
        sink.begin_row(0);
        let result = emit_commit(
            repo,
            oid,
            &EmitOpts {
                ignore_all_space: false,
                skip_file_changes,
                diff_merges,
                rename_threshold: None,
            },
            &mut sink,
            ring,
        );
        sink.finish_row();
        result
    }

    #[test]
    fn read_commit_honors_skip_file_changes() {
        let repo = Repository::open(".").unwrap();
        let oid = git2::Oid::from_str(SECOND_COMMIT).unwrap();

        let mut skipped = CollectingSink::default();
        skipped.begin_row(0);
        let mut skip_ring = BlobRing::new();
        emit_commit(
            &repo,
            oid,
            &EmitOpts {
                ignore_all_space: false,
                skip_file_changes: true,
                diff_merges: DiffMerges::FirstParent,
                rename_threshold: None,
            },
            &mut skipped,
            &mut skip_ring,
        )
        .unwrap();
        skipped.finish_row();
        assert!(skipped.row.file_changes.is_empty());

        let mut kept = CollectingSink::default();
        kept.begin_row(0);
        let mut keep_ring = BlobRing::new();
        emit_commit(
            &repo,
            oid,
            &EmitOpts {
                ignore_all_space: false,
                skip_file_changes: false,
                diff_merges: DiffMerges::FirstParent,
                rename_threshold: None,
            },
            &mut kept,
            &mut keep_ring,
        )
        .unwrap();
        kept.finish_row();
        assert!(!kept.row.file_changes.is_empty());
    }

    #[test]
    fn note_commit_finishes_ring() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "note");
        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert!(ring.finish_count() >= 1);
    }

    #[test]
    fn root_commit_runs_epilogue() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "v1");
        assert_eq!(repo.find_commit(oid).unwrap().parent_count(), 0);
        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert!(ring.finish_count() >= 1);
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn emit_file_changes_trees_matches_commit_path_for_root() {
        use super::super::diff::{emit_file_changes, emit_file_changes_trees};
        use crate::git::sink::CollectingSink;

        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "v1");
        let commit = repo.find_commit(oid).unwrap();
        assert_eq!(commit.parent_count(), 0);

        let mut via_trees = CollectingSink::default();
        let mut ring_trees = BlobRing::new();
        via_trees.begin_row(0);
        emit_file_changes_trees(
            &repo,
            commit.tree_id(),
            None,
            false,
            None,
            &mut via_trees,
            &mut ring_trees,
        )
        .unwrap();

        let mut via_commit = CollectingSink::default();
        let mut ring_commit = BlobRing::new();
        via_commit.begin_row(0);
        emit_file_changes(
            &repo,
            &commit,
            false,
            None,
            &mut via_commit,
            &mut ring_commit,
        )
        .unwrap();

        assert_eq!(via_trees.row.file_changes, via_commit.row.file_changes);
    }

    /// Invalid UTF-8 filename; both root and parent diffs must keep raw bytes.
    const NON_UTF8_PATH: &[u8] = b"\xff.txt";

    fn index_entry(path: &[u8]) -> git2::IndexEntry {
        git2::IndexEntry {
            ctime: git2::IndexTime::new(0, 0),
            mtime: git2::IndexTime::new(0, 0),
            dev: 0,
            ino: 0,
            mode: 0o100644,
            uid: 0,
            gid: 0,
            file_size: 0,
            id: git2::Oid::ZERO_SHA1,
            flags: 0,
            flags_extended: 0,
            path: path.to_vec(),
        }
    }

    /// Keeps raw `file_change` paths (CollectingSink is UTF-8-lossy).
    #[derive(Default)]
    struct PathSink {
        paths: Vec<Vec<u8>>,
    }

    impl CommitSink for PathSink {
        fn begin_row(&mut self, _idx: usize) {}
        fn commit_id(&mut self, _hex: &[u8]) {}
        fn author(&mut self, _name: &[u8], _email: &[u8], _seconds: i64) {}
        fn committer(&mut self, _name: &[u8], _email: &[u8], _seconds: i64) {}
        fn message(&mut self, _msg: &[u8]) {}
        fn begin_parents(&mut self, _count: usize) {}
        fn parent(&mut self, _hex: &[u8]) {}
        fn begin_decorate(&mut self, _count: usize) {}
        fn decorate_name(&mut self, _name: &str) {}
        fn file_change(&mut self, fc: crate::git::sink::FileChangeRef<'_>) {
            self.paths.push(fc.path.to_vec());
        }
        fn finish_row(&mut self) {}
    }

    fn emit_paths(repo: &Repository, oid: git2::Oid) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
        let mut sink = PathSink::default();
        sink.begin_row(0);
        emit_commit(
            repo,
            oid,
            &EmitOpts {
                ignore_all_space: false,
                skip_file_changes: false,
                diff_merges: DiffMerges::Off,
                rename_threshold: None,
            },
            &mut sink,
            &mut BlobRing::new(),
        )?;
        sink.finish_row();
        Ok(sink.paths)
    }

    /// Parent-diff emits non-UTF-8 paths via `path_bytes()`.
    #[test]
    fn non_root_commit_emits_non_utf8_path() {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        let empty = repo.treebuilder(None).unwrap().write().unwrap();
        let empty_tree = repo.find_tree(empty).unwrap();
        let root = repo
            .commit(None, &sig, &sig, "empty root", &empty_tree, &[])
            .unwrap();
        let root_commit = repo.find_commit(root).unwrap();

        let mut index = repo.index().unwrap();
        index
            .add_frombuffer(&index_entry(NON_UTF8_PATH), b"hello\n")
            .unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let child = repo
            .commit(None, &sig, &sig, "add non-utf8", &tree, &[&root_commit])
            .unwrap();

        let paths = emit_paths(&repo, child).unwrap();
        assert!(
            paths.iter().any(|p| p.as_slice() == NON_UTF8_PATH),
            "parent-diff must keep non-UTF-8 paths; got {paths:?}"
        );
    }

    /// Root commits also go through tree-to-tree + `path_bytes()` (not `entry.name()`).
    #[test]
    fn root_commit_emits_non_utf8_path() {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        let mut index = repo.index().unwrap();
        index
            .add_frombuffer(&index_entry(NON_UTF8_PATH), b"hello\n")
            .unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let root = repo
            .commit(None, &sig, &sig, "root with non-utf8", &tree, &[])
            .unwrap();
        assert_eq!(repo.find_commit(root).unwrap().parent_count(), 0);

        let paths = emit_paths(&repo, root).unwrap();
        assert!(
            paths.iter().any(|p| p.as_slice() == NON_UTF8_PATH),
            "root commit must emit non-UTF-8 paths like parent-diff; got {paths:?}"
        );
    }

    #[test]
    fn rename_inserts_old_blob_bytes() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "rename");
        let parent = repo.find_commit(oid).unwrap().parent(0).unwrap();
        let old_id = parent.tree().unwrap().get_name("note.txt").unwrap().id();
        let expected = repo.find_blob(old_id).unwrap().content().to_vec();

        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert!(!ring.is_empty());
        assert_eq!(ring.lookup(old_id), Some(expected.as_slice()));
        assert!(ring.contains_path(b"note.txt"));
        assert!(!ring.contains_path(b"renamed.txt"));
    }

    #[test]
    fn merge_off_does_not_touch_ring() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "merged");
        let mut ring = BlobRing::new();
        emit(&repo, oid, false, DiffMerges::Off, &mut ring).unwrap();
        assert_eq!(ring.finish_count(), 0);
    }

    #[test]
    fn skip_file_changes_does_not_finish_ring() {
        let repo = Repository::open(PARITY).unwrap();
        let oid = peel_commit(&repo, "note");
        let mut ring = BlobRing::new();
        emit(&repo, oid, true, DiffMerges::Off, &mut ring).unwrap();
        assert_eq!(ring.finish_count(), 0);
    }

    #[test]
    fn missing_blob_does_not_mutate_ring() {
        let repo = Repository::open(MISSING_BLOB).unwrap();
        let oid = repo.head().unwrap().peel_to_commit().unwrap().id();
        let mut ring = BlobRing::new();
        assert!(emit(&repo, oid, false, DiffMerges::Off, &mut ring).is_err());
        assert_eq!(ring.finish_count(), 0);
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn err_after_old_miss_does_not_finish_pending() {
        // `a.txt` sorts before `b.bin` so the keep-file old is pending before
        // the missing blob fails (path-sorted deltas).
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        std::fs::write(dir.path().join("a.txt"), "v1\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("a.txt")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let c1 = repo
            .commit(Some("HEAD"), &sig, &sig, "one", &tree, &[])
            .unwrap();
        let parent = repo.find_commit(c1).unwrap();
        let old_keep = tree.get_name("a.txt").unwrap().id();

        std::fs::write(dir.path().join("a.txt"), "v2\n").unwrap();
        std::fs::write(dir.path().join("b.bin"), "gone\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("a.txt")).unwrap();
        index.add_path(std::path::Path::new("b.bin")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let c2 = repo
            .commit(Some("HEAD"), &sig, &sig, "two", &tree, &[&parent])
            .unwrap();
        let z_id = tree.get_name("b.bin").unwrap().id();
        let hex = z_id.to_string();
        std::fs::remove_file(
            dir.path()
                .join(".git/objects")
                .join(&hex[..2])
                .join(&hex[2..]),
        )
        .unwrap();

        let mut ring = BlobRing::new();
        emit(&repo, c1, false, DiffMerges::Off, &mut ring).unwrap();
        assert_eq!(ring.finish_count(), 1);
        assert!(emit(&repo, c2, false, DiffMerges::Off, &mut ring).is_err());
        assert_eq!(ring.finish_count(), 1);
        assert!(ring.lookup(old_keep).is_none());
    }
}

#[cfg(test)]
mod walk_all_refs_tests {
    use super::{collect_all_tips, walk_commit_oids};
    use crate::git::revision::RevisionTerm;
    use git2::Repository;

    const PARITY: &str = "test/fixtures/parity.git";
    const V1_COMMIT: &str = "ff09a62b129cc936f13bc67c5e2dba84f397c64b";

    #[test]
    fn walk_all_refs_matches_rev_list_all() {
        let repo = Repository::open(PARITY).unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        assert_eq!(oids.len(), 14);
        let default = walk_commit_oids(&repo, None, None, false, false).unwrap();
        assert_eq!(default.len(), 10);
        let orphan = git2::Oid::from_str("8a2afdc773a23dcd4aeb85aee134cd884f9463f9").unwrap();
        assert!(oids.contains(&orphan));
        assert!(!default.contains(&orphan));
    }

    #[test]
    fn walk_all_refs_peels_annotated_tag_tip() {
        let repo = Repository::open(PARITY).unwrap();
        let oids = walk_commit_oids(&repo, None, None, false, true).unwrap();
        let v1 = git2::Oid::from_str(V1_COMMIT).unwrap();
        assert!(oids.contains(&v1));
    }

    #[test]
    fn walk_hide_free_max_count_one_peels_annotated_tag() {
        let repo = Repository::open(PARITY).unwrap();
        let terms = [RevisionTerm {
            spec: "v1".into(),
            negate: false,
            origin: "v1".into(),
        }];
        let oids = walk_commit_oids(&repo, Some(&terms), Some(1), false, false).unwrap();
        assert_eq!(oids.len(), 1);
        assert_eq!(oids[0].to_string(), V1_COMMIT);
    }

    #[test]
    fn walk_with_hide_keeps_interesting_filter() {
        let repo = Repository::open(PARITY).unwrap();
        let terms = [
            RevisionTerm {
                spec: "rename".into(),
                negate: false,
                origin: "rename".into(),
            },
            RevisionTerm {
                spec: "note".into(),
                negate: true,
                origin: "^note".into(),
            },
        ];
        let oids = walk_commit_oids(&repo, Some(&terms), None, false, false).unwrap();
        assert_eq!(oids.len(), 1);
        assert_eq!(
            oids[0].to_string(),
            "95937d42365c812ebe6893e756cde1d0d86ae10b"
        );
    }

    #[test]
    fn all_tips_seed_order_is_refname_then_head() {
        let repo = Repository::open(PARITY).unwrap();
        let tips = collect_all_tips(&repo).unwrap();

        let mut expected_named: Vec<(Vec<u8>, git2::Oid)> = Vec::new();
        for reference in repo.references().unwrap() {
            let reference = reference.unwrap();
            let name = reference.name_bytes().to_vec();
            if !crate::git::all_refs::is_log_all_ref_bytes(&name) {
                continue;
            }
            if let Ok(commit) = reference.peel_to_commit() {
                expected_named.push((name, commit.id()));
            }
        }
        expected_named.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected: Vec<git2::Oid> = expected_named.into_iter().map(|(_, id)| id).collect();
        if let Ok(head) = repo.head() {
            if let Ok(commit) = head.peel_to_commit() {
                expected.push(commit.id());
            }
        }
        assert_eq!(tips, expected, "all_refs tips must be refname-byte-sorted then HEAD");
    }
}
