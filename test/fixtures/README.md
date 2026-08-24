# Test fixtures

## `parity.git`

Bare repository used by E2E tests under `test/sql/{libgit,gix}/`. Rebuild with
`./test/fixtures/build.sh` (fixed author/committer timestamps → stable OIDs).

### `blobs/`

Binary inputs checked in for fixture builds (not fetched at CI time):

| File | Role |
| ---- | ---- |
| `trim_repro_old.bin` / `trim_repro_new.bin` | recharts `DefaultTooltipContent.tsx` pair; `common-tail` orphan commit; unit test for numstat `60/42` |

Obtain from recharts if missing:

```bash
COMMIT=265bb5b012cd265a0b2b659e352aef4c9e853c91
FILE=src/component/DefaultTooltipContent.tsx
git cat-file blob ${COMMIT}^:${FILE} > test/fixtures/blobs/trim_repro_old.bin
git cat-file blob ${COMMIT}:${FILE} > test/fixtures/blobs/trim_repro_new.bin
```

Prefer **tag / branch names** in SQL tests (`revision='rename'`, `revision='merged'`,
…). Keep raw SHAs mainly in `param_revision.test`, which exercises revspec syntax.

### Topology

```text
A (root) ── B (left) ── D (merge) ── F ── H ── I ── K ── L ── O (tip)
         └── C (right) ─┘

P (chmod base) ── Q (chmod +x)     [orphan; not on master]
S (common-tail base) ── R (common-tail modified)   [orphan; trim_repro numstat 60/42]
```

Root `A` already includes `tracked.txt` and `space.txt` so typechange / whitespace
do not need setup-only commits. Independent scenarios that do not need isolated
parents share a commit (multi tags, path-filtered in tests):

- F: `typechange` / `binary` / `whitespace` / `deleted`
- L: `gitlink-bump` / `multipath`

### Commit counts

| Walk | Count |
| ---- | ----- |
| HEAD (`git log` / default `git_log`) | **10** |
| `--all` / `all_refs=true` | **14** |

Orphan lineages (`chmod-text`, `common-tail`) are included only in the `--all` walk.

### Refs

| Ref                                             | Tip        | Notes                              |
| ----------------------------------------------- | ---------- | ---------------------------------- |
| `refs/tags/v1` (annotated)                      | A          | peel / decorate                    |
| `refs/tags/left`                                | B          |                                    |
| `refs/tags/right` / `refs/heads/side`           | C          |                                    |
| `refs/tags/merged` / `refs/remotes/origin/main` | D          | fixed; tip may advance             |
| `refs/remotes/origin/HEAD` → `origin/main`      | (symbolic) | remote HEAD alias                  |
| `typechange` / `binary` / `whitespace` / `deleted` | F       | same commit                        |
| `refs/tags/note`                                | H          |                                    |
| `refs/tags/rename`                              | I          |                                    |
| *(untagged)* `gitlink-bump~1`                   | K          | decorate=`[]` coverage             |
| `refs/tags/gitlink-bump` / `refs/tags/multipath` | L         | same commit                        |
| `refs/tags/padded-author` / `refs/tags/amended` | O          | same commit; tip                   |
| `chmod-text` / `chmod-binary`                   | Q          | orphan chmod; not reachable from master tip |
| `common-tail`                                   | R          | orphan; long common suffix; numstat `60/42` on `trim_repro.txt` |
| `refs/heads/master`                             | O (tip)    |                                    |

### Commits

| Tag / rev                                        | Subject                         | Purpose                                                              |
| ------------------------------------------------ | ------------------------------- | -------------------------------------------------------------------- |
| `v1`                                             | root                            | merge base A; ships `tracked.txt` + `space.txt` for later diffs      |
| `left`                                           | left                            | first parent of merge; adds `left.txt`                               |
| `right`                                          | right                           | second parent; adds `right.txt`                                      |
| `merged`                                         | merge                           | `diff_merges` / `parents`; first-parent adds `right.txt`             |
| `typechange` / `binary` / `whitespace` / `deleted` | combo on F                    | T / binary NULL / `-w` 2→1 / D (each path-filtered)                  |
| `note`                                           | add note.txt                    | ordinary text add (5 lines; rename similarity)                       |
| `rename`                                         | rename note.txt to renamed.txt  | `status = R`, `old_path`, numstat `1 1`                              |
| `gitlink-bump~1`                                 | add gitlink vendor              | untagged; gitlink `A`, numstat `1 0`, `file_size` NULL               |
| `gitlink-bump` / `multipath`                     | bump gitlink + multi files      | gitlink `M`; within-list `file_changes` (vendor + 2 paths)           |
| `padded-author` / `amended`                      | amended                         | same tree as L; `%an` keeps leading space; author ≠ committer        |
| `chmod-text` / `chmod-binary`                    | chmod +x text and binary        | same-OID `M`: text numstat `0/0`, binary NULL/NULL (chmod-only)      |
| `common-tail`                                    | common-tail modified            | `trim_repro.txt` `M`; numstat `60/42` (git default; pre-trim was 58/40) |

## `missing-blob.git`

Minimal bare repository (empty root + one commit adding `data.bin`) with that
blob object removed. Independent of `parity.git` — only used to assert that
selecting `file_changes` errors the same way as `git log --numstat` when a blob
is missing.
