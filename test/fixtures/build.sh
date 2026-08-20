#!/usr/bin/env bash
# Rebuild test/fixtures/parity.git and missing-blob.git.
# Deterministic OIDs via fixed author/committer timestamps.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/parity-fixture.XXXXXX")"
trap 'rm -rf "$WORKDIR"' EXIT

export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null
export GIT_AUTHOR_NAME='Fixture'
export GIT_AUTHOR_EMAIL='fixture@example.com'
export GIT_COMMITTER_NAME='Fixture'
export GIT_COMMITTER_EMAIL='fixture@example.com'

ts() { printf '%s +0000' "$1"; }

commit_tree() {
  local unix=$1 msg=$2
  shift 2
  local parents=() p
  for p in "$@"; do
    parents+=(-p "$p")
  done
  GIT_AUTHOR_DATE="$(ts "$unix")" GIT_COMMITTER_DATE="$(ts "$unix")" \
    git commit-tree "$(git write-tree)" -m "$msg" "${parents[@]}"
}

# Publish a work tree as a bare fixture with loose objects and a minimal config.
publish_bare() {
  local src=$1 dest=$2
  shift 2
  rm -rf "$dest"
  git clone -q --bare "$src" "$dest"
  rm -rf "$dest/hooks" "$dest/branches" "$dest/info"
  rm -f "$dest/description" "$dest/packed-refs"
  # Drop clone-local remotes (paths would be machine-specific)
  git --git-dir="$dest" remote | while read -r r; do
    git --git-dir="$dest" remote remove "$r"
  done
  # Expand packs to loose objects (inspectable / deletable as files)
  if compgen -G "$dest/objects/pack/*.pack" >/dev/null; then
    for pack in "$dest"/objects/pack/*.pack; do
      git --git-dir="$dest" unpack-objects -q <"$pack"
    done
    rm -f "$dest"/objects/pack/*
  fi
  rmdir "$dest/objects/pack" 2>/dev/null || true
  # Optional: write loose refs passed as "path=oid" or "path=ref: target"
  local spec path val
  for spec in "$@"; do
    path=${spec%%=*}
    val=${spec#*=}
    mkdir -p "$dest/$(dirname "$path")"
    printf '%s\n' "$val" >"$dest/$path"
  done
  printf '[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = true\n' \
    >"$dest/config"
}

echo "Building in $WORKDIR"
git init -q -b master "$WORKDIR/work"
cd "$WORKDIR/work"
git config user.name Fixture
git config user.email fixture@example.com

# ── DAG (topology / refs / purposes: see README.md) ──────────────────
# Root ships tracked.txt + space.txt as later diff parents.
printf 'base\n' >base.txt
printf 'tracked\n' >tracked.txt
printf 'line1\n  foo\nline3\n' >space.txt
git add base.txt tracked.txt space.txt
A=$(commit_tree 1700000100 'root')

printf 'L\n' >left.txt
git add left.txt
B=$(commit_tree 1700000200 'left' "$A")

# C from A's tree + right.txt only
rm -rf ./*
git read-tree "$A^{tree}"
git checkout-index -f -a
printf 'R\n' >right.txt
git add right.txt
C=$(commit_tree 1700000300 'right' "$A")

# D merge: B's tree + right.txt; first parent B
rm -rf ./*
git read-tree "$B^{tree}"
git checkout-index -f -a
printf 'R\n' >right.txt
git add right.txt
D=$(commit_tree 1700000400 'merge' "$B" "$C")

git update-ref refs/heads/master "$D"
git update-ref refs/heads/side "$C"

GIT_COMMITTER_DATE="$(ts 1700000410)" git tag -a v1 -m 'v1' "$A"
git tag left "$B"
git tag right "$C"
git tag merged "$D"
mkdir -p .git/refs/remotes/origin
printf '%s\n' "$D" >.git/refs/remotes/origin/main
printf 'ref: refs/remotes/origin/main\n' >.git/refs/remotes/origin/HEAD

# ── Diff / ident cases (linear on D; shared tips F/L/O — see README) ─
rm -rf ./*
git read-tree "$D^{tree}"
git checkout-index -f -a

rm -f tracked.txt
ln -s elsewhere tracked.txt
git add tracked.txt
printf 'binary\0data' >data.bin
git add data.bin
# Indent change + real content change: normal numstat 2/2, -w keeps 1/1
printf 'line1\n    foo\nline3 changed\n' >space.txt
git add space.txt
git rm -q left.txt
F=$(commit_tree 1700000600 'typechange, binary, whitespace, delete left.txt' "$D")

printf 'line1\nline2\nline3\nline4\nline5\n' >note.txt
git add note.txt
H=$(commit_tree 1700000800 'add note.txt' "$F")

git mv note.txt renamed.txt
printf 'line1\nline2\nline3 changed\nline4\nline5\n' >renamed.txt
git add renamed.txt
I=$(commit_tree 1700000900 'rename note.txt to renamed.txt' "$H")

EMPTY_TREE='4b825dc642cb6eb9a060e54bf8d69288fbee4904'
# Ensure empty tree object exists
git hash-object -t tree -w --stdin </dev/null >/dev/null
SUB1=$(GIT_AUTHOR_DATE="$(ts 1700000001)" GIT_COMMITTER_DATE="$(ts 1700000001)" \
  git commit-tree "$EMPTY_TREE" -m 'sub v1')
SUB2=$(GIT_AUTHOR_DATE="$(ts 1700000002)" GIT_COMMITTER_DATE="$(ts 1700000002)" \
  git commit-tree "$EMPTY_TREE" -m 'sub v2')

git read-tree "$I^{tree}"
git update-index --add --cacheinfo "160000,$SUB1,vendor"
# Untagged on purpose: decorate=[] coverage via gitlink-bump~1
K=$(commit_tree 1700001200 'add gitlink vendor' "$I")

# gitlink M + multipath adds (len > 1 file_changes); path-filtered for vendor.
rm -rf ./*
git read-tree "$K^{tree}"
git checkout-index -f -a
git update-index --add --cacheinfo "160000,$SUB2,vendor"
printf 'a\n' >multi_a.txt
printf 'b\n' >multi_b.txt
git add multi_a.txt multi_b.txt
L=$(commit_tree 1700001300 'bump gitlink; add multi_a.txt and multi_b.txt' "$K")

# Same tree as L: padded author name + author date ≠ committer (amend-like).
# `git commit-tree` strips spaces from GIT_AUTHOR_NAME, so write the object literally.
TREE=$(git rev-parse "$L^{tree}")
O_BODY=$(printf 'tree %s\nparent %s\nauthor  Pad Name  <pad@ex.com> 1700001600 +0000\ncommitter Fixture <fixture@example.com> 1700001700 +0000\n\namended\n' "$TREE" "$L")
O=$(printf '%s' "$O_BODY" | git hash-object -t commit -w --stdin)

git update-ref refs/heads/master "$O"

# Scenario tags for stable E2E revision= names (do not move DAG tags above).
# gitlink-add (K) stays untagged — revision='gitlink-bump~1'.
git tag typechange "$F"
git tag binary "$F"
git tag whitespace "$F"
git tag deleted "$F"
git tag note "$H"
git tag rename "$I"
git tag gitlink-bump "$L"
git tag multipath "$L"
git tag padded-author "$O"
git tag amended "$O"

# Orphan chmod: not a descendant of O (contained_tags / master stay put).
# Text + binary chmod-only (not rename — D+A before find_similar has a zero OID).
git read-tree "$EMPTY_TREE"
rm -rf ./*
printf 'mode\n' >mode.txt
printf 'mode\0bin' >mode.bin
git add mode.txt mode.bin
P=$(commit_tree 1700002000 'chmod base')
git update-index --chmod=+x -- mode.txt mode.bin
Q=$(commit_tree 1700002100 'chmod +x text and binary' "$P")
git tag chmod-text "$Q"
git tag chmod-binary "$Q"

# ── Publish parity.git ───────────────────────────────────────────────
BARE="$ROOT/parity.git"
TAG_OID=$(git --git-dir="$WORKDIR/work/.git" rev-parse refs/tags/v1)
publish_bare "$WORKDIR/work" "$BARE" \
  "HEAD=ref: refs/heads/master" \
  "refs/heads/master=$O" \
  "refs/heads/side=$C" \
  "refs/tags/left=$B" \
  "refs/tags/right=$C" \
  "refs/tags/merged=$D" \
  "refs/tags/v1=$TAG_OID" \
  "refs/tags/typechange=$F" \
  "refs/tags/binary=$F" \
  "refs/tags/whitespace=$F" \
  "refs/tags/deleted=$F" \
  "refs/tags/note=$H" \
  "refs/tags/rename=$I" \
  "refs/tags/gitlink-bump=$L" \
  "refs/tags/multipath=$L" \
  "refs/tags/padded-author=$O" \
  "refs/tags/amended=$O" \
  "refs/tags/chmod-text=$Q" \
  "refs/tags/chmod-binary=$Q" \
  "refs/remotes/origin/main=$D" \
  "refs/remotes/origin/HEAD=ref: refs/remotes/origin/main"

# ── Minimal missing-blob.git (independent of parity.git) ─────────────
# Empty root + one commit adding data.bin, then delete that blob object.
# Two commits so file_changes uses the parent-diff path (same as git log --numstat).
MISSING_SRC="$WORKDIR/missing"
MISSING="$ROOT/missing-blob.git"
git init -q -b master "$MISSING_SRC"
cd "$MISSING_SRC"
git config user.name Fixture
git config user.email fixture@example.com
MISSING_ROOT=$(commit_tree 1700000000 'root')
printf 'blob\0data' >data.bin
git add data.bin
MISSING_COMMIT=$(commit_tree 1700000001 'add data.bin' "$MISSING_ROOT")
git update-ref refs/heads/master "$MISSING_COMMIT"
MISSING_BLOB=$(git rev-parse "$MISSING_COMMIT:data.bin")
publish_bare "$MISSING_SRC" "$MISSING" \
  "HEAD=ref: refs/heads/master" \
  "refs/heads/master=$MISSING_COMMIT"
rm -f "$MISSING/objects/${MISSING_BLOB:0:2}/${MISSING_BLOB:2}"

{
  echo "A (root)              $A"
  echo "B (left)              $B"
  echo "C (right)             $C"
  echo "D (merge)             $D"
  echo "F (T+binary+ws+D)     $F"
  echo "H (add note.txt)      $H"
  echo "I (rename)            $I"
  echo "K (gitlink A, untagged) $K"
  echo "L (gitlink M+multipath) $L"
  echo "O (padded+amended tip) $O"
  echo "P (chmod base, orphan) $P"
  echo "Q (chmod +x)           $Q"
  echo "missing-blob commit   $MISSING_COMMIT"
  echo "missing-blob blob     $MISSING_BLOB"
}

# Sanity checks
git --git-dir="$BARE" log --oneline --decorate --graph --all | head -40
echo "--- first-parent name-status at D ---"
git --git-dir="$BARE" show --first-parent --name-status --format='' "$D"
echo "--- combo F (T/binary/ws/D) ---"
git --git-dir="$BARE" show --name-status --format='' "$F"
git --git-dir="$BARE" show --numstat --format='' "$F"
git --git-dir="$BARE" show -w --numstat --format='' "$F"
echo "--- rename ---"
git --git-dir="$BARE" show --name-status --format='' "$I"
echo "--- gitlink-bump+multipath ---"
git --git-dir="$BARE" show --name-status --format='' "$L"
echo "--- amended (empty diff) ---"
git --git-dir="$BARE" show --name-status --format='' "$O"
echo "--- chmod-only (M 0/0 text, M -/- binary) ---"
git --git-dir="$BARE" show --name-status --format='' "$Q"
git --git-dir="$BARE" show --numstat --format='' "$Q"
echo "--- missing-blob (expect fatal) ---"
if git --git-dir="$MISSING" log --numstat -1 >/dev/null 2>&1; then
  echo "expected missing blob to fail" >&2
  exit 1
fi
echo "Done."
