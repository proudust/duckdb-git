# duckdb-git

A [DuckDB](https://duckdb.org/) extension for querying Git repository history with SQL.

## Usage

Require DuckDB v1.5.4 or later.

Since it is not signed, you need to start it with the `-unsigned` option.

```sh
duckdb -unsigned
```

```sql
-- Install the binary for your platform
INSTALL 'https://github.com/proudust/duckdb-git/releases/latest/download/duckdb_git.duckdb_extension.linux_amd64.gz';
INSTALL 'https://github.com/proudust/duckdb-git/releases/latest/download/duckdb_git.duckdb_extension.linux_arm64.gz';
INSTALL 'https://github.com/proudust/duckdb-git/releases/latest/download/duckdb_git.duckdb_extension.osx_amd64.gz';
INSTALL 'https://github.com/proudust/duckdb-git/releases/latest/download/duckdb_git.duckdb_extension.osx_arm64.gz';
INSTALL 'https://github.com/proudust/duckdb-git/releases/latest/download/duckdb_git.duckdb_extension.windows_amd64.gz';

LOAD duckdb_git;

SELECT * FROM git_log('.');
```

> [!NOTE]
> If you see the following error when running `LOAD`, your local extension cache might be corrupted.
> Try running `FORCE INSTALL` to reinstall, and then restart duckdb.
> 
> ```
> Invalid Input Error:
> Failed to load 'duckdb_git', The file is not a DuckDB extension. The metadata at the end of the file is invalid
> ```

## Example

```sql
-- git log
select commit_id, author, author_email, author_timestamp, decorate, message from git_log('.');

-- git log --numstat (merge commits omit file_changes by default; same as --diff-merges=off)
select * from git_log('.');

-- git -C /path/to/repo log main -10
select commit_id, message from git_log('/path/to/repo', revision='main', max_count=10);

-- git log dev ^main
select commit_id, message from git_log('.', revision=['dev', '^main']);

-- git log --decorate
select commit_id, decorate from git_log('.') where len(decorate) > 0;

-- git log --numstat -10 --diff-merges=first-parent
select commit_id, change.path, change.add_lines, change.del_lines
from (
  select commit_id, unnest(file_changes) as change from git_log('.', max_count=10, diff_merges='first_parent')
);
```

## Function Reference

### `git_log(repo_path, ...)`

Returns commit history as a table.

#### Parameters

| Name               | Type                         | Default       | Description                                                   |
| ------------------ | ---------------------------- | ------------- | ------------------------------------------------------------- |
| `repo_path`        | `VARCHAR`                    | *(required)*  | Path to the Git repository                                    |
| `revision`         | `VARCHAR` or `LIST(VARCHAR)` | `NULL` (HEAD) | One or more revspecs, same syntax as `git log`. [^1]          |
| `max_count`        | `INTEGER`                    | `NULL` (all)  | Maximum number of commits to return                           |
| `ignore_all_space` | `BOOLEAN`                    | `false`       | Ignore whitespace changes in diffs                            |
| `diff_merges`      | `VARCHAR`                    | `'off'`       | How to show diffs for merge commits (`off`, `first-parent`).  |
| `decorate`         | `VARCHAR`                    | `'short'`     | Ref name format in the `decorate` column (`short` or `full`). |
| `backend`          | `VARCHAR`                    | `'libgit'`    | Determines how history is retrieved. [^2]                     |

[^1]: Symmetric differences (`a...b`) are not supported.
[^2]: If you build with the `gix-backend` feature included, you can also specify `gix`.

#### Output Columns

| Column                | Type                     | Description                                  |
| --------------------- | ------------------------ | -------------------------------------------- |
| `commit_id`           | `VARCHAR NOT NULL`       | Full commit hash                             |
| `author`              | `VARCHAR NOT NULL`       | Author name                                  |
| `author_email`        | `VARCHAR NOT NULL`       | Author email                                 |
| `author_timestamp`    | `TIMESTAMPTZ NOT NULL`   | Author timestamp                             |
| `committer`           | `VARCHAR NOT NULL`       | Committer name                               |
| `committer_email`     | `VARCHAR NOT NULL`       | Committer email                              |
| `committer_timestamp` | `TIMESTAMPTZ NOT NULL`   | Committer timestamp                          |
| `message`             | `VARCHAR NOT NULL`       | Commit message                               |
| `parents`             | `VARCHAR[] NOT NULL`     | Parent commit hashes                         |
| `decorate`            | `VARCHAR[] NOT NULL`     | Branch and tag names pointing at this commit |
| `file_changes`        | `STRUCT(...)[] NOT NULL` | File changes                                 |

The `file_changes` struct contains:

| Field       | Type               | Description                                                 |
| ----------- | ------------------ | ----------------------------------------------------------- |
| `path`      | `VARCHAR NOT NULL` | File path (new path for renames/copies)                     |
| `old_path`  | `VARCHAR NULL`     | Previous path for renames/copies; `NULL` otherwise          |
| `status`    | `VARCHAR NOT NULL` | Change status (`A`dd, `D`elete, `M`odify, `R`ename, `C`opy) |
| `blob_id`   | `VARCHAR NOT NULL` | Git blob object ID                                          |
| `file_size` | `BIGINT NULL`      | File size in bytes                                          |
| `add_lines` | `INTEGER NOT NULL` | Lines added                                                 |
| `del_lines` | `INTEGER NOT NULL` | Lines deleted                                               |

## Building

Require:

- [Rust](https://www.rust-lang.org/tools/install)
- Python 3
- [Make](https://www.gnu.org/software/make)
- Git

```sh
make configure # First only (Needs init submodule)
make debug # Build (debug, libgit-only binary)
make debug_gix # Build (debug, libgit + gix)
make release # Build (release, libgit-only binary)
make release_gix # Build (release, libgit + gix)
make test # E2E tests (sqllogictest, libgit-only binary)
make test_gix # E2E tests (sqllogictest, libgit + gix)
make bench # Run benchmarks (libgit + gix backends)
BENCH_REPO=/path/to/large/repo make bench # Run benchmarks on a specific repo
```

## License

[MIT License](LICENSE).

The [libgit2](https://github.com/libgit2/libgit2) library is statically linked,
but remains under its [linking exception](https://github.com/libgit2/libgit2/blob/main/COPYING),
meaning GPLv2 requirements do not apply to this project.

---

This extension was developed using Claude Code and Cursor.
