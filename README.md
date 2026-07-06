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
> Try running `FORCE INSTALL` to reinstall the extension.
> 
> ```
> Invalid Input Error:
> Failed to load 'duckdb_git', The file is not a DuckDB extension. The metadata at the end of the file is invalid
> ```

## Example

```sql
-- git log
select commit_id, author, author_email, author_timestamp, decorate, message from git_log('.');

-- git log --numstat
select * from git_log('.');

-- git -C /path/to/repo log main -10
select commit_id, message from git_log('/path/to/repo', revision='main', max_count=10);

-- git log --decorate
select commit_id, decorate from git_log('.') where len(decorate) > 0;

-- git log --numstat -10
select commit_id, change.path, change.add_lines, change.del_lines
from (
  select commit_id, unnest(file_changes) as change from git_log('.', max_count=10)
);
```

## Function Reference

### `git_log(repo_path, ...)`

Returns commit history as a table.

#### Parameters

| Name               | Type      | Default       | Description                               |
| ------------------ | --------- | ------------- | ----------------------------------------- |
| `repo_path`        | `VARCHAR` | *(required)*  | Path to the Git repository                |
| `revision`         | `VARCHAR` | `NULL` (HEAD) | Branch, tag, or commit hash               |
| `max_count`        | `INTEGER` | `NULL` (all)  | Maximum number of commits to return       |
| `ignore_all_space` | `BOOLEAN` | `false`       | Ignore whitespace changes in diffs        |
| `backend`          | `VARCHAR` | `'libgit'`    | Determines how history is retrieved. [^1] |

[^1]: If you build with the `gix-backend` feature included, you can also specify `gix`.

#### Output Columns

| Column                | Type                     | Description                                        |
| --------------------- | ------------------------ | -------------------------------------------------- |
| `commit_id`           | `VARCHAR NOT NULL`       | Full commit hash                                   |
| `author`              | `VARCHAR NOT NULL`       | Author name                                        |
| `author_email`        | `VARCHAR NOT NULL`       | Author email                                       |
| `author_timestamp`    | `TIMESTAMPTZ NOT NULL`   | Author timestamp                                   |
| `committer`           | `VARCHAR NOT NULL`       | Committer name                                     |
| `committer_email`     | `VARCHAR NOT NULL`       | Committer email                                    |
| `committer_timestamp` | `TIMESTAMPTZ NOT NULL`   | Committer timestamp                                |
| `message`             | `VARCHAR NOT NULL`       | Commit message                                     |
| `parents`             | `VARCHAR[] NOT NULL`     | Parent commit hashes                               |
| `decorate`            | `VARCHAR[] NOT NULL`     | Branch and tag short names pointing at this commit |
| `file_changes`        | `STRUCT(...)[] NOT NULL` | File changes                                       |

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
make bench # Run benchmarks (cargo bench, libgit-only binary)
BENCH_REPO=/path/to/large/repo make bench # Run benchmarks on a specific repo
make bench_gix # Run benchmarks (cargo bench, libgit + gix)
```
