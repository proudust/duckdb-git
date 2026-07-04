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

## Function Reference

### `git_log(repo_path, ...)`

Returns commit history as a table.

#### Parameters

| Name               | Type      | Default       | Description                                             |
| ------------------ | --------- | ------------- | ------------------------------------------------------- |
| `repo_path`        | `VARCHAR` | *(required)*  | Path to the Git repository                              |
| `revision`         | `VARCHAR` | `NULL` (HEAD) | Branch, tag, or commit hash                             |
| `max_count`        | `INTEGER` | `NULL` (all)  | Maximum number of commits to return                     |
| `diff_merges`      | `VARCHAR` | `'off'`       | File change retrieval mode: `'off'` or `'first-parent'` |
| `ignore_all_space` | `BOOLEAN` | `false`       | Ignore whitespace changes in diffs                      |

#### Output Columns

| Column                | Type            | Description                                     |
| --------------------- | --------------- | ----------------------------------------------- |
| `commit_id`           | `VARCHAR`       | Full commit hash                                |
| `author`              | `VARCHAR`       | Author name                                     |
| `author_email`        | `VARCHAR`       | Author email                                    |
| `author_timestamp`    | `TIMESTAMPTZ`   | Author timestamp                                |
| `committer`           | `VARCHAR`       | Committer name                                  |
| `committer_email`     | `VARCHAR`       | Committer email                                 |
| `committer_timestamp` | `TIMESTAMPTZ`   | Committer timestamp                             |
| `message`             | `VARCHAR`       | Commit message                                  |
| `parents`             | `VARCHAR[]`     | Parent commit hashes                            |
| `file_changes`        | `STRUCT(...)[]` | File changes (only when `diff_merges != 'off'`) |

The `file_changes` struct contains:

| Field       | Type      | Description                                                 |
| ----------- | --------- | ----------------------------------------------------------- |
| `path`      | `VARCHAR` | File path                                                   |
| `status`    | `VARCHAR` | Change status (`A`dd, `D`elete, `M`odify, `R`ename, `C`opy) |
| `blob_id`   | `VARCHAR` | Git blob object ID                                          |
| `file_size` | `BIGINT`  | File size in bytes                                          |
| `add_lines` | `INTEGER` | Lines added                                                 |
| `del_lines` | `INTEGER` | Lines deleted                                               |

## Building

Require:

- [Rust](https://www.rust-lang.org/tools/install)
- Python 3
- [Make](https://www.gnu.org/software/make)
- Git

```sh
make debug # Build (debug)
make release # Build (release)
make test # Run E2E tests (sqllogictest)
make bench # Run benchmarks on this repository
BENCH_REPO=/here/any/repo make bench # Run benchmarks on /here/any/repo
```
