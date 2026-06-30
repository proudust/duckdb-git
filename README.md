# duckdb-git

A [DuckDB](https://duckdb.org/) extension for querying Git repository history with SQL.

## Usage

Load the extension and query any local Git repository:

```sql
LOAD 'duckdb_git';

SELECT * FROM git_log('.');
```

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

### Dependencies

- [Rust](https://www.rust-lang.org/tools/install)
- Python 3
- [Make](https://www.gnu.org/software/make)
- Git

### Build

```sh
# Build
make configure
make release # or `make debug` for debug builds
# The built extension is written to `build/release/extension/duckdb_git/duckdb_git.duckdb_extension`.

# Run
duckdb -unsigned
```

```sql
LOAD 'build/release/extension/duckdb_git/duckdb_git.duckdb_extension';
SELECT * FROM git_log('.');
```
