//! Helpers for microbenchmarks (not part of the public API).

use crate::git::sink::{CommitSink, FileChangeRef};
use crate::git_log::schema::{file_changes_list_type, GitLogColumn};
use crate::git_log::vector::VectorInserter;
use duckdb::core::DataChunkHandle;

/// Fill one DuckDB chunk with `rows` lists of `changes_per_row` file_changes.
///
/// When `reserve` is true, calls [`CommitSink::begin_file_changes`] before each
/// row's inserts (the optimization under test).
pub fn fill_file_changes_chunk(rows: usize, changes_per_row: usize, reserve: bool) {
    let chunk = DataChunkHandle::new(&[file_changes_list_type()]);
    let cols = [GitLogColumn::FileChanges.index()];
    let mut sink = VectorInserter::new(&chunk, &cols);

    let path = b"src/example/path.rs";
    let blob_id = b"0123456789abcdef0123456789abcdef01234567";

    for row in 0..rows {
        sink.begin_row(row);
        if reserve {
            sink.begin_file_changes(changes_per_row);
        }
        for _ in 0..changes_per_row {
            sink.file_change(FileChangeRef {
                path,
                old_path: None,
                status: "M",
                blob_id,
                file_size: Some(128),
                add_lines: Some(3),
                del_lines: Some(1),
            });
        }
        sink.finish_row();
    }
    sink.finish();
    chunk.set_len(rows);
}

/// Replay `commits` rows of `changes_per_commit` inserts in batches of at most
/// `batch_rows` (mirrors `git_log` scanner chunking).
pub fn fill_file_changes_history(
    commits: usize,
    changes_per_commit: usize,
    reserve: bool,
    batch_rows: usize,
) {
    let batch_rows = batch_rows.max(1);
    let mut done = 0;
    while done < commits {
        let rows = (commits - done).min(batch_rows);
        fill_file_changes_chunk(rows, changes_per_commit, reserve);
        done += rows;
    }
}
