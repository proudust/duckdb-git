use crate::git::backend::libgit::{list_tags, CachedRepo, TagListOpts};
use crate::git::ref_row::TagRow;
use crate::git_tag::params::GitTagParameter;
use crate::git_tag::schema;
use crate::git_tag::vector::TagVectorInserter;
use duckdb::core::DataChunkHandle;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Inner {
    rows: Vec<TagRow>,
    current_index: AtomicUsize,
    batch_size: usize,
}

pub struct LibGitTagScanner {
    inner: Arc<Inner>,
}

impl LibGitTagScanner {
    pub fn open(
        repo_path: &str,
        params: &GitTagParameter,
        column_indices: &[u64],
    ) -> Result<Self, Box<dyn Error>> {
        let handle = CachedRepo::open(repo_path)?;
        let repo = handle.repo();
        let rows = list_tags(
            repo,
            &TagListOpts {
                format: params.decorate,
                filter: &params.filter,
                need_annotated_meta: schema::needs_annotated_meta(column_indices),
            },
        )?;
        Ok(Self {
            inner: Arc::new(Inner {
                rows,
                current_index: AtomicUsize::new(0),
                batch_size: 2048,
            }),
        })
    }

    pub fn max_threads(&self) -> u64 {
        1
    }

    pub fn read(
        &self,
        output: &mut DataChunkHandle,
        column_indices: &[u64],
    ) -> Result<u32, Box<dyn Error>> {
        let start = self
            .inner
            .current_index
            .fetch_add(self.inner.batch_size, Ordering::Relaxed);
        if start >= self.inner.rows.len() {
            return Ok(0);
        }
        let end = (start + self.inner.batch_size).min(self.inner.rows.len());
        let mut writer = TagVectorInserter::new(output, column_indices);
        for row in &self.inner.rows[start..end] {
            writer.write_row(row);
        }
        Ok((end - start) as u32)
    }
}
