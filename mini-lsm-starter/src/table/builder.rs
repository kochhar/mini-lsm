use bytes::{BufMut, Bytes};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable};
use crate::{block::BlockBuilder, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    data: Vec<u8>,
    // for use while building the current block
    block_size: usize,
    current_block: BlockBuilder,
    first_key: Vec<u8>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: Vec::new(),
            data: Vec::new(),
            block_size,
            current_block: BlockBuilder::new(block_size),
            first_key: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.
    /// (`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }

        // successfully add to the current block?
        if self.current_block.add(key, value) {
            return;
        }

        // condition above is false, current block full
        // finish up the current block
        self.finalize_block();

        // now adding the new key and value
        assert!(self.current_block.add(key, value));
        self.first_key = key.to_vec();
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just
    /// return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to
    /// actually write to disk until chapter 4 block cache.
    pub fn build(
        mut self,
        _id: usize,
        _block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // finishing up current in-flight block if any.
        self.finalize_block();

        let meta_offset = self.data.len();
        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            block_metas: self.meta,
            block_meta_offset: meta_offset,
        })
    }

    fn finalize_block(&mut self) {
        // Replace the current block builder with a new instance to free up the
        // ownership. This allows invoking the build() method, which needs
        // ownership of the self reference.
        let builder =
            std::mem::replace(&mut self.current_block, BlockBuilder::new(self.block_size));
        let block_bytes = builder.build().encode();

        // First push the block metadata, since it records the data length as offset
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: Bytes::from(std::mem::take(&mut self.first_key)),
        });
        // Then add the block to the data
        self.data.extend(block_bytes);
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
