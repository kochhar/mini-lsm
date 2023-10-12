use std::{debug_assert, sync::Arc};

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    // The internal `SsTable`, wrapped by an `Arc`
    sstable: Arc<SsTable>,
    // Index of the current block
    block_idx: usize,
    // Iterator for the current block
    block_iter: BlockIterator,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_idx = 0;
        let block_iter = BlockIterator::create_and_seek_to_first(table.read_block(0)?);
        let iter = Self {
            sstable: table,
            block_idx,
            block_iter,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.block_idx = 0;
        self.block_iter = BlockIterator::create_and_seek_to_first(self.sstable.read_block(0)?);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (block_idx, block_iter) = Self::_seek_to_key(&table, key)?;
        let iter = Self {
            sstable: table,
            block_idx,
            block_iter,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (block_idx, block_iter) = Self::_seek_to_key(&self.sstable, key)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }

    fn _seek_to_key(sstable: &Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
        println!("Seeking key {:?}", key);
        let mut block_idx = sstable.find_block_idx(key);
        println!("Found in block with index: {:?}", block_idx);
        let mut block_iter =
            BlockIterator::create_and_seek_to_key(sstable.read_block(block_idx)?, key);
        // For the case where a non-existent key lies between two blocks
        // 1. The search yields block_idx as the earlier block.
        // 2. seek_to_key seeks past the last entry in the block making the block_iter invalid
        if !block_iter.is_valid() {
            let (next_idx, opt_next_iter) = Self::_maybe_next_block(sstable, block_idx)?;
            if let Some(next_iter) = opt_next_iter {
                block_idx = next_idx;
                block_iter = next_iter;
            }
        }
        Ok((block_idx, block_iter))
    }

    fn _maybe_next_block(
        sstable: &Arc<SsTable>,
        block_idx: usize,
    ) -> Result<(usize, Option<BlockIterator>)> {
        if block_idx + 1 < sstable.num_of_blocks() {
            Ok((
                block_idx + 1,
                Some(BlockIterator::create_and_seek_to_first(
                    sstable.read_block(block_idx + 1)?,
                )),
            ))
        } else {
            Ok((block_idx, None))
        }
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        debug_assert!(self.is_valid(), "invalid iterator");
        self.block_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid(), "invalid iterator");
        self.block_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid(), "invalid iterator");
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            let (next_idx, opt_next_iter) =
                SsTableIterator::_maybe_next_block(&self.sstable, self.block_idx)?;
            self.block_idx = next_idx;
            if let Some(next_iter) = opt_next_iter {
                self.block_iter = next_iter;
            }
        }
        Ok(())
    }
}
