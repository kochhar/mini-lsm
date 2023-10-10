use bytes::Buf;
use std::cmp::Ordering;
use std::{debug_assert, sync::Arc};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Vec<u8>,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1)
    }

    // Seeks to idx-th key in the block
    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            self.idx = std::usize::MAX;
            return;
        }
        self.idx = idx;
        self.seek_to_offset(self.block.offsets[idx].into());
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        if self.is_valid() && self.key().cmp(key) == Ordering::Equal {
            return;
        }
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = (low + high) / 2;
            self.seek_to_idx(mid);
            assert!(self.is_valid());

            match self.key().cmp(key) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return,
            }
        }
        self.seek_to_idx(low);
    }

    // Seek to the entry found at offset in the block's data
    fn seek_to_offset(&mut self, offset: usize) {
        let entry = &self.block.data[offset..];

        // `get_u16` moves the ptr 2 bytes ahead so we don't need to manually
        // advance it
        // let key_len = entry.get_u16() as usize;
        let key_len = (&entry[0..2]).get_u16() as usize;
        // let key = entry[..key_len].to_vec();
        let key = entry[2..2 + key_len].to_vec();
        self.key.clear();
        self.key.extend(key);
        // // key was read as a slice, so manually advance
        // entry.advance(key_len);

        // // Again, manual advance not needed after `get_u16`
        // let value_len = entry.get_u16() as usize;
        let value_len = (&entry[2 + key_len..4 + key_len]).get_u16() as usize;
        let value = entry[4 + key_len..4 + key_len + value_len].to_vec();
        // let value = entry[..value_len].to_vec();
        self.value.clear();
        self.value.extend(value);
        // // value was read aa a slice, so manually advance
        // entry.advance(value_len);
    }
}
