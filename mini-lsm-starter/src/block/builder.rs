use super::{Block, SIZEOF_U16};
use bytes::BufMut;

/// Builds a block.
pub struct BlockBuilder {
    // All the key-value pairs in the block
    data: Vec<u8>,
    // The offsets of each key-value pair in the block
    offsets: Vec<u16>,
    // The current expected block size
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            block_size,
        }
    }

    fn estimated_size(&self) -> usize {
        self.data.len() + (self.offsets.len() * SIZEOF_U16) + SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key cannot be empty!");
        // Extra 3 U16s come from the key.len() & value.len() which go into
        //  data and the new offset going into offsets.
        let new_size = self.estimated_size() + key.len() + value.len() + (3 * SIZEOF_U16);
        if new_size > self.block_size && !self.is_empty() {
            return false;
        }

        // The offsets are updated first
        self.offsets.push(self.data.len() as u16); // 1 U16 above
        self.data.put_u16(key.len() as u16); // 1 U16 above
        self.data.put(key); // key
        self.data.put_u16(value.len() as u16); // 1 U16 above
        self.data.put(value); // value
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block cannot be built when empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
