mod builder;
mod iterator;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use core::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

pub use builder::SsTableBuilder;
pub use iterator::SsTableIterator;

use crate::block::{Block, SIZEOF_U32, SIZEOF_U64};
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same
    /// buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let original_len = buf.len();
        let estimated_extra = BlockMeta::estimate_size(block_meta);
        // Reserve space before putting
        buf.reserve(estimated_extra);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
        let mut hasher = DefaultHasher::new();
        block_meta.hash(&mut hasher);
        buf.put_u64(hasher.finish());

        assert_eq!(estimated_extra, buf.len() - original_len);
        println!("Encoded block meta, meta_size: {:?}", estimated_extra);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        println!("Decoding block, buf remain: {:?}", buf.remaining());
        while buf.remaining() > SIZEOF_U64 {
            let offset = buf.get_u32() as usize;
            println!("Read offset, buf remain: {:?}", buf.remaining());
            let first_key_len = buf.get_u16() as usize;
            println!("Read first_key_len, buf remain: {:?}", buf.remaining());
            let first_key = buf.copy_to_bytes(first_key_len);
            println!("Read first_key, buf remain: {:?}", buf.remaining());
            block_meta.push(BlockMeta { offset, first_key });
        }
        let meta_checksum = buf.get_u64();

        let mut hasher = DefaultHasher::new();
        block_meta.hash(&mut hasher);
        assert_eq!(
            meta_checksum,
            hasher.finish(),
            "extracted hash != hash(block_meta)"
        );

        block_meta
    }

    fn estimate_size(block_meta: &[BlockMeta]) -> usize {
        let mut estimate = 0;
        for meta in block_meta {
            // For the offset
            estimate += std::mem::size_of::<u32>();
            // For size of the first key
            estimate += std::mem::size_of::<u16>();
            // For the bytes of the first key
            estimate += meta.first_key.len();
        }
        // For the checksum
        estimate += SIZEOF_U64;
        estimate
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(_path: &Path, data: Vec<u8>) -> Result<Self> {
        Ok(FileObject(data.into()))
    }

    pub fn open(_path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

/// -----------------------------------------------------------------------------------------------------------------------------------
/// |              Data Block             |                           Meta Block                            |          Extra          |
/// -----------------------------------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Checksum (u64) | Meta Block Offset (u32) |
/// -----------------------------------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(
        _id: usize,
        _block_cache: Option<Arc<BlockCache>>,
        file: FileObject,
    ) -> Result<Self> {
        println!("Opening sst, file size: {:?}", file.size());
        let meta_offset_start = file.size() - SIZEOF_U32;
        let mut meta_offset_buf: &[u8] = &file.read(meta_offset_start, SIZEOF_U32)?;
        let meta_start = meta_offset_buf.get_u32() as u64;

        let meta_len = meta_offset_start - meta_start;
        let block_meta_buf: &[u8] = &file.read(meta_start, meta_len)?;

        Ok(SsTable {
            file,
            block_metas: BlockMeta::decode_block_meta(block_meta_buf),
            block_meta_offset: meta_start as usize,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        println!("Loading block with index {:?}", block_idx);
        let block_metas = &self.block_metas;
        assert!(block_idx < block_metas.len());

        let block_offset = block_metas[block_idx].offset as u64;
        println!("Block offset is {:?}", block_offset);
        let block_end = if block_idx + 1 < block_metas.len() {
            block_metas[block_idx + 1].offset
        } else {
            self.block_meta_offset
        };
        let block_len = block_end as u64 - block_offset;
        println!("Block length is {:?}", block_len);

        let block_data = self.file.read(block_offset, block_len)?;
        Ok(Arc::new(Block::decode(block_data.as_ref())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, _block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let mut low = 0;
        let mut high = self.block_metas.len();
        while low < high {
            let mid = (low + high) / 2;
            let curr_key = self.block_metas[mid].first_key.as_ref();
            println!("Comparing key: mid:({:?}){:?} and {:?}", mid, curr_key, key);
            match curr_key.cmp(key) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return mid,
            }
        }
        // Since we'll find the block with first_key > key, we need to subtract 1.
        low.saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
