mod builder;
mod iterator;

use bytes::{Buf, BufMut, Bytes};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
pub use iterator::BlockIterator;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub const SIZEOF_U32: u64 = std::mem::size_of::<u32>() as u64;
pub const SIZEOF_U64: usize = std::mem::size_of::<u64>();

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// -----------------------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |            Extra             |
/// -----------------------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements | block hash |
/// -----------------------------------------------------------------------------------------------------------------
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        let num_offsets = self.offsets.len();
        buf.put_u16(num_offsets as u16);

        let checksum = self.hash_default();
        buf.put_u64(checksum);
        buf.into()
    }

    /// Decode from the block layout, transform the input `block` to a single `Block`
    pub fn decode(block: &[u8]) -> Self {
        // ********** Get the block hash **********
        // Last value in the block is the hash as a u64
        let checksum_start = block.len() - SIZEOF_U64;
        // Take final part of block and interpret as u64 to get block hash
        let checksum = (&block[checksum_start..]).get_u64();

        // ********** Get the number of offsets in the block **********
        // Penultimate value in block is the number of offsets as a u16.
        let num_offsets_start = checksum_start - SIZEOF_U16;
        // Take suffix of block and interpret it as u16 to get number of offsets
        let num_offsets = (&block[num_offsets_start..checksum_start]).get_u16() as usize;

        // ********** Get offsets between offsets_start..num_offsets_start **********
        // Get start of offsets by subtracting (num_offsets * u16) from num_offsets_start
        let offsets_start = num_offsets_start - (num_offsets * SIZEOF_U16);
        // Offsets are everything from offsets_start to num_offsets_start
        let offsets_raw = &block[offsets_start..num_offsets_start];
        // Process the offsets in chunks of SIZEOF_U16 converting to u16 values
        let offsets: Vec<_> = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        assert_eq!(num_offsets, offsets.len(), "num_offsets != offsets.len()");

        // ********** Get the data from beginning of the block **********
        // data is everything from start till offsets_start
        let data = block[0..offsets_start].to_vec();

        let decoded = Self { data, offsets };
        assert_eq!(
            checksum,
            decoded.hash_default(),
            "extracted hash != hash(decoded block)"
        );
        decoded
    }

    fn hash_default(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.data.hash(hasher);
        self.offsets.hash(hasher);
    }
}

#[cfg(test)]
mod tests;
