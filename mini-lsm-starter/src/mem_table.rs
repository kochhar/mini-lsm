#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let (lower, upper) = (map_bound(lower), map_bound(upper));
        let mut iter = MemTableIterator {
            map: self.map.clone(),
            iter: self.map.range((lower, upper)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        };
        let _ = iter.next();
        iter
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        let mut iter = self.scan(Bound::Unbounded, Bound::Unbounded);
        while iter.is_valid() {
            builder.add(iter.key(), iter.value());
            iter.next()?;
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
//#[self_referencing]
pub struct MemTableIterator<'a> {
    map: Arc<SkipMap<Bytes, Bytes>>,
    //    #[borrows(map)]
    //    #[not_covariant]
    iter: crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>,
    item: (Bytes, Bytes),
}

impl<'a> MemTableIterator<'a> {
    fn entry_to_item(entry: &Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        match entry {
            Some(entry) => (entry.key().clone(), entry.value().clone()),
            _ => (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }
    }
}

impl StorageIterator for MemTableIterator<'_> {
    fn value(&self) -> &[u8] {
        &self.item.1[..]
    }

    fn key(&self) -> &[u8] {
        &self.item.0[..]
    }

    fn is_valid(&self) -> bool {
        !&self.item.0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.iter.next();
        println!("StorageIterator next entry {:?}", entry);
        self.item = MemTableIterator::entry_to_item(&entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
