use std::debug_assert;

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(Self {
            a: a,
            b: b,
        })
    }

    fn prefer_a(&self) -> bool {
        if self.a.is_valid() & self.b.is_valid() {
            self.a.key() <= self.b.key()     
        } else {
            self.a.is_valid()
        }
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        debug_assert!(self.is_valid(), "invalid iterator");
        if self.prefer_a() {
            &self.a.key()[..]
        } else {
            &self.b.key()[..]
        }
    }

    fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid(), "invalid iterator");
        if self.prefer_a() {
            &self.a.value()[..]
        } else {
            &self.b.value()[..]
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // both iterators cannot be invalid
        debug_assert!(self.is_valid(), "invalid iterator");
        if !self.b.is_valid() {
            self.a.next()
        } else if !self.a.is_valid() {
            self.b.next()
        } else {
            if self.a.key() == self.b.key() {
                self.a.next()?;
                self.b.next()
            } else if self.a.key() < self.b.key() {
                self.a.next()
            } else { 
                self.b.next()
            }
        }
    }
}
