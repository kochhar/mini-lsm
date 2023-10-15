use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::debug_assert;

use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        let mut current = None;

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            } else if heap.is_empty() {
                // If all iters have been invalid, keep the last one as current
                current = Some(HeapWrapper(0, iter));
            }
        }
        // If at least 1 iter was valid, make the smallest one current
        if !heap.is_empty() {
            current = Some(heap.pop().unwrap());
        }

         Self {
            iters: heap,
            current: current,
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        assert!(self.is_valid(), "invalid iterator");
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid(), "invalid iterator");
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            Some(w) => w.1.is_valid(),
            _ => false
        }
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();

        // Loop to advance past all the keys in the heap which are the
        // same as the current key
        while let Some(mut wrap_iter) = self.iters.peek_mut() {
            debug_assert!(
                wrap_iter.1.key() >= current.1.key(),
                "min-heap invariant violaed"
            );

            if wrap_iter.1.key() == current.1.key() {
                // If the key at the top of the heap is same as the current item
                // then advance the heap iterator. This leads to one of three
                // secnarios
                // a. there is an error advancing -- remove from heap and return
                // the error
                if let e @ Err(_) = wrap_iter.1.next() {
                    // PeekMut::pop pops the element without having to reorder
                    // the heap again
                    PeekMut::pop(wrap_iter);
                    return e;
                }

                // b. the iterator terminates becoming invalid -- remove from
                // heap and repeat the loop
                if !wrap_iter.1.is_valid() {
                    PeekMut::pop(wrap_iter);
                }

                // c. the iterator is valid -- repeat the loop
            } else {
                // If the key at the top of the heap is different from the current
                // break out of the loop
                break;
            }
        }

        // Advance the current iterator. This leads to one of two scenarios
        current.1.next()?;
        // a. The current iterator becomes invalid. Replace it by popping
        // from the top of the heap
        if !current.1.is_valid() {
            if let Some(wrap_iter) = self.iters.pop() {
                *current = wrap_iter;
            }
            return Ok(());
        }
        // b. The current iterator remains valid. Compare it with the top of the
        // heap and swap if current it bigger (sign used < because HeapWrapper
        // comparisons are inverted to work with a max heap)
        if let Some(mut wrap_iter) = self.iters.peek_mut() {
            if *current < *wrap_iter {
                std::mem::swap(&mut *wrap_iter, current);
            }
        }

        Ok(())
    }
}
