use crate::engine::{
  iterator::merge_iterator::MergeIterator, memtable::iterator::MemtableIterator,
};

// A cursor based iterator.
pub(crate) trait Iterator {
  // Returns the key of the key-value pair, the cursor is currently pointing to.
  fn key(&self) -> &[u8];

  // Returns the value of the key-value pair, the cursor is currently pointing to.
  fn value(&self) -> &[u8];

  // Tries to move the cursor to the next key-value pair.
  //
  // SAFETY : After each call to next() (even if it succeeds without an error), the implementor is
  //          responsible for updating the internal state so that isValid() correctly reflects
  //          whether the new cursor position points to a valid key-value pair or not.
  fn next(&mut self) -> anyhow::Result<()>;

  // Returns whether the iterator is pointing to a valid key-value pair or not.
  fn isValid(&self) -> bool;
}

// A safe wrapper around an iterator that implements the Iterator trait.
// It is similar to the FusedIterator provided by Rust's standard library :
// .next() continues to yield None, when the underlying iterator points to invalid data.
pub(crate) struct FusedIterator<I: Iterator> {
  iterator: I,
}

impl<I: Iterator> FusedIterator<I> {
  pub fn new(iterator: I) -> Self {
    Self { iterator }
  }

  pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
    if !self.iterator.isValid() {
      return None;
    }

    match self.iterator.next() {
      Ok(_) if self.iterator.isValid() => {
        let kvPair = (self.iterator.key(), self.iterator.value());
        Some(kvPair)
      }

      _ => None,
    }
  }
}

pub(crate) mod merge_iterator {
  use {
    super::*,
    std::{
      cmp::Ordering,
      collections::{BinaryHeap, binary_heap::PeekMut},
      mem,
    },
  };

  // Merge the results returned by multiple (Memtable) iterators
  // and return the latest version of each key to the user.
  //
  // We have assumed that lower is the index of an iterator, more recent is the data that
  // it holds.
  //
  // NOTE : We want to avoid dynamic dispatch, so we use static dispatch using generics.
  pub(crate) struct MergeIterator<I: Iterator> {
    binaryMinHeap: BinaryHeap<BinaryMinHeapNodeData<I>>,
    poppedNode:    Option<BinaryMinHeapNodeData<I>>,
  }

  impl<I: Iterator> MergeIterator<I> {
    pub fn new(iterators: Vec<I>) -> Self {
      // Filter out invalid iterators.
      let iterators: Vec<I> = iterators
        .into_iter()
        .filter(|iterator| iterator.isValid())
        .collect();

      let mut binaryMinHeap = BinaryHeap::new();

      if iterators.is_empty() {
        return Self {
          binaryMinHeap,
          poppedNode: None,
        };
      }

      // Populate the binary min-heap.
      for (iteratorIndex, iterator) in iterators.into_iter().enumerate() {
        binaryMinHeap.push(BinaryMinHeapNodeData {
          iteratorIndex,
          iterator,
        });
      }

      // Pop off the root node of the binary min-heap.
      let poppedNode = binaryMinHeap.pop().unwrap();

      Self {
        binaryMinHeap,
        poppedNode: Some(poppedNode),
      }
    }
  }

  impl<I: Iterator> Iterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
      self.poppedNode.as_ref().unwrap().iterator.key()
    }

    fn value(&self) -> &[u8] {
      self.poppedNode.as_ref().unwrap().iterator.value()
    }

    fn next(&mut self) -> anyhow::Result<()> {
      let poppedNode = self.poppedNode.as_mut().unwrap();

      while let Some(mut childNode) = self.binaryMinHeap.peek_mut() {
        if poppedNode.iterator.key() == childNode.iterator.key() {
          // The popped node and this child node have the same key.
          //
          // And we know that the popped node has the latest value for that key.
          //
          // So, for the child node's iterator, we'll ignore the key-value pair it's cursor is
          // currently pointing to, and advance the cursor.
          // The binary heap will detect that the head key for that child node's iterator has
          // changed, and it'll reposition the child node if required.
          let result = childNode.iterator.next();

          // When trying to progress the cursor of that child node's iterator,
          // either error occurred, or the iterator is now pointing to invalid data.
          // So, we'll remove the iterator from the binary min-heap.
          match result {
            Ok(_) => {
              if childNode.iterator.isValid() {
                PeekMut::pop(childNode);
              }
            }

            Err(error) => {
              PeekMut::pop(childNode);
              return Err(error);
            }
          }
        }
        else {
          break;
        }
      }

      // Advance cursor of the popped node's iterator.
      poppedNode.iterator.next()?;

      match poppedNode.iterator.isValid() {
        // Popped node's iterator became invalid.
        // Let's pop out the current root node of the binary min-heap.
        false => {
          let poppedRootNode = self.binaryMinHeap.pop();
          self.poppedNode = poppedRootNode;
        }

        // Swap the popped node and binary min-heap's current root node if necessary.
        // So, self.poppedNode will always correspond to the key with the least priority.
        true =>
        {
          #[allow(clippy::collapsible_if)]
          if let Some(mut currentRootNode) = self.binaryMinHeap.peek_mut() {
            if *currentRootNode < *poppedNode {
              mem::swap(&mut *currentRootNode, poppedNode);
            }
          }
        }
      }

      Ok(())
    }

    fn isValid(&self) -> bool {
      self
        .poppedNode
        .as_ref()
        .map(|poppedNode| poppedNode.iterator.isValid())
        .unwrap_or(false)
    }
  }

  // This is a binary min-heap.
  // So, iterator with the lowest head key value is first.
  // When multiple iterators have the same head key value, the newest one is first.
  struct BinaryMinHeapNodeData<I: Iterator> {
    iteratorIndex: usize,
    iterator:      I,
  }

  impl<I: Iterator> PartialOrd for BinaryMinHeapNodeData<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
      self.cmp(other).into()
    }
  }

  impl<I: Iterator> Ord for BinaryMinHeapNodeData<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
      match self.iterator.key().cmp(other.iterator.key()) {
        Ordering::Greater => Ordering::Greater,
        Ordering::Less => Ordering::Less,

        Ordering::Equal => self.iteratorIndex.cmp(&other.iteratorIndex),
      }
      // NOTE : Since, the standard library's BinaryHeap is a max-heap,
      //        we reverse the ordering, to get a min-heap.
      .reverse()
    }
  }

  impl<I: Iterator> PartialEq for BinaryMinHeapNodeData<I> {
    fn eq(&self, other: &Self) -> bool {
      self.partial_cmp(other).unwrap() == Ordering::Equal
    }
  }

  impl<I: Iterator> Eq for BinaryMinHeapNodeData<I> {}
}

pub(crate) type MemtablesIterator = MergeIterator<MemtableIterator>;

pub(crate) struct EngineIterator {
  memtablesIterator: MemtablesIterator,
}

impl EngineIterator {
  pub(crate) fn new(memtablesIterator: MemtablesIterator) -> Self {
    Self { memtablesIterator }
  }
}

impl Iterator for EngineIterator {
  fn key(&self) -> &[u8] {
    self.memtablesIterator.key()
  }

  fn value(&self) -> &[u8] {
    self.memtablesIterator.value()
  }

  fn next(&mut self) -> anyhow::Result<()> {
    self.memtablesIterator.next()?;

    // We want to ignore delete tombstones.
    while self.isValid() && self.value().is_empty() {
      self.memtablesIterator.next()?;
    }

    Ok(())
  }

  fn isValid(&self) -> bool {
    self.memtablesIterator.isValid()
  }
}
