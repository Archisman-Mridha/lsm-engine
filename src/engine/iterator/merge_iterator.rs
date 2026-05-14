use {
  crate::engine::iterator::Iterator,
  std::{
    cmp::Ordering,
    collections::{BinaryHeap, binary_heap::PeekMut},
    mem
  }
};

// Merge the results returned by multiple (Memtable) iterators
// and return the latest version of each key to the user.
//
// We have assumed that lower is the index of an iterator, more recent is the data that
// it holds.
//
// NOTE : We want to avoid dynamic dispatch, so we use static dispatch using generics.
pub struct MergeIterator<I: Iterator> {
  // This is a binary min-heap.
  // So, iterator with the lowest head key value is first.
  // When multiple iterators have the same head key value, the newest one is first.
  binary_min_heap: BinaryHeap<BinaryMinHeapNodeData<I>>,

  popped_node: Option<BinaryMinHeapNodeData<I>>
}

impl<I: Iterator> MergeIterator<I> {
  pub fn new(iterators: Vec<I>) -> Self {
    // Filter out invalid iterators.
    let iterators: Vec<I> = iterators.into_iter().filter(|iterator| iterator.is_valid()).collect();

    let mut binary_min_heap = BinaryHeap::new();

    if iterators.is_empty() {
      return Self { binary_min_heap,
                    popped_node: None };
    }

    // Populate the binary min-heap.
    for (iterator_index, iterator) in iterators.into_iter().enumerate() {
      binary_min_heap.push(BinaryMinHeapNodeData { iterator_index,
                                                   iterator });
    }

    // Pop off the root node of the binary min-heap.
    let popped_node = binary_min_heap.pop().unwrap();

    Self { binary_min_heap,
           popped_node: Some(popped_node) }
  }
}

impl<I: Iterator> Iterator for MergeIterator<I> {
  fn key(&self) -> &[u8] {
    self.popped_node.as_ref().unwrap().iterator.key()
  }

  fn value(&self) -> &[u8] {
    self.popped_node.as_ref().unwrap().iterator.value()
  }

  fn next(&mut self) -> anyhow::Result<()> {
    let popped_node = self.popped_node.as_mut().unwrap();

    while let Some(mut child_node) = self.binary_min_heap.peek_mut() {
      if popped_node.iterator.key() == child_node.iterator.key() {
        // The popped node and this child node have the same key.
        //
        // And we know that the popped node has the latest value for that key.
        //
        // So, for the child node's iterator, we'll ignore the key-value pair it's cursor is
        // currently pointing to, and advance the cursor.
        // The binary heap will detect that the head key for that child node's iterator has
        // changed, and it'll reposition the child node if required.
        let result = child_node.iterator.next();

        // When trying to progress the cursor of that child node's iterator,
        // either error occurred, or the iterator is now pointing to invalid data.
        // So, we'll remove the iterator from the binary min-heap.
        match result {
          Ok(_) => {
            if !child_node.iterator.is_valid() {
              PeekMut::pop(child_node);
            }
          }

          Err(error) => {
            PeekMut::pop(child_node);
            return Err(error);
          }
        }
      } else {
        break;
      }
    }

    // Advance cursor of the popped node's iterator.
    popped_node.iterator.next()?;

    match popped_node.iterator.is_valid() {
      // Popped node's iterator became invalid.
      // Let's pop out the current root node of the binary min-heap.
      false => {
        let popped_root_node = self.binary_min_heap.pop();
        self.popped_node = popped_root_node;
      }

      // Swap the popped node and binary min-heap's current root node if necessary.
      // So, self.popped_node will always correspond to the key with the least priority.
      true =>
      {
        #[allow(clippy::collapsible_if)]
        if let Some(mut current_root_node) = self.binary_min_heap.peek_mut() {
          if *current_root_node < *popped_node {
            mem::swap(&mut *current_root_node, popped_node);
          }
        }
      }
    }

    Ok(())
  }

  fn is_valid(&self) -> bool {
    self.popped_node.as_ref().map(|popped_node| popped_node.iterator.is_valid()).unwrap_or(false)
  }
}

struct BinaryMinHeapNodeData<I: Iterator> {
  iterator_index: usize,
  iterator:       I
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

      Ordering::Equal => self.iterator_index.cmp(&other.iterator_index)
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
