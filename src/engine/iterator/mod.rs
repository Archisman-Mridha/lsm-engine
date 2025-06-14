use crate::engine::{
  iterator::merge_iterator::MergeIterator, memtable::iterator::MemtableIterator,
};

pub mod fused_iterator;
pub mod merge_iterator;

// A cursor based iterator.
pub trait Iterator {
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

pub type MemtablesIterator = MergeIterator<MemtableIterator>;

pub struct EngineIterator {
  memtablesIterator: MemtablesIterator,
}

impl EngineIterator {
  pub fn new(memtablesIterator: MemtablesIterator) -> Self {
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
