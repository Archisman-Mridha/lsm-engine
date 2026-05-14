use crate::engine::iterator::Iterator;

// A safe wrapper around an iterator that implements the Iterator trait.
// It is similar to the FusedIterator provided by Rust's standard library :
// .next() continues to yield None, when the underlying iterator points to invalid data.
pub struct FusedIterator<I: Iterator> {
  iterator: I
}

impl<I: Iterator> FusedIterator<I> {
  pub fn new(iterator: I) -> Self {
    Self { iterator }
  }

  pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
    if !self.iterator.is_valid() {
      return None;
    }

    match self.iterator.next() {
      Ok(_) if self.iterator.is_valid() => {
        let kv_pair = (self.iterator.key(), self.iterator.value());
        Some(kv_pair)
      }

      _ => None
    }
  }
}
