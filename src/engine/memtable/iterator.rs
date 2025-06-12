use {
  crate::engine::iterator::Iterator,
  bytes::Bytes,
  crossbeam_skiplist::SkipMap,
  ouroboros::self_referencing,
  std::{ops::Bound, sync::Arc},
};

type SkipMapRangeIterator<'a> =
  crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing(pub_extras)]
pub(crate) struct MemtableIterator {
  // Instead of doing "skipMap: &'skipMap SkipMap<Bytes, Bytes>", we do the following,
  // to avoid complications coming with the lifetime usage. This will also improve the
  // compile time.
  pub(super) skipMap: Arc<SkipMap<Bytes, Bytes>>,

  #[borrows(skipMap)]
  // TODO : Understand why we mark this as not covariant.
  #[not_covariant]
  pub(super) skipMapIterator: SkipMapRangeIterator<'this>,

  // key-value pair the iterator's cursor is currently pointing to.
  //
  // If the key and value are empty bytes, that means the iterator is invalid.
  pub(super) currentKVPair: (Bytes, Bytes),
}

impl Iterator for MemtableIterator {
  fn key(&self) -> &[u8] {
    &self.borrow_currentKVPair().0
  }

  fn value(&self) -> &[u8] {
    &self.borrow_currentKVPair().1
  }

  fn next(&mut self) -> anyhow::Result<()> {
    let kvPair = self.with_skipMapIterator_mut(|skipMapIterator| {
      skipMapIterator
        .next()
        .map(|skipMapEntry| {
          // Convert the SkipMap entry to a key-value pair.
          (skipMapEntry.key().clone(), skipMapEntry.value().clone())
        })
        .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    });

    self.with_mut(|memtableIterator| {
      *memtableIterator.currentKVPair = kvPair;
    });

    Ok(())
  }

  fn isValid(&self) -> bool {
    !self.borrow_currentKVPair().0.is_empty()
  }
}
