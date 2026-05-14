use {
  crate::engine::iterator::Iterator,
  bytes::Bytes,
  crossbeam_skiplist::SkipMap,
  ouroboros::self_referencing,
  std::{ops::Bound, sync::Arc}
};

type SkipMapRangeIterator<'a> =
  crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing(pub_extras)]
pub struct MemtableIterator {
  // Instead of doing "skip_map: &'skip_map SkipMap<Bytes, Bytes>", we do the following,
  // to avoid complications coming with the lifetime usage. This will also improve the
  // compile time.
  pub skip_map: Arc<SkipMap<Bytes, Bytes>>,

  #[borrows(skip_map)]
  // TODO : Understand why we mark this as not covariant.
  #[not_covariant]
  pub skip_map_iterator: SkipMapRangeIterator<'this>,

  // key-value pair the iterator's cursor is currently pointing to.
  // When the key and value are empty bytes, that means the iterator is invalid.
  pub current_kv_pair: (Bytes, Bytes)
}

impl Iterator for MemtableIterator {
  fn key(&self) -> &[u8] {
    &self.borrow_current_kv_pair().0
  }

  fn value(&self) -> &[u8] {
    &self.borrow_current_kv_pair().1
  }

  fn next(&mut self) -> anyhow::Result<()> {
    let kv_pair = self.with_skip_map_iterator_mut(|skip_map_iterator| {
                        skip_map_iterator.next()
                                         .map(|skip_map_entry| {
                                           // Convert the SkipMap entry to a key-value pair.
                                           (skip_map_entry.key().clone(),
                                            skip_map_entry.value().clone())
                                         })
                                         .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
                      });

    self.with_mut(|memtable_iterator| {
          *memtable_iterator.current_kv_pair = kv_pair;
        });

    Ok(())
  }

  fn is_valid(&self) -> bool {
    !self.borrow_current_kv_pair().0.is_empty()
  }
}
