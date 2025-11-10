use {
  crate::engine::{
    iterator::Iterator,
    memtable::iterator::{MemtableIterator, MemtableIteratorBuilder},
  },
  bytes::Bytes,
  crossbeam_skiplist::SkipMap,
  std::{
    ops::Bound,
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
  },
};

pub mod iterator;

// A Memtable is the in-memory structure of the LSM storage engine.
pub struct Memtable {
  id: usize,

  skip_map: Arc<SkipMap<Bytes, Bytes>>,

  // Approxiamte size of the memtable (in bytes).
  approximate_size: Arc<AtomicUsize>,
}

impl Memtable {
  pub fn create(id: usize) -> Self {
    Self {
      id,
      skip_map: Arc::new(SkipMap::new()),
      approximate_size: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn get(&self, key: &[u8]) -> Option<Bytes> {
    let entry = self.skip_map.get(key)?;
    Some(entry.value().clone())
  }

  // Inserts the given key-value pair into the SkipMap.
  // If the key already exists, then its value gets overriden.
  pub fn put(&self, key: &[u8], value: &[u8]) {
    let _ = self
      .skip_map
      .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
  }

  // Returns a MemtableIterator.
  pub fn scan(&self, lower_bound: Bound<&[u8]>, upper_bound: Bound<&[u8]>) -> MemtableIterator {
    let lower_bound = convert_slice_to_bytes_bound(lower_bound);
    let upper_bound = convert_slice_to_bytes_bound(upper_bound);

    let mut memtable_iterator = MemtableIteratorBuilder {
      skip_map:                  self.skip_map.clone(),
      skip_map_iterator_builder: |skip_map| skip_map.range((lower_bound, upper_bound)),
      current_kv_pair:           (Bytes::new(), Bytes::new()),
    }
    .build();

    // Keep the cursor moved to the first key-value pair.
    let _ = memtable_iterator.next();

    memtable_iterator
  }

  pub fn get_approximate_size(&self) -> usize {
    self.approximate_size.load(Ordering::Relaxed)
  }
}

// Converts from Bound<&[u8]> to Bound<Bytes>.
fn convert_slice_to_bytes_bound(slice_bound: Bound<&[u8]>) -> Bound<Bytes> {
  match slice_bound {
    Bound::Included(bound) => Bound::Included(Bytes::copy_from_slice(bound)),
    Bound::Excluded(bound) => Bound::Excluded(Bytes::copy_from_slice(bound)),

    Bound::Unbounded => Bound::Unbounded,
  }
}
