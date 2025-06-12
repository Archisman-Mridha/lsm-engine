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

  skipMap: Arc<SkipMap<Bytes, Bytes>>,

  // Approxiamte size of the memtable (in bytes).
  approximateSize: Arc<AtomicUsize>,
}

impl Memtable {
  pub fn create(id: usize) -> Self {
    Self {
      id,
      skipMap: Arc::new(SkipMap::new()),
      approximateSize: Arc::new(AtomicUsize::new(0)),
    }
  }

  pub fn get(&self, key: &[u8]) -> Option<Bytes> {
    let entry = self.skipMap.get(key)?;
    Some(entry.value().clone())
  }

  // Inserts the given key-value pair into the SkipMap.
  // If the key already exists, then its value gets overriden.
  pub fn put(&self, key: &[u8], value: &[u8]) {
    let _ = self
      .skipMap
      .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
  }

  // Returns a MemtableIterator.
  pub fn scan(&self, lowerBound: Bound<&[u8]>, upperBound: Bound<&[u8]>) -> MemtableIterator {
    let lowerBound = convertSliceToBytesBound(lowerBound);
    let upperBound = convertSliceToBytesBound(upperBound);

    let mut memtableIterator = MemtableIteratorBuilder {
      skipMap:                 self.skipMap.clone(),
      skipMapIterator_builder: |skipMap| skipMap.range((lowerBound, upperBound)),
      currentKVPair:           (Bytes::new(), Bytes::new()),
    }
    .build();

    // Keep the cursor moved to the first key-value pair.
    let _ = memtableIterator.next();

    memtableIterator
  }

  pub fn getApproximateSize(&self) -> usize {
    self.approximateSize.load(Ordering::Relaxed)
  }
}

// Converts from Bound<&[u8]> to Bound<Bytes>.
fn convertSliceToBytesBound(sliceBound: Bound<&[u8]>) -> Bound<Bytes> {
  match sliceBound {
    Bound::Included(bound) => Bound::Included(Bytes::copy_from_slice(bound)),
    Bound::Excluded(bound) => Bound::Excluded(Bytes::copy_from_slice(bound)),

    Bound::Unbounded => Bound::Unbounded,
  }
}
