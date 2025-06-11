use {
  bytes::Bytes,
  crossbeam_skiplist::SkipMap,
  std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

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

  pub fn getApproximateSize(&self) -> usize {
    self.approximateSize.load(Ordering::Relaxed)
  }
}
