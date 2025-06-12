use {
  crate::engine::{
    EngineConfig,
    iterator::{EngineIterator, FusedIterator, merge_iterator::MergeIterator},
    memtable::Memtable,
    state::EngineState,
  },
  bytes::Bytes,
  parking_lot::{Mutex, MutexGuard, RwLock},
  std::{
    mem,
    ops::Bound,
    sync::{
      Arc,
      atomic::{AtomicUsize, Ordering},
    },
  },
};

pub struct EngineCore {
  config: EngineConfig,

  state: Arc<RwLock<Arc<EngineState>>>,

  mutableMemtableFreezerLock: Mutex<()>,
  nextMutableMemtableID:      AtomicUsize,
}

impl EngineCore {
  pub fn get(&self, key: &[u8]) -> Option<Bytes> {
    assert!(!key.is_empty(), "key cannot be empty");

    let stateReadLockGuard = self.state.read();
    let state = &stateReadLockGuard;

    // First search the mutable memtable.
    if let Some(value) = state.mutableMemtable.get(key) {
      if value.is_empty() {
        return None;
      }

      return Some(value);
    }

    // Search through the immutable memtables, one by one.
    for immutableMemtable in &state.immutableMemtables {
      if let Some(value) = immutableMemtable.get(key) {
        if value.is_empty() {
          return None;
        }

        return Some(value);
      }
    }

    drop(stateReadLockGuard);

    None
  }

  pub fn scan(
    &self,
    lowerBound: Bound<&[u8]>,
    upperBound: Bound<&[u8]>,
  ) -> FusedIterator<EngineIterator> {
    let stateReadLockGuard = self.state.read();
    let state = Arc::clone(&stateReadLockGuard);

    // Since scan() may take good amount of time,
    // we want to drop the read lock sooner.
    // NOTE : But, state can be mutated from other threads, while scan() is running.
    //        Most probably, this problem will be addresses in the future.
    drop(stateReadLockGuard);

    let mut memtableIterators = Vec::with_capacity(1 + state.immutableMemtables.len());

    memtableIterators.push(state.mutableMemtable.scan(lowerBound, upperBound));

    for immutableMemtable in state.immutableMemtables.iter() {
      memtableIterators.push(immutableMemtable.scan(lowerBound, upperBound));
    }

    let memtablesIterator = MergeIterator::new(memtableIterators);

    let engineIterator = EngineIterator::new(memtablesIterator);

    FusedIterator::new(engineIterator)
  }

  pub fn put(&self, key: &[u8], value: &[u8]) {
    assert!(
      (!key.is_empty() && !value.is_empty()),
      "key and value cannot be empty"
    );

    let mutableMemtableSizeAfterPut: usize;

    // Insert the key-value pair into the mutable memtable.
    {
      let stateReadLockGuard = self.state.read();
      let state = &stateReadLockGuard;

      state.mutableMemtable.put(key, value);

      mutableMemtableSizeAfterPut = state.mutableMemtable.getApproximateSize();

      drop(stateReadLockGuard);
    }

    // If the mutable memtable has reached its size limit,
    // then we need to freeze it.
    self.tryFreezeMutableMemtable(mutableMemtableSizeAfterPut);
  }

  pub fn delete(&self, key: &[u8]) {
    assert!(!key.is_empty(), "key cannot be empty");

    let mutableMemtableSizeAfterDelete: usize;

    // Instead of actually deleting the key, we'll insert an empty value for it.
    // The entry is called a delete tombstone.
    {
      let stateReadLockGuard = self.state.read();
      let state = &stateReadLockGuard;

      state.mutableMemtable.put(key, &[]);

      mutableMemtableSizeAfterDelete = state.mutableMemtable.getApproximateSize();

      drop(stateReadLockGuard);
    }

    // If the mutable memtable has reached its size limit,
    // then we need to freeze it.
    self.tryFreezeMutableMemtable(mutableMemtableSizeAfterDelete);
  }
}

impl EngineCore {
  // Freezes the current mutable memtable,
  // if it has exceeded the size limit.
  fn tryFreezeMutableMemtable(&self, currentMutableMemtableSize: usize) {
    if currentMutableMemtableSize < self.config.sstableSize {
      return;
    }

    // TODO : Create a WAL (Write Ahead Log) file, which will be used by the new mutable memtable.

    // Take a mutable memtable freezer (MUTEX) lock.
    // The following section, thus, will be executed by only one thread at a time.
    let mutableMemtableFreezerLockGuard = self.mutableMemtableFreezerLock.lock();

    // The WAL (Write Ahead Log) file creation is an I/O operation, so it may take an indefinite
    // amount of time to complete.
    // Now, while this thread was stuck on creating the WAL file, suppose, another thread invoked
    // tryFreeze() and finished the freezing process.
    // In that case, we don't want to freeze the current mutable memtable.
    {
      let stateReadLockGuard = self.state.read();
      let state = &stateReadLockGuard;

      if state.mutableMemtable.getApproximateSize() < self.config.sstableSize {
        return;
      }

      drop(stateReadLockGuard);
    }

    // Freeze the current mutable memtable.
    self.freezeMutableMemtable(&mutableMemtableFreezerLockGuard);

    drop(mutableMemtableFreezerLockGuard);
  }

  // Freezes the current mutable memtable,
  // regardless of whether it has reached the size limit or not.
  fn freezeMutableMemtable(&self, _mutableMemtableFreezerLockGuard: &MutexGuard<()>) {
    // Create the new mutable memtable.
    let newMutableMemtableID = self.nextMutableMemtableID.fetch_add(1, Ordering::Relaxed);
    let newMutableMemtable = Arc::new(Memtable::create(newMutableMemtableID));

    {
      let mut stateWriteLockGuard = self.state.write();

      // Create a new EngineState by cloning the old EngineState.
      let mut newState = stateWriteLockGuard.as_ref().clone();

      // Replace the old mutable memtable with the new mutable memtable.
      let oldMutableMemtable = mem::replace(&mut newState.mutableMemtable, newMutableMemtable);

      // Mark the old mutable memtable as frozen.
      newState.immutableMemtables.insert(0, oldMutableMemtable);

      // Swap the old EngineState with the new EngineState.
      *stateWriteLockGuard = Arc::new(newState);

      drop(stateWriteLockGuard)
    }
  }
}
