use {
  crate::engine::{
    EngineConfig,
    iterator::{EngineIterator, fused_iterator::FusedIterator, merge_iterator::MergeIterator},
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

  mutable_memtable_freezer_lock: Mutex<()>,
  next_mutable_memtable_id:      AtomicUsize,
}

impl EngineCore {
  pub fn get(&self, key: &[u8]) -> Option<Bytes> {
    assert!(!key.is_empty(), "key cannot be empty");

    let state_read_lock_guard = self.state.read();
    let state = &state_read_lock_guard;

    // First search the mutable memtable.
    if let Some(value) = state.mutable_memtable.get(key) {
      if value.is_empty() {
        return None;
      }

      return Some(value);
    }

    // Search through the immutable memtables, one by one.
    for immutable_memtable in &state.immutable_memtables {
      if let Some(value) = immutable_memtable.get(key) {
        if value.is_empty() {
          return None;
        }

        return Some(value);
      }
    }

    drop(state_read_lock_guard);

    None
  }

  pub fn scan(
    &self,
    lower_bound: Bound<&[u8]>,
    upper_bound: Bound<&[u8]>,
  ) -> FusedIterator<EngineIterator> {
    let state_read_lock_guard = self.state.read();
    let state = Arc::clone(&state_read_lock_guard);

    // Since scan() may take good amount of time,
    // we want to drop the read lock sooner.
    // NOTE : But, state can be mutated from other threads, while scan() is running.
    //        Most probably, this problem will be addresses in the future.
    drop(state_read_lock_guard);

    let mut memtable_iterators = Vec::with_capacity(1 + state.immutable_memtables.len());

    memtable_iterators.push(state.mutable_memtable.scan(lower_bound, upper_bound));

    for immutable_memtable in state.immutable_memtables.iter() {
      memtable_iterators.push(immutable_memtable.scan(lower_bound, upper_bound));
    }

    let memtables_iterator = MergeIterator::new(memtable_iterators);

    let engine_iterator = EngineIterator::new(memtables_iterator);

    FusedIterator::new(engine_iterator)
  }

  pub fn put(&self, key: &[u8], value: &[u8]) {
    assert!(
      (!key.is_empty() && !value.is_empty()),
      "key and value cannot be empty"
    );

    let mutable_memtable_size_after_put: usize;

    // Insert the key-value pair into the mutable memtable.
    {
      let state_read_lock_guard = self.state.read();
      let state = &state_read_lock_guard;

      state.mutable_memtable.put(key, value);

      mutable_memtable_size_after_put = state.mutable_memtable.get_approximate_size();

      drop(state_read_lock_guard);
    }

    // If the mutable memtable has reached its size limit,
    // then we need to freeze it.
    self.try_freeze_mutable_memtable(mutable_memtable_size_after_put);
  }

  pub fn delete(&self, key: &[u8]) {
    assert!(!key.is_empty(), "key cannot be empty");

    let mutable_memtable_size_after_delete: usize;

    // Instead of actually deleting the key, we'll insert an empty value for it.
    // The entry is called a delete tombstone.
    {
      let state_read_lock_guard = self.state.read();
      let state = &state_read_lock_guard;

      state.mutable_memtable.put(key, &[]);

      mutable_memtable_size_after_delete = state.mutable_memtable.get_approximate_size();

      drop(state_read_lock_guard);
    }

    // If the mutable memtable has reached its size limit,
    // then we need to freeze it.
    self.try_freeze_mutable_memtable(mutable_memtable_size_after_delete);
  }
}

impl EngineCore {
  // Freezes the current mutable memtable,
  // if it has exceeded the size limit.
  fn try_freeze_mutable_memtable(&self, current_mutable_memtable_size: usize) {
    if current_mutable_memtable_size < self.config.sstable_size {
      return;
    }

    // TODO : Create a WAL (Write Ahead Log) file, which will be used by the new mutable memtable.

    // Take a mutable memtable freezer (MUTEX) lock.
    // The following section, thus, will be executed by only one thread at a time.
    let mutable_memtable_freezer_lock_guard = self.mutable_memtable_freezer_lock.lock();

    // The WAL (Write Ahead Log) file creation is an I/O operation, so it may take an indefinite
    // amount of time to complete.
    // Now, while this thread was stuck on creating the WAL file, suppose, another thread invoked
    // try_freeze() and finished the freezing process.
    // In that case, we don't want to freeze the current mutable memtable.
    {
      let state_read_lock_guard = self.state.read();
      let state = &state_read_lock_guard;

      if state.mutable_memtable.get_approximate_size() < self.config.sstable_size {
        return;
      }

      drop(state_read_lock_guard);
    }

    // Freeze the current mutable memtable.
    self.freeze_mutable_memtable(&mutable_memtable_freezer_lock_guard);

    drop(mutable_memtable_freezer_lock_guard);
  }

  // Freezes the current mutable memtable,
  // regardless of whether it has reached the size limit or not.
  fn freeze_mutable_memtable(&self, _mutable_memtable_freezer_lock_guard: &MutexGuard<()>) {
    // Create the new mutable memtable.
    let new_mutable_memtable_id = self
      .next_mutable_memtable_id
      .fetch_add(1, Ordering::Relaxed);
    let new_mutable_memtable = Arc::new(Memtable::create(new_mutable_memtable_id));

    // TODO : Understand why we clone the state, do our mutations and swap it with the old state,
    //        instead of just mutating the old state.
    {
      let mut state_write_lock_guard = self.state.write();

      // Create a new Engine_state by cloning the old Engine_state.
      let mut new_state = state_write_lock_guard.as_ref().clone();

      // Replace the old mutable memtable with the new mutable memtable.
      let old_mutable_memtable =
        mem::replace(&mut new_state.mutable_memtable, new_mutable_memtable);

      // Mark the old mutable memtable as frozen.
      new_state
        .immutable_memtables
        .insert(0, old_mutable_memtable);

      // Swap the old Engine_state with the new Engine_state.
      *state_write_lock_guard = Arc::new(new_state);

      drop(state_write_lock_guard)
    }
  }
}
