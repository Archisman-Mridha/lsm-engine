use {crate::engine::core::EngineCore, std::sync::Arc};

#[path = "./memtable/memtable.rs"]
mod memtable;

#[path = "./sstable/sstable.rs"]
mod sstable;

mod core;
pub mod iterator;
mod state;

pub struct EngineConfig {
  // SSTable size (in bytes).
  //
  // This decides the memtable size limit as well. When a mutable memtable becomes of this size,
  // it is frozen, and flushed to the disk in the form of an SSTable.
  sstable_size: usize,
}

pub struct Engine {
  core: Arc<EngineCore>,
}
