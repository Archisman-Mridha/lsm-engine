use {crate::engine::memtable::Memtable, std::sync::Arc};

#[derive(Clone)]
pub struct EngineState {
  pub(super) mutableMemtable:    Arc<Memtable>,
  pub(super) immutableMemtables: Vec<Arc<Memtable>>,
}
