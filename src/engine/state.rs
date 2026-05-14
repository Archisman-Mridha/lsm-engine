use {crate::engine::memtable::Memtable, std::sync::Arc};

#[derive(Clone)]
pub struct EngineState {
  pub(super) mutable_memtable:    Arc<Memtable>,
  pub(super) immutable_memtables: Vec<Arc<Memtable>>
}
