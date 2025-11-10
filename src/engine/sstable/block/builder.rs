use {
  crate::engine::sstable::block::{Block, U16_SIZE},
  anyhow::bail,
  bytes::BufMut,
};

pub struct BlockBuilder {
  raw_entries:    Vec<u8>,
  offsets:        Vec<u16>,
  max_block_size: usize,
}

impl BlockBuilder {
  pub fn new(max_block_size: usize) -> Self {
    Self {
      raw_entries: Vec::new(),
      offsets: Vec::new(),
      max_block_size,
    }
  }

  pub fn current_block_size(&self) -> usize {
    self.raw_entries.len() + (self.offsets.len() * U16_SIZE) + (/* entry count */U16_SIZE)
  }

  #[must_use = "atleast one key-value pair must be inserted into the block"]
  pub fn insert_kv_pair(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
    // Calculate what the block size will be after we insert the entry.
    let post_insertion_block_size =
      self.current_block_size() + ((U16_SIZE + key.len()) + (U16_SIZE + value.len()) + U16_SIZE);

    // If the post insertion block size is greater than the allowed max block size,
    // then we cannot insert the entry.
    if post_insertion_block_size > self.max_block_size {
      bail!("Inserting this KV pair will violate the maximum block size constraint.");
    }

    // Insert the entry into the block.

    let raw_entry_offset = self.raw_entries.len();
    self.offsets.push(raw_entry_offset as u16);

    self.raw_entries.put_u16(key.len() as u16);
    self.raw_entries.put(key);

    self.raw_entries.put_u16(value.len() as u16);
    self.raw_entries.put(value);

    Ok(())
  }

  pub fn build(self) -> Block {
    if self.raw_entries.is_empty() {
      panic!("Block has no entries. Consider an increased maximum block size.");
    }

    Block {
      raw_entries: self.raw_entries,
      offsets:     self.offsets,
    }
  }
}
