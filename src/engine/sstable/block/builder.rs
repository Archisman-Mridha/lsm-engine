use {
  crate::engine::sstable::block::{Block, U16_SIZE},
  anyhow::bail,
  bytes::BufMut,
};

pub struct BlockBuilder {
  rawEntries:   Vec<u8>,
  offsets:      Vec<u16>,
  maxBlockSize: usize,
}

impl BlockBuilder {
  pub fn new(maxBlockSize: usize) -> Self {
    Self {
      rawEntries: Vec::new(),
      offsets: Vec::new(),
      maxBlockSize,
    }
  }

  pub fn currentBlockSize(&self) -> usize {
    self.rawEntries.len() + (self.offsets.len() * U16_SIZE) + (/* entry count */U16_SIZE)
  }

  #[must_use = "atleast one key-value pair must be inserted into the block"]
  pub fn insertKVPair(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
    // Calculate what the block size will be after we insert the entry.
    let postInsertionBlockSize =
      self.currentBlockSize() + ((U16_SIZE + key.len()) + (U16_SIZE + value.len()) + U16_SIZE);

    // If the post insertion block size is greater than the allowed max block size,
    // then we cannot insert the entry.
    if postInsertionBlockSize > self.maxBlockSize {
      bail!("Inserting this KV pair will violate the maximum block size constraint.");
    }

    // Insert the entry into the block.

    let rawEntryOffset = self.rawEntries.len();
    self.offsets.push(rawEntryOffset as u16);

    self.rawEntries.put_u16(key.len() as u16);
    self.rawEntries.put(key);

    self.rawEntries.put_u16(value.len() as u16);
    self.rawEntries.put(value);

    Ok(())
  }

  pub fn build(self) -> Block {
    if self.rawEntries.is_empty() {
      panic!("Block has no entries. Consider an increased maximum block size.");
    }

    Block {
      rawEntries: self.rawEntries,
      offsets:    self.offsets,
    }
  }
}
