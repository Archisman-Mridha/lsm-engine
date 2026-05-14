use {
  crate::engine::{
    iterator::Iterator,
    sstable::block::{Block, U16_SIZE}
  },
  bytes::Buf,
  std::{cmp::Ordering, sync::Arc}
};

pub struct BlockIterator {
  block: Arc<Block>,

  // Whetever entry the cursor of this iterator is currently pointing to,
  // this is the index of that entry in self.block.raw_entries.
  current_index:       usize,
  //
  // This is the key of that entry.
  current_key:         Vec<u8>,
  //
  // And this is the byte range of the value of that entry in self.block.raw_entries.
  current_value_range: (usize, usize)
}

impl BlockIterator {
  pub fn new(block: Arc<Block>) -> Self {
    Self { block,

           current_index: 0,
           current_key: Vec::new(),
           current_value_range: (0, 0) }
  }

  // Seeks to the first key >= the given target key.
  pub fn seek_to_key(&mut self, target_key: &Vec<u8>) {
    // Use Binary Search to find the index of the key >= the given key,
    // in self.block.raw_entries.

    let mut lower_index = 0;
    let mut upper_index = self.block.raw_entries.len();

    while lower_index < upper_index {
      let mid_index = lower_index + ((lower_index + upper_index) / 2);

      self.seek_to_entry_with_index(mid_index);

      match self.current_key.cmp(target_key) {
        Ordering::Equal => return,

        Ordering::Less => upper_index = mid_index - 1,
        Ordering::Greater => lower_index = mid_index + 1
      }
    }

    // At this point lower_index = higherIndex.
    self.seek_to_entry_with_index(lower_index);
  }

  // Seeks to the entry with the given index.
  pub fn seek_to_entry_with_index(&mut self, index: usize) {
    if index >= self.block.raw_entries.len() {
      self.current_index = 0;
      self.current_key = Vec::new();
      self.current_value_range = (0, 0);
    }

    let offset = self.block.offsets[index];
    self.seek_to_offset(offset);
  }

  // Seeks to the entry at the given byte offset in self.raw_entries.
  pub fn seek_to_offset(&mut self, offset: u16) {
    let mut raw_entry = &self.block.raw_entries[offset as usize..];

    let key_size = raw_entry.get_u16() as usize;
    let key = raw_entry[..key_size].to_vec();
    //
    // Yes, we do allocate a Vec for 'key'. But that's very short lived.
    // Doing 'self.current_key.extend(key)', we reuse the allocated heap memory for
    // self.current_key.
    // NOTE : Doing 'self.current_key = key', won't lead to a memory leak.
    //
    self.current_key.clear();
    self.current_key.extend(key);

    // Advance the internal cursor maintained in raw_entry.
    raw_entry.advance(key_size);

    {
      let value_size = raw_entry.get_u16() as usize;

      let current_value_offset = (U16_SIZE + key_size) + U16_SIZE;
      let current_value_ends_at = current_value_offset + value_size;

      self.current_value_range = (current_value_offset, current_value_ends_at);

      // Advance the internal cursor maintained in raw_entry.
      raw_entry.advance(value_size);
    }
  }
}

impl Iterator for BlockIterator {
  fn key(&self) -> &[u8] {
    &self.current_key
  }

  fn value(&self) -> &[u8] {
    let (value_offset, value_ends_at) = self.current_value_range;
    &self.block.raw_entries[value_offset..value_ends_at]
  }

  fn next(&mut self) -> anyhow::Result<()> {
    self.current_index += 1;

    self.seek_to_entry_with_index(self.current_index);

    Ok(())
  }

  fn is_valid(&self) -> bool {
    !self.current_key.is_empty()
  }
}
