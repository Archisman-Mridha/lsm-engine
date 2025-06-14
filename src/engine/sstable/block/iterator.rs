use {
  crate::engine::{
    iterator::Iterator,
    sstable::block::{Block, U16_SIZE},
  },
  bytes::Buf,
  std::{cmp::Ordering, sync::Arc},
};

pub struct BlockIterator {
  block: Arc<Block>,

  // Whetever entry the cursor of this iterator is currently pointing to,
  // this is the index of that entry in self.block.rawEntries.
  currentIndex:      usize,
  //
  // This is the key of that entry.
  currentKey:        Vec<u8>,
  //
  // And this is the byte range of the value of that entry in self.block.rawEntries.
  currentValueRange: (usize, usize),
}

impl BlockIterator {
  pub fn new(block: Arc<Block>) -> Self {
    Self {
      block,

      currentIndex: 0,
      currentKey: Vec::new(),
      currentValueRange: (0, 0),
    }
  }

  // Seeks to the first key >= the given target key.
  pub fn seekToKey(&mut self, targetKey: &Vec<u8>) {
    // Use Binary Search to find the index of the key >= the given key,
    // in self.block.rawEntries.

    let mut lowerIndex = 0;
    let mut upperIndex = self.block.rawEntries.len();

    while lowerIndex < upperIndex {
      let midIndex = lowerIndex + ((lowerIndex + upperIndex) / 2);

      self.seekToEntryWithIndex(midIndex);

      match self.currentKey.cmp(targetKey) {
        Ordering::Equal => return,

        Ordering::Less => upperIndex = midIndex - 1,
        Ordering::Greater => lowerIndex = midIndex + 1,
      }
    }

    // At this point lowerIndex = higherIndex.
    self.seekToEntryWithIndex(lowerIndex);
  }

  // Seeks to the entry with the given index.
  pub fn seekToEntryWithIndex(&mut self, index: usize) {
    if index >= self.block.rawEntries.len() {
      self.currentIndex = 0;
      self.currentKey = Vec::new();
      self.currentValueRange = (0, 0);
    }

    let offset = self.block.offsets[index];
    self.seekToOffset(offset);
  }

  // Seeks to the entry at the given byte offset in self.rawEntries.
  pub fn seekToOffset(&mut self, offset: u16) {
    let mut rawEntry = &self.block.rawEntries[offset as usize..];

    let keySize = rawEntry.get_u16() as usize;
    let key = rawEntry[..keySize].to_vec();
    //
    // Yes, we do allocate a Vec for 'key'. But that's very short lived.
    // Doing 'self.currentKey.extend(key)', we reuse the allocated heap memory for
    // self.currentKey.
    // NOTE : Doing 'self.currentKey = key', won't lead to a memory leak.
    //
    self.currentKey.clear();
    self.currentKey.extend(key);

    // Advance the internal cursor maintained in rawEntry.
    rawEntry.advance(keySize);

    {
      let valueSize = rawEntry.get_u16() as usize;

      let currentValueOffset = (U16_SIZE + keySize) + U16_SIZE;
      let currentValueEndsAt = currentValueOffset + valueSize;

      self.currentValueRange = (currentValueOffset, currentValueEndsAt);

      // Advance the internal cursor maintained in rawEntry.
      rawEntry.advance(valueSize);
    }
  }
}

impl Iterator for BlockIterator {
  fn key(&self) -> &[u8] {
    &self.currentKey
  }

  fn value(&self) -> &[u8] {
    let (valueOffset, valueEndsAt) = self.currentValueRange;
    &self.block.rawEntries[valueOffset..valueEndsAt]
  }

  fn next(&mut self) -> anyhow::Result<()> {
    self.currentIndex += 1;

    self.seekToEntryWithIndex(self.currentIndex);

    Ok(())
  }

  fn isValid(&self) -> bool {
    !self.currentKey.is_empty()
  }
}
