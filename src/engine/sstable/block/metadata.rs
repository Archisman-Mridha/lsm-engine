use {
  crate::engine::sstable::block::U16_SIZE,
  bytes::{Buf, BufMut, Bytes},
  std::mem,
};

pub const U64_SIZE: usize = mem::size_of::<u64>();

pub struct BlockMetadata {
  // Offset of the block, in the corresponding sstable.rawBlocks.
  pub offset: u64,

  pub firstKey: Bytes,
  pub lastKey:  Bytes,
}

impl BlockMetadata {
  // Returns the block metadata encoding size.
  pub fn encodingSize(&self) -> usize {
    U64_SIZE + (U16_SIZE + self.firstKey.len()) + (U16_SIZE + self.lastKey.len())
  }

  // Encodes the block metadata into the given buffer.
  pub fn encode(&self, buffer: &mut Vec<u8>) {
    buffer.put_u64(self.offset);

    buffer.put_u16(self.firstKey.len() as u16);
    buffer.extend(&self.firstKey);

    buffer.put_u16(self.lastKey.len() as u16);
    buffer.extend(&self.lastKey);
  }

  // Decodes the block metadata from the given buffer.
  pub fn decode(mut buffer: impl Buf) -> Self {
    let offset = buffer.get_u64();

    let firstKeySize = buffer.get_u16() as usize;
    let firstKey = buffer.copy_to_bytes(firstKeySize);

    let lastKeySize = buffer.get_u16() as usize;
    let lastKey = buffer.copy_to_bytes(lastKeySize);

    Self {
      offset,

      firstKey,
      lastKey,
    }
  }
}
