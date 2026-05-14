use {
  crate::engine::sstable::block::U16_SIZE,
  bytes::{Buf, BufMut, Bytes},
  std::mem
};

pub const U64_SIZE: usize = mem::size_of::<u64>();

pub struct BlockMetadata {
  // Offset of the block, in the corresponding sstable.raw_blocks.
  pub offset: u64,

  pub first_key: Bytes,
  pub last_key:  Bytes
}

impl BlockMetadata {
  // Returns the block metadata encoding size.
  pub fn encoding_size(&self) -> usize {
    U64_SIZE + (U16_SIZE + self.first_key.len()) + (U16_SIZE + self.last_key.len())
  }

  // Encodes the block metadata into the given buffer.
  pub fn encode(&self, buffer: &mut Vec<u8>) {
    buffer.put_u64(self.offset);

    buffer.put_u16(self.first_key.len() as u16);
    buffer.extend(&self.first_key);

    buffer.put_u16(self.last_key.len() as u16);
    buffer.extend(&self.last_key);
  }

  // Decodes the block metadata from the given buffer.
  pub fn decode(mut buffer: impl Buf) -> Self {
    let offset = buffer.get_u64();

    let first_key_size = buffer.get_u16() as usize;
    let first_key = buffer.copy_to_bytes(first_key_size);

    let last_key_size = buffer.get_u16() as usize;
    let last_key = buffer.copy_to_bytes(last_key_size);

    Self { offset,

           first_key,
           last_key }
  }
}
