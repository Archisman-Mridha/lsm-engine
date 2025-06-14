use {
  bytes::{Buf, BufMut, Bytes},
  std::mem,
};

pub mod builder;
pub mod iterator;

const U16_SIZE: usize = mem::size_of::<u16>();

// The basic unit of the on-disk structure is blocks.
// A block stores ordered key-value pairs.
// Blocks are usually of 4-KB size (the size may vary depending on the storage medium), which is
// equivalent to the page size in the operating system and the page size on an SSD.
pub struct Block {
  rawEntries: Vec<u8>,

  // Offset of each entry.
  offsets: Vec<u16>,
}

impl Block {
  pub fn encode(&self) -> Bytes {
    // Encode the raw entries.
    let mut encoding = self.rawEntries.clone();

    let entryCount = self.offsets.len() as u16;

    // Encode the offsets.
    for offset in self.offsets.iter() {
      encoding.put_u16(*offset);
    }

    // Encode the entry count.
    encoding.put_u16(entryCount);

    encoding.into()
  }

  pub fn decode(encoding: &[u8]) -> Self {
    // Last 2 bytes of the encoding, stores the entry count.
    // So, let's get that first.
    let entryCountStartsAt = encoding.len() - U16_SIZE;
    let entryCount = (&encoding[entryCountStartsAt..]).get_u16() as usize;

    // Get the offsets.
    let offsetsStartAt = entryCountStartsAt - (U16_SIZE * entryCount);
    let offsets = encoding[offsetsStartAt..entryCountStartsAt]
      .chunks(U16_SIZE)
      .map(|mut chunk| chunk.get_u16())
      .collect();

    // Get the (raw) entries.
    let rawEntries = encoding[0..offsetsStartAt].to_vec();

    Self {
      rawEntries,
      offsets,
    }
  }
}
