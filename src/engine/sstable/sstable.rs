use {
  crate::engine::sstable::{
    block::{
      Block,
      metadata::{BlockMetadata, U64_SIZE},
    },
    file::SSTableFile,
  },
  bytes::{Buf, BufMut, Bytes},
  std::{path::Path, sync::Arc},
};

#[path = "./block/block.rs"]
mod block;

pub mod builder;
pub mod iterator;

pub mod file;

pub struct SSTable {
  id: usize,

  file: SSTableFile,

  firstKey: Bytes,
  lastKey:  Bytes,

  // We have assumed that, all block metadata encodings can fit in memory.
  // Also, during size calculation of an SSTable, we ignore the size of block metadata encodings.
  blockMetadatas:               Vec<BlockMetadata>,
  blockMetadataEncodingsOffset: u64,
}

impl SSTable {
  // Returns an SSTable constructed using the contents of the given file.
  pub fn newFromFile(id: usize, path: &Path) -> anyhow::Result<Self> {
    // Read data off the SSTable file.
    let file = SSTableFile::new(path)?;

    let dataSize = file.size()?;

    // Determine the block metadata encodings offset.
    let rawBlockMetadataEncodingsOffset = file.read(dataSize - U64_SIZE as u64, U64_SIZE as u64)?;
    let blockMetadataEncodingsOffset = (&rawBlockMetadataEncodingsOffset[..]).get_u64();

    // Read block metadata encodings.

    let blockMetadataEncodings = file.read(
      blockMetadataEncodingsOffset,
      dataSize - (blockMetadataEncodingsOffset + U64_SIZE as u64),
    )?;

    let mut blockMetadatas = Vec::new();
    while blockMetadataEncodings.has_remaining_mut() {
      let blockMetadata = BlockMetadata::decode(&blockMetadataEncodings[..]);
      blockMetadatas.push(blockMetadata);
    }

    Ok(Self {
      id,

      file,

      firstKey: blockMetadatas.first().unwrap().firstKey.clone(),
      lastKey: blockMetadatas.last().unwrap().lastKey.clone(),

      blockMetadataEncodingsOffset,
      blockMetadatas,
    })
  }

  // Get block at the given index.
  pub fn readBlockAtIndex(&self, index: usize) -> anyhow::Result<Arc<Block>> {
    let blockMetadata = &self.blockMetadatas[index];

    let blockOffset = blockMetadata.offset;
    let blockEndsAt = {
      if index < self.blockMetadatas.len() - 1 {
        self.blockMetadataEncodingsOffset
      }
      else {
        self.blockMetadatas[index + 1].offset
      }
    };
    let blockSize = blockOffset - blockEndsAt;

    let rawBlock = self.file.read(blockOffset, blockSize)?;

    let block = Block::decode(&rawBlock);
    Ok(Arc::new(block))
  }
}
