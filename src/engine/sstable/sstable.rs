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

  first_key: Bytes,
  last_key:  Bytes,

  // We have assumed that, all block metadata encodings can fit in memory.
  // Also, during size calculation of an SSTable, we ignore the size of block metadata encodings.
  block_metadatas:                 Vec<BlockMetadata>,
  block_metadata_encodings_offset: u64,
}

impl SSTable {
  // Returns an SSTable constructed using the contents of the given file.
  pub fn new_from_file(id: usize, path: &Path) -> anyhow::Result<Self> {
    // Read data off the SSTable file.
    let file = SSTableFile::new(path)?;

    let data_size = file.size()?;

    // Determine the block metadata encodings offset.
    let raw_block_metadata_encodings_offset =
      file.read(data_size - U64_SIZE as u64, U64_SIZE as u64)?;
    let block_metadata_encodings_offset = (&raw_block_metadata_encodings_offset[..]).get_u64();

    // Read block metadata encodings.

    let block_metadata_encodings = file.read(
      block_metadata_encodings_offset,
      data_size - (block_metadata_encodings_offset + U64_SIZE as u64),
    )?;

    let mut block_metadatas = Vec::new();
    while block_metadata_encodings.has_remaining_mut() {
      let block_metadata = BlockMetadata::decode(&block_metadata_encodings[..]);
      block_metadatas.push(block_metadata);
    }

    Ok(Self {
      id,

      file,

      first_key: block_metadatas.first().unwrap().first_key.clone(),
      last_key: block_metadatas.last().unwrap().last_key.clone(),

      block_metadata_encodings_offset,
      block_metadatas,
    })
  }

  // Get block at the given index.
  pub fn read_block_at_index(&self, index: usize) -> anyhow::Result<Arc<Block>> {
    let block_metadata = &self.block_metadatas[index];

    let block_offset = block_metadata.offset;
    let block_ends_at = {
      if index < self.block_metadatas.len() - 1 {
        self.block_metadata_encodings_offset
      }
      else {
        self.block_metadatas[index + 1].offset
      }
    };
    let block_size = block_offset - block_ends_at;

    let raw_block = self.file.read(block_offset, block_size)?;

    let block = Block::decode(&raw_block);
    Ok(Arc::new(block))
  }
}
