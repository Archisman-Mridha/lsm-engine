use {
  crate::engine::sstable::{
    SSTable,
    block::{builder::BlockBuilder, metadata::BlockMetadata},
    file::SSTableFile,
  },
  bytes::BufMut,
  std::mem,
};

pub struct SSTableBuilder {
  // Contains :
  // (encoded) blocks | (encoded) block metadatas | (encoded) block metadatas offset.
  data: Vec<u8>,

  blockMetadatas: Vec<BlockMetadata>,

  maxBlockSize: usize,

  currentBlockBuilder:  BlockBuilder,
  currentBlockFirstKey: Vec<u8>,
  currentBlockLastKey:  Vec<u8>,
}

impl SSTableBuilder {
  pub fn new(maxBlockSize: usize) -> Self {
    Self {
      data: Vec::new(),
      blockMetadatas: Vec::new(),

      maxBlockSize,

      currentBlockBuilder: BlockBuilder::new(maxBlockSize),
      currentBlockFirstKey: Vec::new(),
      currentBlockLastKey: Vec::new(),
    }
  }

  pub fn insertKVPair(&mut self, key: &[u8], value: &[u8]) {
    if self.currentBlockFirstKey.is_empty() {
      self.currentBlockFirstKey.clear();
      self.currentBlockFirstKey.extend(key);
    }

    if self.currentBlockBuilder.insertKVPair(key, value).is_ok() {
      self.currentBlockLastKey.clear();
      self.currentBlockLastKey.extend(key);

      return;
    }
    //
    // Current block which is being built, has reached it's size limit.
    // So, we need to finish building that block and include it in the SSTable.
    // And instead, start using a new block for inserting this and any further key-value pairs.
    self.buildCurrentBlock();

    // Finally, insert the key-value pair into the new block.
    // NOTE : We could've recursively called self.insertKVPair( ), but we want to make sure that
    //        atleast 1 key-value pair gets inserted into the new block.
    //        Otherwise, we'll be stuck in an infinite recursion, creating and finalizing an empty
    //        block everytime.
    assert!(self.currentBlockBuilder.insertKVPair(key, value).is_ok());

    self.currentBlockFirstKey.clear();
    self.currentBlockFirstKey.extend(key);

    self.currentBlockLastKey.clear();
    self.currentBlockLastKey.extend(key);
  }

  // Finishes building the current block and adds it to the SSTable.
  // Instead creates a new block where further key-value pairs will be inserted.
  fn buildCurrentBlock(&mut self) {
    // Create the new block builder.
    let newBlockBuilder = BlockBuilder::new(self.maxBlockSize);
    let oldBlockBuilder = mem::replace(&mut self.currentBlockBuilder, newBlockBuilder);

    // Finish building the old block.
    let oldBlock = oldBlockBuilder.build();

    // Include the old block in the SSTable.

    self.blockMetadatas.push(BlockMetadata {
      offset: self.data.len() as u64,

      firstKey: mem::take(&mut self.currentBlockFirstKey).into(),
      lastKey:  mem::take(&mut self.currentBlockLastKey).into(),
    });

    self.data.extend(oldBlock.encode());
  }

  pub fn build(mut self, id: usize, file: SSTableFile) -> SSTable {
    // Finish building the last block.
    self.buildCurrentBlock();

    let blockMetadataEncodingsOffset = self.data.len() as u64;

    // We have already added block encodings.

    // Add metadata encodings.
    {
      // Calculate the total size required by the block metadata encodings.
      let mut blockMetadataEncodingsSize = 0;
      for blockMetadata in &self.blockMetadatas {
        blockMetadataEncodingsSize += blockMetadata.encodingSize();
      }

      // Expand the capacity of self.data by that amount.
      // So, we avoided multiple small allocations :).
      self.data.reserve(blockMetadataEncodingsSize);

      // Write block metadata encodings into self.data.
      for blockMetadata in &self.blockMetadatas {
        blockMetadata.encode(&mut self.data);
      }
    }

    // Add metadata encodings offset.
    self.data.put_u64(blockMetadataEncodingsOffset);

    SSTable {
      id,

      file,

      firstKey: self.blockMetadatas.first().unwrap().firstKey.clone(),
      lastKey: self.blockMetadatas.last().unwrap().lastKey.clone(),

      blockMetadatas: self.blockMetadatas,
      blockMetadataEncodingsOffset,
    }
  }
}
