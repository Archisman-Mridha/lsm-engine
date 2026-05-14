use {
  crate::engine::sstable::{
    SSTable,
    block::{builder::BlockBuilder, metadata::BlockMetadata},
    file::SSTableFile
  },
  bytes::BufMut,
  std::mem
};

pub struct SSTableBuilder {
  // Contains :
  // (encoded) blocks | (encoded) block metadatas | (encoded) block metadatas offset.
  data: Vec<u8>,

  block_metadatas: Vec<BlockMetadata>,

  max_block_size: usize,

  current_block_builder:   BlockBuilder,
  current_block_first_key: Vec<u8>,
  current_block_last_key:  Vec<u8>
}

impl SSTableBuilder {
  pub fn new(max_block_size: usize) -> Self {
    Self { data: Vec::new(),
           block_metadatas: Vec::new(),

           max_block_size,

           current_block_builder: BlockBuilder::new(max_block_size),
           current_block_first_key: Vec::new(),
           current_block_last_key: Vec::new() }
  }

  pub fn insert_kv_pair(&mut self, key: &[u8], value: &[u8]) {
    if self.current_block_first_key.is_empty() {
      self.current_block_first_key.clear();
      self.current_block_first_key.extend(key);
    }

    if self.current_block_builder.insert_kv_pair(key, value).is_ok() {
      self.current_block_last_key.clear();
      self.current_block_last_key.extend(key);

      return;
    }
    //
    // Current block which is being built, has reached it's size limit.
    // So, we need to finish building that block and include it in the SSTable.
    // And instead, start using a new block for inserting this and any further key-value pairs.
    self.build_current_block();

    // Finally, insert the key-value pair into the new block.
    // NOTE : We could've recursively called self.insert_kv_pair( ), but we want to make sure that
    //        atleast 1 key-value pair gets inserted into the new block.
    //        Otherwise, we'll be stuck in an infinite recursion, creating and finalizing an empty
    //        block everytime.
    assert!(self.current_block_builder.insert_kv_pair(key, value).is_ok());

    self.current_block_first_key.clear();
    self.current_block_first_key.extend(key);

    self.current_block_last_key.clear();
    self.current_block_last_key.extend(key);
  }

  // Finishes building the current block and adds it to the SSTable.
  // Instead creates a new block where further key-value pairs will be inserted.
  fn build_current_block(&mut self) {
    // Create the new block builder.
    let new_block_builder = BlockBuilder::new(self.max_block_size);
    let old_block_builder = mem::replace(&mut self.current_block_builder, new_block_builder);

    // Finish building the old block.
    let old_block = old_block_builder.build();

    // Include the old block in the SSTable.

    self.block_metadatas.push(BlockMetadata {
      offset: self.data.len() as u64,

      first_key: mem::take(&mut self.current_block_first_key).into(),
      last_key:  mem::take(&mut self.current_block_last_key).into(),
    });

    self.data.extend(old_block.encode());
  }

  pub fn build(mut self, id: usize, file: SSTableFile) -> SSTable {
    // Finish building the last block.
    self.build_current_block();

    let block_metadata_encodings_offset = self.data.len() as u64;

    // We have already added block encodings.

    // Add metadata encodings.
    {
      // Calculate the total size required by the block metadata encodings.
      let mut block_metadata_encodings_size = 0;
      for block_metadata in &self.block_metadatas {
        block_metadata_encodings_size += block_metadata.encoding_size();
      }

      // Expand the capacity of self.data by that amount.
      // So, we avoided multiple small allocations :).
      self.data.reserve(block_metadata_encodings_size);

      // Write block metadata encodings into self.data.
      for block_metadata in &self.block_metadatas {
        block_metadata.encode(&mut self.data);
      }
    }

    // Add metadata encodings offset.
    self.data.put_u64(block_metadata_encodings_offset);

    SSTable { id,

              file,

              first_key: self.block_metadatas.first().unwrap().first_key.clone(),
              last_key: self.block_metadatas.last().unwrap().last_key.clone(),

              block_metadatas: self.block_metadatas,
              block_metadata_encodings_offset }
  }
}
