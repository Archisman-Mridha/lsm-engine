use std::{
  fs::File,
  os::unix::fs::{FileExt, MetadataExt},
  path::Path
};

pub struct SSTableFile(File);

impl SSTableFile {
  pub fn new(path: &Path) -> anyhow::Result<Self> {
    let file = File::options().read(true).open(path)?;

    Ok(Self(file))
  }

  pub fn size(&self) -> anyhow::Result<u64> {
    Ok(self.0.metadata()?.size())
  }

  // Read given number of bytes, from the given offset,
  // off the SSTable file.
  pub fn read(&self, offset: u64, size: u64) -> anyhow::Result<Vec<u8>> {
    let mut bytes = vec![0; size as usize];
    self.0.read_exact_at(&mut bytes, offset)?;

    Ok(bytes)
  }
}
