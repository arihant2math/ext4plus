use crate::Ext4;
use crate::block_index::FsBlockIndex;
use crate::error::{CorruptKind, Ext4Error};
use crate::util::usize_from_u32;
use alloc::collections::BTreeMap;
use alloc::vec;
use alloc::vec::Vec;

/// In-memory transaction state for staged block writes.
///
/// This is currently backed only by an in-memory dirty-block map. Reads observe
/// staged writes, and commit writes the final block images to their home blocks
/// on disk. Until commit, the underlying storage is unchanged.
pub(crate) struct Transaction {
    fs: Ext4,
    dirty_blocks: BTreeMap<FsBlockIndex, Vec<u8>>,
}

impl Transaction {
    pub(crate) fn new(fs: Ext4) -> Self {
        Self {
            fs,
            dirty_blocks: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_dirty(&self) -> bool {
        !self.dirty_blocks.is_empty()
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn read_from_block(
        &self,
        block_index: FsBlockIndex,
        offset_within_block: u32,
        dst: &mut [u8],
    ) -> Result<(), Ext4Error> {
        if let Some(block) = self.dirty_blocks.get(&block_index) {
            let (start, end) = self.validate_read(
                block_index,
                offset_within_block,
                dst.len(),
            )?;
            dst.copy_from_slice(&block[start..end]);
            return Ok(());
        }

        self.fs
            .read_from_block(block_index, offset_within_block, dst)
            .await
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn read_block(
        &self,
        block_index: FsBlockIndex,
    ) -> Result<Vec<u8>, Ext4Error> {
        if let Some(block) = self.dirty_blocks.get(&block_index) {
            return Ok(block.clone());
        }

        let mut block = vec![0; self.fs.superblock().block_size().to_usize()];
        self.read_from_block(block_index, 0, &mut block).await?;
        Ok(block)
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn write_to_block(
        &mut self,
        block_index: FsBlockIndex,
        offset_within_block: u32,
        src: &[u8],
    ) -> Result<(), Ext4Error> {
        let (start, end) =
            self.validate_write(block_index, offset_within_block, src.len())?;

        let mut block = if let Some(block) = self.dirty_blocks.get(&block_index)
        {
            block.clone()
        } else {
            self.read_block(block_index).await?
        };

        block[start..end].copy_from_slice(src);
        self.dirty_blocks.insert(block_index, block);
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn commit(self) -> Result<(), Ext4Error> {
        for (block_index, block) in self.dirty_blocks {
            self.fs.write_to_block(block_index, 0, &block).await?;
        }
        Ok(())
    }

    fn validate_read(
        &self,
        block_index: FsBlockIndex,
        offset_within_block: u32,
        read_len: usize,
    ) -> Result<(usize, usize), Ext4Error> {
        let err = || {
            Ext4Error::from(CorruptKind::BlockRead {
                block_index,
                original_block_index: block_index,
                offset_within_block,
                read_len,
            })
        };

        self.validate_range(block_index, offset_within_block, read_len, err)
    }

    fn validate_write(
        &self,
        block_index: FsBlockIndex,
        offset_within_block: u32,
        write_len: usize,
    ) -> Result<(usize, usize), Ext4Error> {
        let err = || {
            Ext4Error::from(CorruptKind::BlockWrite {
                block_index,
                original_block_index: block_index,
                offset_within_block,
                write_len,
            })
        };

        self.validate_range(block_index, offset_within_block, write_len, err)
    }

    fn validate_range(
        &self,
        block_index: FsBlockIndex,
        offset_within_block: u32,
        len: usize,
        err: impl Fn() -> Ext4Error,
    ) -> Result<(usize, usize), Ext4Error> {
        let block_size = self.fs.superblock().block_size();

        if block_index == 0 && offset_within_block < 1024 {
            return Err(err());
        }

        if block_index >= self.fs.superblock().blocks_count() {
            return Err(err());
        }

        if offset_within_block >= block_size {
            return Err(err());
        }

        let end = usize_from_u32(offset_within_block)
            .checked_add(len)
            .ok_or_else(&err)?;
        if end > block_size.to_usize() {
            return Err(err());
        }

        Ok((usize_from_u32(offset_within_block), end))
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::file_blocks::FileBlocks;
    use crate::resolve::FollowSymlinks;
    use crate::test_util::load_compressed_filesystem_rw;
    use std::sync::{Arc, Mutex};

    fn image_byte(
        fs: &Ext4,
        image: &Arc<Mutex<Vec<u8>>>,
        block_index: FsBlockIndex,
        offset: usize,
    ) -> u8 {
        let block_start = usize::try_from(
            block_index
                .checked_mul(fs.superblock().block_size().to_u64())
                .unwrap(),
        )
        .unwrap();
        image.lock().unwrap()[block_start + offset]
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_transaction_stages_writes_until_commit() {
        let (fs, image) =
            load_compressed_filesystem_rw("test_disk1.bin.zst").await;
        let inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        let block_index = FileBlocks::from_inode(&inode, fs.clone())
            .unwrap()
            .get_block(0)
            .await
            .unwrap();

        let old = image_byte(&fs, &image, block_index, 0);
        let new = old ^ 0xff;

        let mut tx = fs.begin_transaction().unwrap();
        assert!(!tx.is_dirty());
        tx.write_to_block(block_index, 0, &[new]).await.unwrap();
        assert!(tx.is_dirty());

        assert_eq!(image_byte(&fs, &image, block_index, 0), old);

        let mut buf = [0; 1];
        tx.read_from_block(block_index, 0, &mut buf).await.unwrap();
        assert_eq!(buf[0], new);

        tx.commit().await.unwrap();
        assert_eq!(image_byte(&fs, &image, block_index, 0), new);
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_transaction_drop_discards_staged_writes() {
        let (fs, image) =
            load_compressed_filesystem_rw("test_disk1.bin.zst").await;
        let inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        let block_index = FileBlocks::from_inode(&inode, fs.clone())
            .unwrap()
            .get_block(0)
            .await
            .unwrap();

        let old = image_byte(&fs, &image, block_index, 1);
        let new = old ^ 0xff;

        {
            let mut tx = fs.begin_transaction().unwrap();
            tx.write_to_block(block_index, 1, &[new]).await.unwrap();

            let mut buf = [0; 1];
            tx.read_from_block(block_index, 1, &mut buf).await.unwrap();
            assert_eq!(buf[0], new);
            assert_eq!(image_byte(&fs, &image, block_index, 1), old);
        }

        assert_eq!(image_byte(&fs, &image, block_index, 1), old);
    }
}
