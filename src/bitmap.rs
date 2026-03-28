use crate::block_index::FsBlockIndex;
use crate::checksum::Checksum;
use crate::{Ext4, Ext4Error};

use crate::util::usize_from_u32;
use alloc::vec;
use core::ops::RangeBounds;

pub(crate) struct BitmapHandle {
    block: FsBlockIndex,
    is_inode_bitmap: bool,
}

#[allow(unused)]
impl BitmapHandle {
    pub(crate) fn new(block: FsBlockIndex, is_inode_bitmap: bool) -> Self {
        Self {
            block,
            is_inode_bitmap,
        }
    }

    /// Query the bitmap for the value of bit `n`.
    #[maybe_async::maybe_async]
    pub(crate) async fn query(
        &self,
        n: u32,
        ext4: &Ext4,
    ) -> Result<bool, Ext4Error> {
        let mut dst = [0; 1];
        let byte_index = n / 8;
        let bit_index = n % 8;
        ext4.read_from_block(self.block, byte_index, &mut dst)
            .await?;
        // Get the value of the bit at `bit_index` in `dst[0]`.
        Ok((dst[0] & (1 << bit_index)) != 0)
    }

    /// Set the value of bit `n` in the bitmap to `value`.
    #[maybe_async::maybe_async]
    pub(crate) async fn set(
        &self,
        n: u32,
        value: bool,
        ext4: &Ext4,
    ) -> Result<(), Ext4Error> {
        let mut dst = [0; 1];
        let byte_index = n / 8;
        let bit_index = n % 8;
        ext4.read_from_block(self.block, byte_index, &mut dst)
            .await?;
        if value {
            dst[0] |= 1 << bit_index;
        } else {
            dst[0] &= !(1 << bit_index);
        }
        ext4.write_to_block(self.block, byte_index, &dst).await?;
        Ok(())
    }

    /// Find the first bit in the bitmap with value `value`, and return its index.
    /// Returns `Ok(None)` if no such bit is found.
    #[maybe_async::maybe_async]
    pub(crate) async fn find_first(
        &self,
        value: bool,
        range: impl RangeBounds<u32>,
        ext4: &Ext4,
    ) -> Result<Option<u32>, Ext4Error> {
        let mut dst = [0; 1];
        for byte_index in 0..ext4.0.superblock.block_size().to_u32() {
            ext4.read_from_block(self.block, byte_index, &mut dst)
                .await?;
            if value {
                // Look for a bit with value 1.
                if dst[0] != 0 {
                    for bit_index in 0..8 {
                        if (dst[0] & (1 << bit_index)) != 0 {
                            let index = byte_index
                                .checked_mul(8)
                                .unwrap()
                                .checked_add(bit_index)
                                .unwrap();
                            if !range.contains(&(index)) {
                                continue;
                            }
                            return Ok(Some(index));
                        }
                    }
                }
            } else {
                // Look for a bit with value 0.
                if dst[0] != 0xFF {
                    for bit_index in 0..8 {
                        if (dst[0] & (1 << bit_index)) == 0 {
                            let index = byte_index
                                .checked_mul(8)
                                .unwrap()
                                .checked_add(bit_index)
                                .unwrap();
                            if !range.contains(&(index)) {
                                continue;
                            }
                            return Ok(Some(index));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Find the first `n` bits in the bitmap with value `value`, and return the initial index.
    /// Returns `Ok(None)` if no such sequence of bits is found.
    #[maybe_async::maybe_async]
    pub(crate) async fn find_first_n(
        &self,
        n: u32,
        value: bool,
        range: impl RangeBounds<u32>,
        ext4: &Ext4,
    ) -> Result<Option<u32>, Ext4Error> {
        let mut dst = [0; 1];
        let mut count: u32 = 0;
        for byte_index in 0..ext4.0.superblock.block_size().to_u32() {
            ext4.read_from_block(self.block, byte_index, &mut dst)
                .await?;
            for bit_index in 0..8 {
                if ((dst[0] & (1 << bit_index)) != 0) == value {
                    let index = byte_index
                        .checked_mul(8)
                        .unwrap()
                        .checked_add(bit_index)
                        .unwrap();
                    if !range.contains(&(index)) {
                        count = 0;
                        continue;
                    }
                    count = count.checked_add(1).unwrap();
                    if count == n {
                        return Ok(Some(
                            index
                                .checked_add(1)
                                .unwrap()
                                .checked_sub(n)
                                .unwrap(),
                        ));
                    }
                } else {
                    count = 0;
                }
            }
        }
        Ok(None)
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn calc_checksum(
        &self,
        ext4: &Ext4,
        block_group_index: u32,
    ) -> Result<u32, Ext4Error> {
        let mut dst = vec![0; ext4.0.superblock.block_size().to_usize()];
        ext4.read_from_block(self.block, 0, &mut dst).await?;
        let mut checksum =
            Checksum::with_seed(ext4.0.superblock.checksum_seed());

        let bytes_to_hash = if self.is_inode_bitmap {
            let inodes_per_group =
                ext4.0.superblock.inodes_per_block_group().get();
            ((usize_from_u32(inodes_per_group)).checked_add(7).unwrap()) / 8
        } else {
            ext4.0.superblock.block_size().to_usize()
        };

        checksum.update(&dst[..bytes_to_hash]);
        Ok(checksum.finalize())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "std")]
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_bitmap_handle() {
        let fs = crate::test_util::load_test_disk1().await;

        let bitmap = fs.get_block_bitmap_handle(0);
        let first = bitmap.find_first(false, .., &fs).await.unwrap();
        // Ensure false
        let query = bitmap.query(first.unwrap(), &fs).await.unwrap();
        assert!(!query);
        let first = bitmap.find_first(true, .., &fs).await.unwrap();
        // Ensure true
        let query = bitmap.query(first.unwrap(), &fs).await;
        assert!(query.unwrap());
    }
}
