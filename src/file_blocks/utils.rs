use crate::Ext4;
use crate::block_index::{FileBlockIndex, FsBlockIndex};
use crate::error::{CorruptKind, Ext4Error};
use crate::inode::InodeIndex;
use crate::util::u64_from_usize;
use alloc::vec::Vec;
use core::num::NonZeroU32;

pub(super) fn add_to_file_offset(
    offset: u64,
    delta: usize,
) -> Result<u64, Ext4Error> {
    offset
        .checked_add(u64_from_usize(delta))
        .ok_or(Ext4Error::FileTooLarge)
}

pub(super) fn file_block_from_offset(
    offset: u64,
    block_size: u64,
) -> Result<FileBlockIndex, Ext4Error> {
    let block = offset
        .checked_div(block_size)
        .ok_or(CorruptKind::InvalidBlockSize)?;
    FileBlockIndex::try_from(block).map_err(|_| Ext4Error::FileTooLarge)
}

pub(super) fn offset_in_block_usize(
    offset: u64,
    block_size: u64,
) -> Result<usize, Ext4Error> {
    let offset_in_block = offset
        .checked_rem(block_size)
        .ok_or(CorruptKind::InvalidBlockSize)?;
    usize::try_from(offset_in_block)
        .map_err(|_| CorruptKind::InvalidBlockSize.into())
}

pub(super) fn range_end(start: usize, len: usize) -> Result<usize, Ext4Error> {
    start.checked_add(len).ok_or(Ext4Error::FileTooLarge)
}

#[maybe_async::maybe_async]
pub(super) async fn free_freed_ranges(
    ext4: &Ext4,
    inode: InodeIndex,
    freed: Vec<(FsBlockIndex, u32)>,
) -> Result<u64, Ext4Error> {
    let mut freed_data_blocks = 0u64;

    for (start, len) in freed {
        if start == 0 || len == 0 {
            continue;
        }

        if let Some(nz) = NonZeroU32::new(len) {
            if ext4.free_blocks(start, nz).await.is_err() {
                for i in 0..len {
                    ext4.free_block(
                        start
                            .checked_add(u64::from(i))
                            .ok_or(CorruptKind::ExtentBlock(inode))?,
                    )
                    .await?;
                }
            }

            freed_data_blocks = freed_data_blocks
                .checked_add(u64::from(len))
                .ok_or(CorruptKind::InodeBlockCount(inode))?;
        }
    }

    Ok(freed_data_blocks)
}
