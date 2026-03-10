// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::block_index::FsBlockIndex;
use crate::checksum::Checksum;
use crate::error::{CorruptKind, Ext4Error};
use crate::features::{IncompatibleFeatures, ReadOnlyCompatibleFeatures};
use crate::superblock::Superblock;
use crate::util::{
    read_u16le, read_u32le, u32_from_hilo, u32_to_hilo, u64_from_hilo,
    u64_to_hilo, usize_from_u32, write_u16le, write_u32le,
};
use crate::{Ext4, Ext4Read};
use alloc::vec;
use alloc::vec::Vec;
use core::ops::Deref;
use core::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

pub(crate) type BlockGroupIndex = u32;

pub(crate) enum BlockGroupDescriptorBytes {
    OnDisk32([u8; BlockGroupDescriptor::SIZE_IN_BYTES_ON_DISK_32]),
    OnDisk64([u8; BlockGroupDescriptor::SIZE_IN_BYTES_ON_DISK_64]),
}

impl Deref for BlockGroupDescriptorBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::OnDisk32(bytes) => bytes,
            Self::OnDisk64(bytes) => bytes,
        }
    }
}

#[expect(unused)]
pub(crate) enum TruncatedChecksum {
    Truncated(u16),
    Full(u32),
}

impl From<&AtomicTruncatedChecksum> for TruncatedChecksum {
    fn from(atomic: &AtomicTruncatedChecksum) -> Self {
        match atomic {
            AtomicTruncatedChecksum::Truncated(c) => {
                Self::Truncated(c.load(Ordering::Relaxed))
            }
            AtomicTruncatedChecksum::Full(c) => {
                Self::Full(c.load(Ordering::Relaxed))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum AtomicTruncatedChecksum {
    Truncated(AtomicU16),
    Full(AtomicU32),
}

impl AtomicTruncatedChecksum {
    fn update(&self, checksum: u32) {
        match self {
            Self::Truncated(c) =>
            {
                #[expect(clippy::as_conversions)]
                c.store(checksum as u16, Ordering::Relaxed)
            }
            Self::Full(c) => c.store(checksum, Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockGroupDescriptor {
    index: BlockGroupIndex,
    is_64bit: bool,
    block_bitmap: AtomicU64,
    inode_bitmap: AtomicU64,
    inode_table_first_block: AtomicU64,
    free_blocks_count: AtomicU32,
    free_inodes_count: AtomicU32,
    used_dirs_count: AtomicU32,
    flags: AtomicU16,
    exclude_bitmap: AtomicU64,
    block_bitmap_checksum: AtomicTruncatedChecksum,
    inode_bitmap_checksum: AtomicTruncatedChecksum,
    unused_inodes_count: AtomicU32,
    checksum: AtomicU16,
}

#[expect(dead_code)]
impl BlockGroupDescriptor {
    pub(crate) const SIZE_IN_BYTES_ON_DISK_32: usize = 32;
    pub(crate) const SIZE_IN_BYTES_ON_DISK_64: usize = 64;
    const BG_CHECKSUM_OFFSET: usize = 0x1e;

    /// Parse a block group descriptor from raw bytes read from disk.
    ///
    /// # Panics
    /// If `bytes` is not at least [`Self::SIZE_IN_BYTES_ON_DISK`] bytes long.
    fn from_bytes(
        superblock: &Superblock,
        index: BlockGroupIndex,
        bytes: &[u8],
    ) -> Self {
        let is_64_bit = superblock
            .incompatible_features()
            .contains(IncompatibleFeatures::IS_64BIT);
        let block_bitmap = u64_from_hilo(
            if is_64_bit {
                read_u32le(bytes, 0x20)
            } else {
                0
            },
            read_u32le(bytes, 0x0),
        );
        let inode_bitmap = u64_from_hilo(
            if is_64_bit {
                read_u32le(bytes, 0x24)
            } else {
                0
            },
            read_u32le(bytes, 0x4),
        );
        let inode_table_first_block = u64_from_hilo(
            if is_64_bit {
                read_u32le(bytes, 0x28)
            } else {
                0
            },
            read_u32le(bytes, 0x8),
        );
        let free_blocks_count = u32_from_hilo(
            if is_64_bit {
                read_u16le(bytes, 0x2C)
            } else {
                0
            },
            read_u16le(bytes, 0xC),
        );
        let free_inodes_count = u32_from_hilo(
            if is_64_bit {
                read_u16le(bytes, 0x2E)
            } else {
                0
            },
            read_u16le(bytes, 0xE),
        );
        let used_dirs_count = u32_from_hilo(
            if is_64_bit {
                read_u16le(bytes, 0x30)
            } else {
                0
            },
            read_u16le(bytes, 0x10),
        );
        let flags = read_u16le(bytes, 0x12);
        let exclude_bitmap = u64_from_hilo(
            if is_64_bit {
                read_u32le(bytes, 0x34)
            } else {
                0
            },
            read_u32le(bytes, 0x14),
        );
        let block_bitmap_checksum = if is_64_bit {
            AtomicTruncatedChecksum::Full(AtomicU32::new(u32_from_hilo(
                read_u16le(bytes, 0x38),
                read_u16le(bytes, 0x18),
            )))
        } else {
            AtomicTruncatedChecksum::Truncated(AtomicU16::new(read_u16le(
                bytes, 0x18,
            )))
        };
        let inode_bitmap_checksum = if is_64_bit {
            AtomicTruncatedChecksum::Full(AtomicU32::new(u32_from_hilo(
                read_u16le(bytes, 0x3A),
                read_u16le(bytes, 0x1A),
            )))
        } else {
            AtomicTruncatedChecksum::Truncated(AtomicU16::new(read_u16le(
                bytes, 0x1A,
            )))
        };
        let unused_inodes_count = u32_from_hilo(
            if is_64_bit {
                read_u16le(bytes, 0x3C)
            } else {
                0
            },
            read_u16le(bytes, 0x1C),
        );
        let checksum = read_u16le(bytes, Self::BG_CHECKSUM_OFFSET);
        Self {
            index,
            is_64bit: is_64_bit,
            block_bitmap: AtomicU64::new(block_bitmap),
            inode_bitmap: AtomicU64::new(inode_bitmap),
            inode_table_first_block: AtomicU64::new(inode_table_first_block),
            free_blocks_count: AtomicU32::new(free_blocks_count),
            free_inodes_count: AtomicU32::new(free_inodes_count),
            used_dirs_count: AtomicU32::new(used_dirs_count),
            flags: AtomicU16::new(flags),
            exclude_bitmap: AtomicU64::new(exclude_bitmap),
            block_bitmap_checksum,
            inode_bitmap_checksum,
            unused_inodes_count: AtomicU32::new(unused_inodes_count),
            checksum: AtomicU16::new(checksum),
        }
    }

    fn to_bytes(&self) -> BlockGroupDescriptorBytes {
        let (block_bitmap_hi, block_bitmap_lo) =
            u64_to_hilo(self.block_bitmap.load(Ordering::Relaxed));
        let (inode_bitmap_hi, inode_bitmap_lo) =
            u64_to_hilo(self.inode_bitmap.load(Ordering::Relaxed));
        let (inode_table_first_block_hi, inode_table_first_block_lo) =
            u64_to_hilo(self.inode_table_first_block.load(Ordering::Relaxed));
        let (free_blocks_count_hi, free_blocks_count_lo) =
            u32_to_hilo(self.free_blocks_count.load(Ordering::Relaxed));
        let (free_inodes_count_hi, free_inodes_count_lo) =
            u32_to_hilo(self.free_inodes_count.load(Ordering::Relaxed));
        let (used_dirs_count_hi, used_dirs_count_lo) =
            u32_to_hilo(self.used_dirs_count.load(Ordering::Relaxed));
        let flags = self.flags.load(Ordering::Relaxed);
        let (exclude_bitmap_hi, exclude_bitmap_lo) =
            u64_to_hilo(self.exclude_bitmap.load(Ordering::Relaxed));
        let (block_bitmap_checksum_hi, block_bitmap_checksum_lo) =
            u32_to_hilo(match &self.block_bitmap_checksum {
                AtomicTruncatedChecksum::Truncated(c) => {
                    u32::from(c.load(Ordering::Relaxed))
                }
                AtomicTruncatedChecksum::Full(c) => c.load(Ordering::Relaxed),
            });
        let (inode_bitmap_checksum_hi, inode_bitmap_checksum_lo) =
            u32_to_hilo(match &self.inode_bitmap_checksum {
                AtomicTruncatedChecksum::Truncated(c) => {
                    u32::from(c.load(Ordering::Relaxed))
                }
                AtomicTruncatedChecksum::Full(c) => c.load(Ordering::Relaxed),
            });
        let (unused_inodes_count_hi, unused_inodes_count_lo) =
            u32_to_hilo(self.unused_inodes_count.load(Ordering::Relaxed));
        let checksum = self.checksum.load(Ordering::Relaxed);
        let update_32 = |bytes: &mut [u8]| {
            write_u32le(bytes, 0x0, block_bitmap_lo);
            write_u32le(bytes, 0x4, inode_bitmap_lo);
            write_u32le(bytes, 0x8, inode_table_first_block_lo);
            write_u16le(bytes, 0xC, free_blocks_count_lo);
            write_u16le(bytes, 0xE, free_inodes_count_lo);
            write_u16le(bytes, 0x10, used_dirs_count_lo);
            write_u16le(bytes, 0x12, flags);
            write_u32le(bytes, 0x14, exclude_bitmap_lo);
            write_u16le(bytes, 0x18, block_bitmap_checksum_lo);
            write_u16le(bytes, 0x1A, inode_bitmap_checksum_lo);
            write_u16le(bytes, 0x1C, unused_inodes_count_lo);
            write_u16le(bytes, 0x1E, checksum);
        };
        if self.is_64bit {
            let mut bytes = [0; Self::SIZE_IN_BYTES_ON_DISK_64];
            update_32(&mut bytes);
            write_u32le(&mut bytes, 0x20, block_bitmap_hi);
            write_u32le(&mut bytes, 0x24, inode_bitmap_hi);
            write_u32le(&mut bytes, 0x28, inode_table_first_block_hi);
            write_u16le(&mut bytes, 0x2C, free_blocks_count_hi);
            write_u16le(&mut bytes, 0x2E, free_inodes_count_hi);
            write_u16le(&mut bytes, 0x30, used_dirs_count_hi);
            write_u32le(&mut bytes, 0x34, exclude_bitmap_hi);
            write_u16le(&mut bytes, 0x38, block_bitmap_checksum_hi);
            write_u16le(&mut bytes, 0x3A, inode_bitmap_checksum_hi);
            write_u16le(&mut bytes, 0x3C, unused_inodes_count_hi);
            BlockGroupDescriptorBytes::OnDisk64(bytes)
        } else {
            let mut bytes = [0; Self::SIZE_IN_BYTES_ON_DISK_32];
            update_32(&mut bytes);
            BlockGroupDescriptorBytes::OnDisk32(bytes)
        }
    }

    fn update_checksum(&self, superblock: &Superblock) {
        if superblock
            .read_only_compatible_features()
            .contains(ReadOnlyCompatibleFeatures::METADATA_CHECKSUMS)
        {
            let mut checksum = Checksum::with_seed(superblock.checksum_seed());
            let bytes = self.to_bytes();
            checksum.update_u32_le(self.index);
            // Up to the checksum field.
            checksum.update(&bytes[..Self::BG_CHECKSUM_OFFSET]);
            // Zero'd checksum field.
            checksum.update_u16_le(0);
            // Rest of the block group descriptor.
            checksum.update(&bytes[Self::BG_CHECKSUM_OFFSET + 2..]);
            // Truncate to the lower 16 bits.
            let checksum = u16::try_from(checksum.finalize() & 0xffff).unwrap();
            self.checksum.store(checksum, Ordering::Relaxed);
        } else if superblock
            .read_only_compatible_features()
            .contains(ReadOnlyCompatibleFeatures::GROUP_DESCRIPTOR_CHECKSUMS)
        {
            unimplemented!(
                "Support for the GROUP_DESCRIPTOR_CHECKSUMS feature is not yet implemented"
            );
        }
    }

    pub(crate) fn block_bitmap_block(&self) -> FsBlockIndex {
        self.block_bitmap.load(Ordering::Relaxed)
    }

    pub(crate) fn inode_bitmap_block(&self) -> FsBlockIndex {
        self.inode_bitmap.load(Ordering::Relaxed)
    }

    pub(crate) fn inode_table_first_block(&self) -> FsBlockIndex {
        self.inode_table_first_block.load(Ordering::Relaxed)
    }

    pub(crate) fn free_blocks_count(&self) -> u32 {
        self.free_blocks_count.load(Ordering::Relaxed)
    }

    pub(crate) fn set_free_blocks_count(&self, count: u32) {
        self.free_blocks_count.store(count, Ordering::Relaxed);
    }

    pub(crate) fn free_inodes_count(&self) -> u32 {
        self.free_inodes_count.load(Ordering::Relaxed)
    }

    pub(crate) fn set_free_inodes_count(&self, count: u32) {
        self.free_inodes_count.store(count, Ordering::Relaxed);
    }

    pub(crate) fn unused_inodes_count(&self) -> u32 {
        self.unused_inodes_count.load(Ordering::Relaxed)
    }

    pub(crate) fn set_unused_inodes_count(&self, count: u32) {
        self.unused_inodes_count.store(count, Ordering::Relaxed);
    }

    pub(crate) fn used_dirs_count(&self) -> u32 {
        self.used_dirs_count.load(Ordering::Relaxed)
    }

    pub(crate) fn set_used_dirs_count(&self, count: u32) {
        self.used_dirs_count.store(count, Ordering::Relaxed);
    }

    pub(crate) fn block_bitmap_checksum(&self) -> TruncatedChecksum {
        (&self.block_bitmap_checksum).into()
    }

    pub(crate) fn set_block_bitmap_checksum(&self, checksum: u32) {
        self.block_bitmap_checksum.update(checksum);
    }

    pub(crate) fn inode_bitmap_checksum(&self) -> TruncatedChecksum {
        (&self.inode_bitmap_checksum).into()
    }

    pub(crate) fn set_inode_bitmap_checksum(&self, checksum: u32) {
        self.inode_bitmap_checksum.update(checksum);
    }

    fn checksum(&self) -> u16 {
        self.checksum.load(Ordering::Relaxed)
    }

    /// Map from a block group descriptor index to the absolute byte
    /// within the file where the descriptor starts.
    fn get_start_byte(
        sb: &Superblock,
        bgd_index: BlockGroupIndex,
    ) -> Option<u64> {
        let bgd_start_block: u32 = if sb.block_size() == 1024 { 2 } else { 1 };
        let bgd_per_block = sb
            .block_size()
            .to_u32()
            .checked_div(u32::from(sb.block_group_descriptor_size()))?;
        let block_index = bgd_start_block
            .checked_add(bgd_index.checked_div(bgd_per_block)?)?;
        let offset_within_block = (bgd_index.checked_rem(bgd_per_block)?)
            .checked_mul(u32::from(sb.block_group_descriptor_size()))?;

        u64::from(block_index)
            .checked_mul(sb.block_size().to_u64())?
            .checked_add(u64::from(offset_within_block))
    }

    /// Read a block group descriptor.
    async fn read(
        sb: &Superblock,
        reader: &mut dyn Ext4Read,
        bgd_index: BlockGroupIndex,
    ) -> Result<Self, Ext4Error> {
        // Allocate a byte vec to read the raw data into.
        let block_group_descriptor_size =
            usize::from(sb.block_group_descriptor_size());
        let mut data = vec![0; block_group_descriptor_size];

        let start = Self::get_start_byte(sb, bgd_index)
            .ok_or(CorruptKind::BlockGroupDescriptor(bgd_index))?;
        reader.read(start, &mut data).await.map_err(Ext4Error::Io)?;

        let block_group_descriptor = Self::from_bytes(sb, bgd_index, &data);

        let has_metadata_checksums = sb
            .read_only_compatible_features()
            .contains(ReadOnlyCompatibleFeatures::METADATA_CHECKSUMS);

        // Verify the descriptor checksum.
        if has_metadata_checksums {
            let mut checksum = Checksum::with_seed(sb.checksum_seed());
            checksum.update_u32_le(bgd_index);
            // Up to the checksum field.
            checksum.update(&data[..Self::BG_CHECKSUM_OFFSET]);
            // Zero'd checksum field.
            checksum.update_u16_le(0);
            // Rest of the block group descriptor.
            checksum.update(&data[Self::BG_CHECKSUM_OFFSET + 2..]);
            // Truncate to the lower 16 bits.
            let checksum = u16::try_from(checksum.finalize() & 0xffff).unwrap();

            if checksum != block_group_descriptor.checksum() {
                return Err(CorruptKind::BlockGroupDescriptorChecksum(
                    bgd_index,
                )
                .into());
            }
        } else if sb
            .read_only_compatible_features()
            .contains(ReadOnlyCompatibleFeatures::GROUP_DESCRIPTOR_CHECKSUMS)
        {
            // TODO: prior to general checksum metadata being added,
            // there was a separate feature just for block group
            // descriptors. Add support for that here.
        }
        // TODO: Check checksums for the block bitmap and inode bitmap

        Ok(block_group_descriptor)
    }

    pub(crate) async fn write(&self, ext4: &Ext4) -> Result<(), Ext4Error> {
        let start = Self::get_start_byte(&ext4.0.superblock, self.index)
            .ok_or(CorruptKind::BlockGroupDescriptor(self.index))?;
        self.update_checksum(&ext4.0.superblock);
        // Write only the data we've saved to avoid overwriting any unread info
        let writer = ext4.0.writer.as_ref().ok_or(Ext4Error::Readonly)?;
        writer
            .write(start, &self.to_bytes())
            .await
            .map_err(Ext4Error::Io)?;
        Ok(())
    }

    /// Read all block group descriptors.
    pub(crate) async fn read_all(
        sb: &Superblock,
        reader: &mut dyn Ext4Read,
    ) -> Result<Vec<Self>, Ext4Error> {
        let mut block_group_descriptors =
            Vec::with_capacity(usize_from_u32(sb.num_block_groups()));

        for bgd_index in 0..sb.num_block_groups() {
            let bgd = Self::read(sb, reader, bgd_index).await?;
            block_group_descriptors.push(bgd);
        }

        Ok(block_group_descriptors)
    }
}
