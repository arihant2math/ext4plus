// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::block_group::BlockGroupIndex;
use crate::block_index::FsBlockIndex;
use crate::checksum::Checksum;
use crate::error::{CorruptKind, Ext4Error};
use crate::file_blocks::FileBlocks;
use crate::file_type::FileType;
use crate::metadata::Metadata;
use crate::path::PathBuf;
use crate::superblock::Superblock;
use crate::util::{
    read_u16le, read_u32le, u32_from_hilo, u32_to_hilo, u64_from_hilo,
    u64_to_hilo, write_u16le, write_u32le,
};
use crate::{Ext4, IncompatibleFeatures};
use alloc::vec;
use alloc::vec::Vec;
use bitflags::bitflags;
use core::num::NonZeroU32;
use core::time::Duration;

/// Inode index.
///
/// This is always nonzero.
pub(crate) type InodeIndex = NonZeroU32;

/// Options for creating a new inode.
pub struct InodeCreationOptions {
    /// File type of the new inode.
    pub file_type: FileType,
    /// Mode bits of the new inode, should match file type.
    pub mode: InodeMode,
    /// User ID of the new inode.
    pub uid: u32,
    /// Group ID of the new inode.
    pub gid: u32,
    /// Creation, modification, and access time of the new inode.
    pub time: Duration,
    /// Inode flags for the new inode. EXTENTS is not supported and will be ignored if set.
    pub flags: InodeFlags,
}

bitflags! {
    /// Inode flags.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct InodeFlags: u32 {
        /// File is immutable.
        const IMMUTABLE = 0x10;

        /// Directory is encrypted.
        const DIRECTORY_ENCRYPTED = 0x800;

        /// Directory has hashed indexes.
        const DIRECTORY_HTREE = 0x1000;

        /// File is huge.
        const HUGE_FILE = 0x4_0000;

        /// Inode uses extents.
        const EXTENTS = 0x8_0000;

        /// Verity protected data.
        const VERITY = 0x10_0000;

        /// Inode stores a large extended attribute value in its data blocks.
        const EXTENDED_ATTRIBUTES = 0x20_0000;

        /// Inode has inline data.
        const INLINE_DATA = 0x1000_0000;

        // TODO: other flags
    }
}

bitflags! {
    /// Inode mode.
    ///
    /// The mode bitfield stores file permissions in the lower bits and
    /// file type in the upper bits.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
    pub struct InodeMode: u16 {
        /// Other execute permission.
        const S_IXOTH = 0x0001;
        /// Other write permission.
        const S_IWOTH = 0x0002;
        /// Other read permission.
        const S_IROTH = 0x0004;

        /// Group execute permission.
        const S_IXGRP = 0x0008;
        /// Group write permission.
        const S_IWGRP = 0x0010;
        /// Group read permission.
        const S_IRGRP = 0x0020;

        /// User execute permission.
        const S_IXUSR = 0x0040;
        /// User write permission.
        const S_IWUSR = 0x0080;
        /// User read permission.
        const S_IRUSR = 0x0100;

        /// Sticky bit.
        const S_ISVTX = 0x0200;

        /// Setgid bit.
        const S_ISGID = 0x0400;
        /// Setuid bit.
        const S_ISUID = 0x0800;

        // Mutually-exclusive file types:
        /// Named pipe (FIFO).
        const S_IFIFO = 0x1000;
        /// Character device.
        const S_IFCHR = 0x2000;
        /// Directory.
        const S_IFDIR = 0x4000;
        /// Block device.
        const S_IFBLK = 0x6000;
        /// Regular file.
        const S_IFREG = 0x8000;
        /// Symbolic link.
        const S_IFLNK = 0xA000;
        /// Socket.
        const S_IFSOCK = 0xC000;
    }
}

fn timestamp_to_duration(timestamp: u32, _high: Option<u32>) -> Duration {
    if timestamp == u32::MAX {
        panic!("timestamp overflow");
    }
    // TODO: nanosecond precision
    Duration::from_secs(u64::from(timestamp))
}

/// An inode within an Ext4 filesystem.
#[derive(Clone, Debug)]
pub struct Inode {
    /// This inode's index.
    /// This is constant, so it is safe to cache it and expose it as a public field.
    pub index: InodeIndex,

    /// Kept for backwards compatibility, because initialization can cause erroring out.
    file_type: FileType,

    /// Full inode data as read from disk.
    pub(crate) inode_data: Vec<u8>,

    /// Checksum seed used in various places.
    checksum_base: Checksum,
}

impl Inode {
    const INLINE_DATA_LEN: usize = 60;
    const L_I_CHECKSUM_LO_OFFSET: usize = 0x74 + 0x8;
    const I_CHECKSUM_HI_OFFSET: usize = 0x82;

    /// Load an inode from `bytes`.
    ///
    /// If successful, returns a tuple containing the inode and its
    /// checksum field.
    fn from_bytes(
        ext4: &Ext4,
        index: InodeIndex,
        data: &[u8],
    ) -> Result<(Self, u32), Ext4Error> {
        // Inodes must be at least 128 bytes.
        if data.len() < 128 {
            return Err(CorruptKind::InodeTruncated {
                inode: index,
                size: data.len(),
            }
            .into());
        }

        // If metadata checksums are enabled, the inode must be big
        // enough to include the checksum fields.
        if ext4.has_metadata_checksums()
            && data.len() < (Self::I_CHECKSUM_HI_OFFSET + 2)
        {
            return Err(CorruptKind::InodeTruncated {
                inode: index,
                size: data.len(),
            }
            .into());
        }

        let i_mode = read_u16le(data, 0x0);
        let i_generation = read_u32le(data, 0x64);
        let (l_i_checksum_lo, i_checksum_hi) = if ext4.has_metadata_checksums()
        {
            (
                read_u16le(data, Self::L_I_CHECKSUM_LO_OFFSET),
                read_u16le(data, Self::I_CHECKSUM_HI_OFFSET),
            )
        } else {
            // If metadata checksums aren't enabled then these values
            // aren't used; arbitrarily set to zero.
            (0, 0)
        };

        let checksum = u32_from_hilo(i_checksum_hi, l_i_checksum_lo);
        let mode = InodeMode::from_bits_retain(i_mode);

        let mut checksum_base =
            Checksum::with_seed(ext4.0.superblock.checksum_seed());
        checksum_base.update_u32_le(index.get());
        checksum_base.update_u32_le(i_generation);

        Ok((
            Self {
                index,
                file_type: FileType::try_from(mode).map_err(|_| {
                    CorruptKind::InodeFileType { inode: index, mode }
                })?,
                inode_data: data.to_vec(),
                checksum_base,
            },
            checksum,
        ))
    }

    /// Initialize a new inode with the given index and creation data, and write it to disk.
    /// Assumes that the caller has already allocated the inode and is passing in a valid index.
    pub(crate) async fn create(
        index: InodeIndex,
        inode_creation_data: InodeCreationOptions,
        ext4: &Ext4,
    ) -> Result<Self, Ext4Error> {
        let inode_data = vec![0; usize::from(ext4.0.superblock.inode_size())];
        let mut checksum_base =
            Checksum::with_seed(ext4.0.superblock.checksum_seed());
        checksum_base.update_u32_le(index.get());
        checksum_base.update_u32_le(0); // i_generation is zero for new inodes

        let mut inode = Self {
            index,
            file_type: inode_creation_data.file_type,
            inode_data,
            checksum_base,
        };

        inode.set_mode(inode_creation_data.mode)?;
        inode.set_uid(inode_creation_data.uid);
        inode.set_gid(inode_creation_data.gid);
        inode.set_size_in_bytes(0);
        inode.set_atime(inode_creation_data.time);
        inode.set_ctime(inode_creation_data.time);
        inode.set_mtime(inode_creation_data.time);
        inode.set_dtime(Duration::from_secs(0));
        inode.set_links_count(0);
        let mut flags = inode_creation_data.flags;
        if ext4
            .0
            .superblock
            .incompatible_features()
            .contains(IncompatibleFeatures::EXTENTS)
        {
            flags |= InodeFlags::EXTENTS;
        } else {
            flags &= !InodeFlags::EXTENTS;
        }
        inode.set_flags(flags);
        let blocks = FileBlocks::initialize(ext4.clone(), &inode)?;
        let inline_data = blocks.to_bytes()?;
        inode.set_inline_data(inline_data);
        inode.write(ext4).await?;
        Ok(inode)
    }

    /// Read an inode.
    pub async fn read(
        ext4: &Ext4,
        inode: InodeIndex,
    ) -> Result<Self, Ext4Error> {
        let (block_index, offset_within_block) =
            get_inode_location(ext4, inode)?;

        let mut data = vec![0; usize::from(ext4.0.superblock.inode_size())];
        ext4.read_from_block(block_index, offset_within_block, &mut data)
            .await?;

        let (inode, expected_checksum) = Self::from_bytes(ext4, inode, &data)?;

        // Verify the inode checksum.
        if ext4.has_metadata_checksums() {
            let mut checksum = inode.checksum_base.clone();

            // Hash all the inode data, but treat the two checksum
            // fields as zeroes.

            // Up to the l_i_checksum_lo field.
            checksum.update(&data[..Self::L_I_CHECKSUM_LO_OFFSET]);

            // Zero'd field.
            checksum.update_u16_le(0);

            // Up to the i_checksum_hi field.
            checksum.update(
                &data[Self::L_I_CHECKSUM_LO_OFFSET + 2
                    ..Self::I_CHECKSUM_HI_OFFSET],
            );

            // Zero'd field.
            checksum.update_u16_le(0);

            // Rest of the inode.
            checksum.update(&data[Self::I_CHECKSUM_HI_OFFSET + 2..]);

            let actual_checksum = checksum.finalize();
            if actual_checksum != expected_checksum {
                return Err(CorruptKind::InodeChecksum(inode.index).into());
            }
        }

        Ok(inode)
    }

    pub(crate) fn update_inode_data(&mut self, ext4: &Ext4) {
        if ext4.has_metadata_checksums() {
            let mut checksum = self.checksum_base.clone();
            // Up to the l_i_checksum_lo field.
            checksum.update(&self.inode_data[..Self::L_I_CHECKSUM_LO_OFFSET]);
            // Zero'd field.
            checksum.update_u16_le(0);
            // Up to the i_checksum_hi field.
            checksum.update(
                &self.inode_data[Self::L_I_CHECKSUM_LO_OFFSET + 2
                    ..Self::I_CHECKSUM_HI_OFFSET],
            );
            // Zero'd field.
            checksum.update_u16_le(0);
            // Rest of the inode.
            checksum.update(&self.inode_data[Self::I_CHECKSUM_HI_OFFSET + 2..]);
            let final_checksum = checksum.finalize();
            let (checksum_hi, checksum_lo) = u32_to_hilo(final_checksum);
            self.inode_data[Self::L_I_CHECKSUM_LO_OFFSET
                ..Self::L_I_CHECKSUM_LO_OFFSET + 2]
                .copy_from_slice(&checksum_lo.to_le_bytes());
            self.inode_data
                [Self::I_CHECKSUM_HI_OFFSET..Self::I_CHECKSUM_HI_OFFSET + 2]
                .copy_from_slice(&checksum_hi.to_le_bytes());
        }
    }

    /// Write the inode back to disk.
    pub async fn write(&mut self, ext4: &Ext4) -> Result<(), Ext4Error> {
        let (block_index, offset_within_block) =
            get_inode_location(ext4, self.index)?;
        let block_size = ext4.0.superblock.block_size().to_u64();
        let pos = block_index
            .checked_mul(block_size)
            .unwrap()
            .checked_add(u64::from(offset_within_block))
            .unwrap();
        self.update_inode_data(ext4);
        // Write only the data we've saved to avoid overwriting any unread info
        let writer = ext4.0.writer.as_ref().ok_or(Ext4Error::Readonly)?;
        writer
            .write(pos, &self.inode_data)
            .await
            .map_err(Ext4Error::Io)?;
        Ok(())
    }

    /// Get the target path of a symlink inode.
    pub async fn symlink_target(
        &self,
        ext4: &Ext4,
    ) -> Result<PathBuf, Ext4Error> {
        if !self.file_type.is_symlink() {
            return Err(Ext4Error::NotASymlink);
        }

        // An empty symlink target is not allowed.
        if self.size_in_bytes() == 0 {
            return Err(CorruptKind::SymlinkTarget(self.index).into());
        }

        // Symlink targets of up to 59 bytes are stored inline. Longer
        // targets are stored as regular file data.
        const MAX_INLINE_SYMLINK_LEN: u64 = 59;

        if self.size_in_bytes() <= MAX_INLINE_SYMLINK_LEN {
            // OK to unwrap since we checked the size above.
            let len = usize::try_from(self.size_in_bytes()).unwrap();
            let target = &self.inline_data()[..len];

            PathBuf::try_from(target)
                .map_err(|_| CorruptKind::SymlinkTarget(self.index).into())
        } else {
            let data = ext4.read_inode_file(self).await?;
            PathBuf::try_from(data)
                .map_err(|_| CorruptKind::SymlinkTarget(self.index).into())
        }
    }

    /// Get the number of blocks in the file.
    ///
    /// If the file size is not an even multiple of the block size,
    /// round up.
    ///
    /// # Errors
    ///
    /// Ext4 allows at most `2^32` blocks in a file. Returns
    /// `CorruptKind::TooManyBlocksInFile` if that limit is exceeded.
    pub fn file_size_in_blocks(&self, ext4: &Ext4) -> Result<u32, Ext4Error> {
        Ok(self
            .size_in_bytes()
            // Round up.
            .div_ceil(ext4.0.superblock.block_size().to_u64())
            // Ext4 allows at most `2^32` blocks in a file.
            .try_into()
            .map_err(|_| CorruptKind::TooManyBlocksInFile)?)
    }

    #[must_use]
    pub(crate) fn inline_data(&self) -> [u8; Self::INLINE_DATA_LEN] {
        // OK to unwrap: already checked the length.
        let i_block = self
            .inode_data
            .get(0x28..0x28 + Self::INLINE_DATA_LEN)
            .unwrap();
        // OK to unwrap, we know `i_block` is 60 bytes.
        i_block.try_into().unwrap()
    }

    pub(crate) fn set_inline_data(
        &mut self,
        data: [u8; Self::INLINE_DATA_LEN],
    ) {
        self.inode_data[0x28..0x28 + Self::INLINE_DATA_LEN]
            .copy_from_slice(&data);
    }

    /// Get the inode's mode bits.
    #[must_use]
    pub fn mode(&self) -> InodeMode {
        let i_mode = read_u16le(&self.inode_data, 0x0);
        InodeMode::from_bits_retain(i_mode)
    }

    /// Set the inode's mode bits.
    pub fn set_mode(&mut self, mode: InodeMode) -> Result<(), Ext4Error> {
        write_u16le(&mut self.inode_data, 0x0, mode.bits());
        self.file_type = FileType::try_from(mode).map_err(|_| {
            CorruptKind::InodeFileType {
                inode: self.index,
                mode,
            }
        })?;
        Ok(())
    }

    /// Get the file type based on the mode bits.
    #[must_use]
    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    /// Set the file type based on the mode bits.
    pub fn set_file_type(&mut self, file_type: FileType) {
        self.file_type = file_type;
    }

    /// Get the inode's user ID.
    #[must_use]
    pub fn uid(&self) -> u32 {
        let i_uid = read_u16le(&self.inode_data, 0x2);
        let l_i_uid_high = read_u16le(&self.inode_data, 0x74 + 0x4);
        u32_from_hilo(l_i_uid_high, i_uid)
    }

    /// Set the inode's user ID.
    pub fn set_uid(&mut self, uid: u32) {
        let (l_i_uid_high, i_uid) = u32_to_hilo(uid);
        write_u16le(&mut self.inode_data, 0x2, i_uid);
        write_u16le(&mut self.inode_data, 0x74 + 0x4, l_i_uid_high);
    }

    /// Get the inode's group ID.
    #[must_use]
    pub fn gid(&self) -> u32 {
        let i_gid = read_u16le(&self.inode_data, 0x18);
        let l_i_gid_high = read_u16le(&self.inode_data, 0x74 + 0x6);
        u32_from_hilo(l_i_gid_high, i_gid)
    }

    /// Set the inode's group ID.
    pub fn set_gid(&mut self, gid: u32) {
        let (l_i_gid_high, i_gid) = u32_to_hilo(gid);
        write_u16le(&mut self.inode_data, 0x18, i_gid);
        write_u16le(&mut self.inode_data, 0x74 + 0x6, l_i_gid_high);
    }

    /// Get the inode's size in bytes.
    #[must_use]
    pub fn size_in_bytes(&self) -> u64 {
        let i_size_lo = read_u32le(&self.inode_data, 0x4);
        let i_size_high = read_u32le(&self.inode_data, 0x6c);
        u64_from_hilo(i_size_high, i_size_lo)
    }

    /// Set the inode's size in bytes.
    pub fn set_size_in_bytes(&mut self, size_in_bytes: u64) {
        let (i_size_high, i_size_lo) = u64_to_hilo(size_in_bytes);
        write_u32le(&mut self.inode_data, 0x4, i_size_lo);
        write_u32le(&mut self.inode_data, 0x6c, i_size_high);
    }

    /// Get the inode's access time.
    #[must_use]
    pub fn atime(&self) -> Duration {
        let i_atime = read_u32le(&self.inode_data, 0x8);
        timestamp_to_duration(i_atime, None)
    }

    /// Set the inode's access time.
    pub fn set_atime(&mut self, atime: Duration) {
        let i_atime = atime.as_secs().try_into().unwrap_or(u32::MAX);
        write_u32le(&mut self.inode_data, 0x8, i_atime);
    }

    /// Get the inode's creation time.
    #[must_use]
    pub fn ctime(&self) -> Duration {
        let i_ctime = read_u32le(&self.inode_data, 0xc);
        timestamp_to_duration(i_ctime, None)
    }

    /// Set the inode's creation time.
    pub fn set_ctime(&mut self, ctime: Duration) {
        let i_ctime = ctime.as_secs().try_into().unwrap_or(u32::MAX);
        write_u32le(&mut self.inode_data, 0xc, i_ctime);
    }

    /// Get the inode's modification time.
    #[must_use]
    pub fn mtime(&self) -> Duration {
        let i_mtime = read_u32le(&self.inode_data, 0x10);
        timestamp_to_duration(i_mtime, None)
    }

    /// Set the inode's modification time.
    pub fn set_mtime(&mut self, mtime: Duration) {
        let i_mtime = mtime.as_secs().try_into().unwrap_or(u32::MAX);
        write_u32le(&mut self.inode_data, 0x10, i_mtime);
    }

    /// Get the inode's delete time.
    #[must_use]
    pub fn dtime(&self) -> Duration {
        let i_dtime = read_u32le(&self.inode_data, 0x14);
        timestamp_to_duration(i_dtime, None)
    }

    /// Set the inode's delete time.
    pub fn set_dtime(&mut self, dtime: Duration) {
        let i_dtime = dtime.as_secs().try_into().unwrap_or(u32::MAX);
        write_u32le(&mut self.inode_data, 0x14, i_dtime);
    }

    /// Get the inode's links count.
    #[must_use]
    pub fn links_count(&self) -> u16 {
        read_u16le(&self.inode_data, 0x1a)
    }

    /// Set the inode's links count.
    pub fn set_links_count(&mut self, links_count: u16) {
        write_u16le(&mut self.inode_data, 0x1a, links_count);
    }

    /// Get the inode's metadata.
    #[must_use]
    pub fn metadata(&self) -> Metadata {
        let i_mode = read_u16le(&self.inode_data, 0x0);
        let i_uid = read_u16le(&self.inode_data, 0x2);
        let i_size_lo = read_u32le(&self.inode_data, 0x4);
        let i_atime = read_u32le(&self.inode_data, 0x8);
        let i_ctime = read_u32le(&self.inode_data, 0xc);
        let i_mtime = read_u32le(&self.inode_data, 0x10);
        let i_dtime = read_u32le(&self.inode_data, 0x14);
        let i_gid = read_u16le(&self.inode_data, 0x18);
        let i_links_count = read_u16le(&self.inode_data, 0x1a);
        let i_size_high = read_u32le(&self.inode_data, 0x6c);
        let l_i_uid_high = read_u16le(&self.inode_data, 0x74 + 0x4);
        let l_i_gid_high = read_u16le(&self.inode_data, 0x74 + 0x6);
        let size_in_bytes = u64_from_hilo(i_size_high, i_size_lo);
        let uid = u32_from_hilo(l_i_uid_high, i_uid);
        let gid = u32_from_hilo(l_i_gid_high, i_gid);
        let mode = InodeMode::from_bits_retain(i_mode);

        Metadata {
            size_in_bytes,
            mode,
            uid,
            gid,
            atime: timestamp_to_duration(i_atime, None),
            ctime: timestamp_to_duration(i_ctime, None),
            dtime: timestamp_to_duration(i_dtime, None),
            file_type: self.file_type,
            mtime: timestamp_to_duration(i_mtime, None),
            links_count: i_links_count,
        }
    }

    pub(crate) fn checksum_base(&self) -> &Checksum {
        &self.checksum_base
    }

    /// Get the inode's flags.
    #[must_use]
    pub fn flags(&self) -> InodeFlags {
        let i_flags = read_u32le(&self.inode_data, 0x20);
        InodeFlags::from_bits_retain(i_flags)
    }

    /// Set the inode's flags.
    pub fn set_flags(&mut self, flags: InodeFlags) {
        // i_flags
        self.inode_data[0x20..0x24]
            .copy_from_slice(&flags.bits().to_le_bytes());
    }
}

pub(crate) fn get_inode_block_group_location(
    sb: &Superblock,
    inode: InodeIndex,
) -> Result<(BlockGroupIndex, u32), Ext4Error> {
    let inode_minus_1 = inode.get().checked_sub(1).unwrap();

    let block_group_index = inode_minus_1 / sb.inodes_per_block_group();
    let index_within_group = inode_minus_1 % sb.inodes_per_block_group();

    Ok((block_group_index, index_within_group))
}

/// Get an inode's location: block index and offset within that block.
/// Note that this is the location of the inode itself, not the file
/// data associated with the inode.
fn get_inode_location(
    ext4: &Ext4,
    inode: InodeIndex,
) -> Result<(FsBlockIndex, u32), Ext4Error> {
    let sb = &ext4.0.superblock;

    let (block_group_index, index_within_group) =
        get_inode_block_group_location(sb, inode)?;

    let group = ext4.get_block_group_descriptor(block_group_index);

    let err = || CorruptKind::InodeLocation {
        inode,
        block_group: block_group_index,
        inodes_per_block_group: sb.inodes_per_block_group(),
        inode_size: sb.inode_size(),
        block_size: sb.block_size(),
        inode_table_first_block: group.inode_table_first_block(),
    };

    let byte_offset_within_group = u64::from(index_within_group)
        .checked_mul(u64::from(sb.inode_size()))
        .ok_or_else(err)?;

    let byte_offset_of_group = sb
        .block_size()
        .to_u64()
        .checked_mul(group.inode_table_first_block())
        .ok_or_else(err)?;

    // Absolute byte index of the inode.
    let start_byte = byte_offset_of_group
        .checked_add(byte_offset_within_group)
        .ok_or_else(err)?;

    let block_index = start_byte / sb.block_size().to_nz_u64();
    let offset_within_block =
        u32::try_from(start_byte % sb.block_size().to_nz_u64())
            .map_err(|_| err())?;

    Ok((block_index, offset_within_block))
}
