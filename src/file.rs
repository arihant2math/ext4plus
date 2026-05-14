// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
//! Module for reading and writing file data within an [`Ext4`] filesystem.
//!
//! This module provides the [`File`] struct, which represents an open file and
//! is similar in concept to [`std::fs::File`]. It also provides lower-level functions
//! for reading and writing bytes at specific offsets within a file, which are used
//! by the methods of [`File`] but can also be used directly if needed.

use crate::Ext4;
use crate::block_index::FileBlockIndex;
use crate::error::Ext4Error;
use crate::file_blocks::FileBlocks;
use crate::inode::Inode;
use crate::path::Path;
use crate::resolve::FollowSymlinks;
use crate::util::u64_from_usize;
use core::fmt::{self, Debug, Formatter};

const MAX_COALESCED_READ_BYTES: usize = 64 * 1024;

/// An open file within an [`Ext4`] filesystem.
pub struct File {
    fs: Ext4,
    inode: Inode,
    file_blocks: FileBlocks,

    /// Current byte offset within the file.
    position: u64,
}

impl File {
    /// Open the file at `path`.
    #[maybe_async::maybe_async]
    pub(crate) async fn open(
        fs: &Ext4,
        path: Path<'_>,
    ) -> Result<Self, Ext4Error> {
        let inode = fs.path_to_inode(path, FollowSymlinks::All).await?;
        if !inode.file_type().is_regular_file() {
            return Err(Ext4Error::IsASpecialFile);
        }

        Self::open_inode(fs, inode)
    }

    /// Open `inode`. Note that unlike `File::open`, this allows any
    /// type of `inode` to be opened, including directories and
    /// symlinks. This is used by `Ext4::read_inode_file`.
    pub fn open_inode(fs: &Ext4, inode: Inode) -> Result<Self, Ext4Error> {
        Ok(Self {
            fs: fs.clone(),
            position: 0,
            file_blocks: FileBlocks::from_inode(&inode, fs.clone())?,
            inode,
        })
    }

    /// Access the internal [`Inode`] for this file. This allows for reading metadata etc.
    #[must_use]
    pub fn inode(&self) -> &Inode {
        &self.inode
    }

    /// Mutable access to the internal [`Inode`] for this file. This allows for modifying metadata etc.
    /// Note that changes to the inode will not be persisted until [`Inode::write`] is called.
    pub fn inode_mut(&mut self) -> &mut Inode {
        &mut self.inode
    }

    /// Read bytes from the file into `buf`, returning how many bytes
    /// were read. The number may be smaller than the length of the
    /// input buffer.
    ///
    /// This advances the position of the file by the number of bytes
    /// read, so calling `read_bytes` repeatedly can be used to read the
    /// entire file.
    ///
    /// Returns `Ok(0)` if the end of the file has been reached.
    #[maybe_async::maybe_async]
    pub async fn read_bytes(
        &mut self,
        buf: &mut [u8],
    ) -> Result<usize, Ext4Error> {
        let bytes_read = read_at_inner(
            &self.fs,
            &self.inode,
            &self.file_blocks,
            buf,
            self.position,
        )
        .await?;
        self.position = add_to_file_offset(self.position, bytes_read)?;
        Ok(bytes_read)
    }

    /// Read bytes from the file at position `pos` into `buf`, returning how many bytes were read. The number may be smaller than the length of the input buffer.
    /// This does not change the position of the file.
    #[maybe_async::maybe_async]
    pub async fn read_bytes_at(
        &mut self,
        buf: &mut [u8],
        pos: u64,
    ) -> Result<usize, Ext4Error> {
        read_at_inner(&self.fs, &self.inode, &self.file_blocks, buf, pos).await
    }

    /// Write bytes from `buf` into the file, returning how many bytes
    /// were written. The number may be smaller than the length of the
    /// input buffer.
    #[maybe_async::maybe_async]
    pub async fn write_bytes(
        &mut self,
        buf: &[u8],
    ) -> Result<usize, Ext4Error> {
        let written = self
            .file_blocks
            .write_at(&mut self.inode, buf, self.position)
            .await?;
        self.position = add_to_file_offset(self.position, written)?;
        Ok(written)
    }

    /// Write bytes from `buf` into the file at position `pos`, returning how many bytes
    /// were written. The number may be smaller than the length of the
    /// input buffer.
    #[maybe_async::maybe_async]
    pub async fn write_bytes_at(
        &mut self,
        buf: &[u8],
        pos: u64,
    ) -> Result<usize, Ext4Error> {
        self.file_blocks.write_at(&mut self.inode, buf, pos).await
    }

    /// Truncate the file to `new_size` bytes.
    #[maybe_async::maybe_async]
    pub async fn truncate(&mut self, new_size: u64) -> Result<(), Ext4Error> {
        self.file_blocks.truncate(&mut self.inode, new_size).await
    }

    /// Claim `num_blocks` filesystem blocks for this file as uninitialized extents.
    ///
    /// Claimed blocks read back as zeroes until they are written. The file size is
    /// unchanged.
    ///
    /// This operation is only supported for files that use extents.
    #[maybe_async::maybe_async]
    pub async fn claim_uninitialized_blocks(
        &mut self,
        start_block: u32,
        num_blocks: u32,
    ) -> Result<(), Ext4Error> {
        self.file_blocks
            .claim_uninitialized_blocks(
                &mut self.inode,
                start_block,
                num_blocks,
            )
            .await
    }

    /// Free any still-uninitialized blocks in `[start_block, start_block + num_blocks)`.
    ///
    /// Initialized blocks are left intact. The file size is unchanged.
    ///
    /// This operation is only supported for files that use extents.
    #[maybe_async::maybe_async]
    pub async fn free_uninitialized_blocks(
        &mut self,
        start_block: u32,
        num_blocks: u32,
    ) -> Result<(), Ext4Error> {
        self.file_blocks
            .free_uninitialized_blocks(&mut self.inode, start_block, num_blocks)
            .await
    }

    /// Current position within the file.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Seek from the start of the file to `position`.
    ///
    /// Seeking past the end of the file is allowed.
    #[maybe_async::maybe_async]
    pub async fn seek_to(&mut self, position: u64) -> Result<(), Ext4Error> {
        self.position = position;

        Ok(())
    }

    /// Consume the `File`, returning the underlying `Inode`.
    #[must_use]
    pub fn into_inode(self) -> Inode {
        self.inode
    }
}

impl Debug for File {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("File")
            // Just show the index from `self.inode`, the full `Inode`
            // output is verbose.
            .field("inode", &self.inode.index)
            .field("position", &self.position)
            // Don't show all fields, as that would make the output less
            // readable.
            .finish_non_exhaustive()
    }
}

fn add_to_file_offset(offset: u64, delta: usize) -> Result<u64, Ext4Error> {
    offset
        .checked_add(u64_from_usize(delta))
        .ok_or(Ext4Error::FileTooLarge)
}

/// Read from `inode` into `buf` starting at `offset`, returning how many bytes were read.
/// The number may be smaller than the length of the input buffer if the read is only partially successful (e.g., due to reaching EOF).
#[maybe_async::maybe_async]
#[allow(clippy::arithmetic_side_effects)]
pub(crate) async fn read_at_inner(
    ext4: &Ext4,
    inode: &Inode,
    file_blocks: &FileBlocks,
    buf: &mut [u8],
    offset: u64,
) -> Result<usize, Ext4Error> {
    if buf.is_empty() {
        return Ok(0);
    }

    if offset >= inode.size_in_bytes() {
        return Ok(0);
    }

    let bytes_remaining = inode
        .size_in_bytes()
        .checked_sub(offset)
        .ok_or(Ext4Error::FileTooLarge)?;

    let len = match usize::try_from(bytes_remaining) {
        Ok(bytes_remaining) => core::cmp::min(buf.len(), bytes_remaining),
        Err(_) => buf.len(),
    };
    let buf = &mut buf[..len];

    let block_size = ext4.0.superblock.block_size().to_usize();
    let block_size_u64 = u64_from_usize(block_size);
    let mut filled = 0usize;
    let mut current_offset = offset;

    while filled < buf.len() {
        let offset_within_block =
            usize::try_from(current_offset % block_size_u64).unwrap();
        let block_index =
            FileBlockIndex::try_from(current_offset / block_size_u64).unwrap();
        let bytes_left = buf.len() - filled;
        let bytes_this_block = core::cmp::min(
            block_size.checked_sub(offset_within_block).unwrap(),
            bytes_left,
        );
        let max_additional_blocks =
            u32::try_from((bytes_left - bytes_this_block).div_ceil(block_size))
                .unwrap_or(u32::MAX);
        let (first_fs_block, run_blocks) = file_blocks
            .get_block_run(block_index, max_additional_blocks.saturating_add(1))
            .await?;
        let mut run_len = bytes_this_block
            .checked_add(
                core::cmp::min(
                    usize::try_from(run_blocks.saturating_sub(1))
                        .unwrap_or(usize::MAX),
                    (bytes_left - bytes_this_block) / block_size,
                )
                .checked_mul(block_size)
                .unwrap(),
            )
            .unwrap();
        run_len = core::cmp::min(run_len, MAX_COALESCED_READ_BYTES);

        if first_fs_block == 0 {
            buf[filled..filled + run_len].fill(0);
            filled += run_len;
            current_offset += u64_from_usize(run_len);
            continue;
        }

        ext4.0
            .reader
            .read(
                first_fs_block
                    .checked_mul(ext4.0.superblock.block_size().to_u64())
                    .ok_or(crate::error::CorruptKind::InvalidBlockSize)?
                    .checked_add(u64_from_usize(offset_within_block))
                    .ok_or(crate::error::CorruptKind::InvalidBlockSize)?,
                &mut buf[filled..filled + run_len],
            )
            .await
            .map_err(Ext4Error::Io)?;

        filled += run_len;
        current_offset += u64_from_usize(run_len);
    }

    Ok(len)
}

/// Read from `inode` into `buf` starting at `offset`, returning how many bytes were read.
/// The number may be smaller than the length of the input buffer if the read is only partially successful (e.g., due to reaching EOF).
#[maybe_async::maybe_async]
pub async fn read_at(
    ext4: &Ext4,
    inode: &Inode,
    buf: &mut [u8],
    offset: u64,
) -> Result<usize, Ext4Error> {
    let file_blocks = FileBlocks::from_inode(inode, ext4.clone())?;
    read_at_inner(ext4, inode, &file_blocks, buf, offset).await
}

/// Write `buf` into `inode` starting at `offset`, returning how many bytes were written.
/// The number may be smaller than the length of the input buffer if the write is only partially successful (e.g., due to lack of space).
#[maybe_async::maybe_async]
pub async fn write_at(
    ext4: &Ext4,
    inode: &mut Inode,
    buf: &[u8],
    offset: u64,
) -> Result<usize, Ext4Error> {
    let mut file_blocks = FileBlocks::from_inode(inode, ext4.clone())?;
    file_blocks.write_at(inode, buf, offset).await
}

/// Claim `num_blocks` filesystem blocks for `inode` as uninitialized extents.
///
/// Claimed blocks read back as zeroes until they are written. The inode size is
/// unchanged.
///
/// This operation is only supported for files that use extents.
#[maybe_async::maybe_async]
pub async fn claim_uninitialized_blocks(
    ext4: &Ext4,
    inode: &mut Inode,
    start_block: u32,
    num_blocks: u32,
) -> Result<(), Ext4Error> {
    let mut file_blocks = FileBlocks::from_inode(inode, ext4.clone())?;
    file_blocks
        .claim_uninitialized_blocks(inode, start_block, num_blocks)
        .await
}

/// Free any still-uninitialized blocks in `[start_block, start_block + num_blocks)`.
///
/// Initialized blocks are left intact. The inode size is unchanged.
///
/// This operation is only supported for files that use extents.
#[maybe_async::maybe_async]
pub async fn free_uninitialized_blocks(
    ext4: &Ext4,
    inode: &mut Inode,
    start_block: u32,
    num_blocks: u32,
) -> Result<(), Ext4Error> {
    let mut file_blocks = FileBlocks::from_inode(inode, ext4.clone())?;
    file_blocks
        .free_uninitialized_blocks(inode, start_block, num_blocks)
        .await
}

/// Truncate `inode` to `new_size` bytes, freeing blocks as necessary.
/// If `new_size` is larger than the current size, this just updates the size in the inode without allocating blocks
/// and the new blocks will be allocated on demand when writing to them.
#[maybe_async::maybe_async]
pub async fn truncate(
    ext4: &Ext4,
    inode: &mut Inode,
    new_size: u64,
) -> Result<(), Ext4Error> {
    let mut file_blocks = FileBlocks::from_inode(inode, ext4.clone())?;
    file_blocks.truncate(inode, new_size).await
}
