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

use crate::block_index::FileBlockIndex;
use crate::error::Ext4Error;
use crate::extent::Extent;
use crate::file_blocks::FileBlocks;
use crate::inode::Inode;
use crate::path::Path;
use crate::resolve::FollowSymlinks;
use crate::util::{u64_from_usize, usize_from_u32};
use crate::{Ext4, InodeFlags, file_blocks};
use core::cmp::max;
use core::fmt::{self, Debug, Formatter};
use core::num::NonZeroU32;

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
        // OK to unwrap: the buffer length is capped such that this
        // calculation is at most the length of the file, which fits in
        // a `u64`.
        self.position = self
            .position
            .checked_add(u64_from_usize(bytes_read))
            .unwrap();
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
        let written =
            write_at(&self.fs, &mut self.inode, buf, self.position).await?;
        self.position = self
            .position
            .checked_add(u64::try_from(written).unwrap())
            .unwrap();
        // Update file blocks to reflect any changes from the write (e.g., new blocks allocated, extents split/merged, etc.).
        self.seek_to(self.position).await?;
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
        write_at(&self.fs, &mut self.inode, buf, pos).await
    }

    /// Truncate the file to `new_size` bytes.
    #[maybe_async::maybe_async]
    pub async fn truncate(&mut self, new_size: u64) -> Result<(), Ext4Error> {
        truncate(&self.fs, &mut self.inode, new_size).await?;
        Ok(())
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

/// Read from `inode` into `buf` starting at `offset`, returning how many bytes were read.
/// The number may be smaller than the length of the input buffer if the read is only partially successful (e.g., due to reaching EOF).
#[maybe_async::maybe_async]
pub(crate) async fn read_at_inner(
    ext4: &Ext4,
    inode: &Inode,
    file_blocks: &FileBlocks,
    mut buf: &mut [u8],
    offset: u64,
) -> Result<usize, Ext4Error> {
    // Nothing to do if output buffer is empty.
    if buf.is_empty() {
        return Ok(0);
    }

    // Nothing to do if already at the end of the file.
    if offset >= inode.size_in_bytes() {
        return Ok(0);
    }

    // Get the number of bytes remaining in the file, starting from
    // the current `position`.
    //
    // OK to unwrap: just checked that `position` is less than the
    // file size.
    let bytes_remaining =
        inode.metadata().size_in_bytes.checked_sub(offset).unwrap();

    // If the the number of bytes remaining is less than the output
    // buffer length, shrink the buffer.
    //
    // If the conversion to `usize` fails, the output buffer is
    // definitely not larger than the remaining bytes to read.
    if let Ok(bytes_remaining) = usize::try_from(bytes_remaining) {
        if buf.len() > bytes_remaining {
            buf = &mut buf[..bytes_remaining];
        }
    }

    let block_size = ext4.0.superblock.block_size();

    // Byte offset within the current block.
    //
    // OK to unwrap: block size fits in a `u32`, so an offset within
    // the block will as well.
    let offset_within_block: u32 =
        u32::try_from(offset % block_size.to_nz_u64()).unwrap();

    // OK to unwrap: `offset_within_block` is always less than or
    // equal to the block length.
    //
    // Note that if this block is at the end of the file, the block
    // may extend past the actual number of bytes in the file. This
    // does not matter because the output buffer's length was
    // already capped earlier against the number of bytes remaining
    // in the file.
    let bytes_remaining_in_block: u32 = block_size
        .to_u32()
        .checked_sub(offset_within_block)
        .unwrap();

    // If the output buffer is larger than the number of bytes
    // remaining in the block, shink the buffer.
    if buf.len() > usize_from_u32(bytes_remaining_in_block) {
        buf = &mut buf[..usize_from_u32(bytes_remaining_in_block)];
    }

    // Read the block data, or zeros if in a hole.
    let block_index = file_blocks
        .get_block(
            FileBlockIndex::try_from(offset / block_size.to_nz_u64()).unwrap(),
        )
        .await?;
    if block_index == 0 {
        buf.fill(0);
    } else {
        ext4.read_from_block(block_index, offset_within_block, buf)
            .await?;
    }

    Ok(buf.len())
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
    if inode.flags().contains(InodeFlags::IMMUTABLE) {
        return Err(Ext4Error::Readonly);
    }
    if inode.flags().contains(InodeFlags::EXTENTS) {
        write_at_extent(ext4, inode, buf, offset).await
    } else {
        write_at_block_map(ext4, inode, buf, offset).await
    }
}

#[maybe_async::maybe_async]
async fn write_at_block_map(
    ext4: &Ext4,
    inode: &mut Inode,
    buf: &[u8],
    offset: u64,
) -> Result<usize, Ext4Error> {
    let mut block_map =
        file_blocks::block_map::BlockMap::from_inode(inode, ext4.clone());
    let block_size = ext4.0.superblock.block_size();
    let start_block =
        FileBlockIndex::try_from(offset / block_size.to_nz_u64()).unwrap();
    let offset_in_block =
        usize::try_from(offset % block_size.to_nz_u64()).unwrap();
    let remaining_in_block =
        block_size.to_usize().checked_sub(offset_in_block).unwrap();
    if remaining_in_block > 0 {
        let to_write = core::cmp::min(buf.len(), remaining_in_block);
        let new_size = offset
            .checked_add(u64_from_usize(to_write))
            .ok_or(Ext4Error::FileTooLarge)?;
        let fs_block = match block_map.get_block(start_block).await? {
            0 => {
                // Hole: need to allocate a block.
                let new_fs_block = ext4.alloc_block(inode.index).await?;
                block_map.set_block(start_block, new_fs_block).await?;
                inode.set_blocks(
                    inode
                        .blocks()
                        .checked_add(1)
                        .ok_or(Ext4Error::FileTooLarge)?,
                );
                inode.set_inline_data(block_map.to_bytes());
                inode.write(ext4).await?;
                new_fs_block
            }
            fs_block => fs_block,
        };
        ext4.write_to_block(
            fs_block,
            u32::try_from(offset_in_block).unwrap(),
            &buf[..to_write],
        )
        .await?;
        if new_size > inode.size_in_bytes() {
            inode.set_size_in_bytes(new_size);
            inode.write(ext4).await?;
        }
        Ok(to_write)
    } else {
        let to_write = core::cmp::min(buf.len(), block_size.to_usize());
        let new_size = offset
            .checked_add(u64_from_usize(to_write))
            .ok_or(Ext4Error::FileTooLarge)?;
        let fs_block = match block_map
            .get_block(
                start_block.checked_add(1).ok_or(Ext4Error::FileTooLarge)?,
            )
            .await?
        {
            0 => {
                // Hole: need to allocate a block.
                let new_fs_block = ext4.alloc_block(inode.index).await?;
                block_map
                    .set_block(
                        start_block
                            .checked_add(1)
                            .ok_or(Ext4Error::FileTooLarge)?,
                        new_fs_block,
                    )
                    .await?;
                inode.set_blocks(inode.blocks().checked_add(1).unwrap());
                inode.set_inline_data(block_map.to_bytes());
                inode.write(ext4).await?;
                new_fs_block
            }
            fs_block => fs_block,
        };
        let write_buf = &buf[..to_write];
        ext4.write_to_block(fs_block, 0, write_buf).await?;
        if new_size > inode.size_in_bytes() {
            inode.set_size_in_bytes(new_size);
            inode.write(ext4).await?;
        }
        Ok(write_buf.len())
    }
}

#[maybe_async::maybe_async]
async fn write_at_extent(
    ext4: &Ext4,
    inode: &mut Inode,
    buf: &[u8],
    offset: u64,
) -> Result<usize, Ext4Error> {
    fn blocks_needed_for_bytes(
        offset_in_block: usize,
        bytes_remaining: usize,
        block_size: usize,
    ) -> usize {
        // Compute how many blocks are needed to write bytes_remaining starting at offset_in_block within the first block.
        if offset_in_block >= block_size {
            return 0; // Invalid offset, but treat as needing 0 blocks to avoid overflow
        }
        let first_block_capacity =
            block_size.checked_sub(offset_in_block).unwrap();
        if bytes_remaining <= first_block_capacity {
            1
        } else {
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "We check for offset_in_block >= block_size above, so first_block_capacity is always > 0, preventing overflow in div_ceil"
            )]
            {
                1 + (bytes_remaining - first_block_capacity)
                    .div_ceil(block_size)
            }
        }
    }

    #[maybe_async::maybe_async]
    fn bytes_for_blocks(
        num_blocks: usize,
        offset_in_block: usize,
        block_size: usize,
    ) -> usize {
        // Compute how many bytes correspond to num_blocks starting at offset_in_block.
        if num_blocks == 0 {
            return 0;
        }
        let first_block_capacity =
            block_size.checked_sub(offset_in_block).unwrap();
        if num_blocks == 1 {
            return first_block_capacity.min(block_size);
        }
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "We check for offset_in_block >= block_size above, so first_block_capacity is always > 0, preventing overflow in the multiplication"
        )]
        {
            first_block_capacity + (num_blocks - 1) * block_size
        }
    }

    #[maybe_async::maybe_async]
    async fn write_into_mapped_initialized_extent(
        ext4: &Ext4,
        extent: &Extent,
        offset_in_extent: usize,
        run_blocks: usize,
        buf: &[u8],
        offset_in_block: usize,
        block_size: usize,
    ) -> Result<usize, Ext4Error> {
        if buf.is_empty() || run_blocks == 0 {
            return Ok(0);
        }

        let extent_blocks = usize::from(extent.num_blocks);
        let blocks_to_write = core::cmp::min(
            run_blocks,
            extent_blocks.saturating_sub(offset_in_extent),
        );
        if blocks_to_write == 0 {
            return Ok(0);
        }

        let mut written = 0usize;

        for i in 0..blocks_to_write {
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "Extent start + offset stays within u64 for valid filesystems"
            )]
            let fs_block =
                extent.start_block + u64_from_usize(offset_in_extent + i);

            let (block_off, cap) = if i == 0 {
                (
                    offset_in_block,
                    block_size.checked_sub(offset_in_block).unwrap(),
                )
            } else {
                (0usize, block_size)
            };

            let remaining = buf.len().saturating_sub(written);
            let take = core::cmp::min(remaining, cap);
            if take == 0 {
                break;
            }

            // Full block write fast path.
            if block_off == 0 && take == block_size {
                ext4.write_to_block(
                    fs_block,
                    0,
                    &buf[written..written.checked_add(take).unwrap()],
                )
                .await?;
                written = written.checked_add(take).unwrap();
                continue;
            }

            // Read-modify-write for partial block(s) to preserve existing bytes.
            let mut block_buf = alloc::vec![0u8; block_size];
            ext4.read_from_block(fs_block, 0, &mut block_buf).await?;
            block_buf[block_off..block_off.checked_add(take).unwrap()]
                .copy_from_slice(
                    &buf[written..written.checked_add(take).unwrap()],
                );
            ext4.write_to_block(fs_block, 0, &block_buf).await?;

            written = written.checked_add(take).unwrap();
        }

        Ok(written)
    }

    #[expect(clippy::too_many_arguments)]
    #[maybe_async::maybe_async]
    async fn write_into_uninitialized_extent(
        _ext4: &Ext4,
        _inode: &Inode,
        _extent: &Extent,
        _offset_in_extent: usize,
        _run_blocks: usize,
        _buf: &[u8],
        _offset_in_block: usize,
        _block_size: usize,
    ) -> Result<usize, Ext4Error> {
        // For an uninitialized (unwritten) extent, we need to allocate blocks and mark them initialized.
        // For full-blocks, we can directly write and then flip to initialized.
        // For partial blocks, we must zero the unwritten parts to preserve the semantics of uninitialized extents.
        // This helper should handle both cases and return the number of bytes written.
        unimplemented!()
    }

    #[maybe_async::maybe_async]
    async fn write_into_newly_allocated_extent(
        ext4: &Ext4,
        extent: &Extent,
        offset_in_block: usize,
        buf: &[u8],
        block_size: usize,
    ) -> Result<usize, Ext4Error> {
        // Contract:
        // - `extent` describes newly allocated blocks (no prior file data).
        // - We must write `buf` starting at `offset_in_block` within the first block.
        // - Any bytes in the allocated blocks not covered by `buf` must be zeroed, so
        //   we don't expose stale disk contents.
        // - Returns the number of bytes from `buf` written.

        if buf.is_empty() {
            return Ok(0);
        }
        if offset_in_block >= block_size {
            return Ok(0);
        }

        // How many blocks from this extent are needed to store `buf` starting at
        // `offset_in_block` in the first block.
        let first_block_capacity =
            block_size.checked_sub(offset_in_block).unwrap();
        let needed_blocks = if buf.len() <= first_block_capacity {
            1usize
        } else {
            1usize
                .checked_add(
                    (buf.len().checked_sub(first_block_capacity).unwrap())
                        .div_ceil(block_size),
                )
                .unwrap()
        };

        // Caller should only pass a slice that fits in the allocated extent, but be
        // defensive. (Also handles weird zero-length extents.)
        let extent_blocks = usize::from(extent.num_blocks);
        let blocks_to_write = core::cmp::min(needed_blocks, extent_blocks);
        if blocks_to_write == 0 {
            return Ok(0);
        }

        let mut written = 0usize;

        for i in 0..blocks_to_write {
            // Filesystem block corresponding to the i'th block in this extent.
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "Extent start + offset stays within u64 for valid filesystems"
            )]
            let fs_block = extent.start_block + u64_from_usize(i);

            let (block_offset, capacity) = if i == 0 {
                (
                    offset_in_block,
                    block_size.checked_sub(offset_in_block).unwrap(),
                )
            } else {
                (0usize, block_size)
            };

            // Bytes from `buf` that go into this block.
            let remaining = buf.len().saturating_sub(written);
            let take = core::cmp::min(remaining, capacity);
            if take == 0 {
                break;
            }

            let chunk = &buf[written..written.checked_add(take).unwrap()];

            // If this chunk doesn't fill the entire block from offset 0..block_size,
            // we must write a full block with zeros everywhere else.
            let is_full_block_write = block_offset == 0 && take == block_size;

            if is_full_block_write {
                ext4.write_to_block(fs_block, 0, chunk).await?;
            } else {
                // Zero-fill and place the payload at block_offset.
                let mut block_buf = alloc::vec![0u8; block_size];
                block_buf
                    [block_offset..block_offset.checked_add(take).unwrap()]
                    .copy_from_slice(chunk);
                ext4.write_to_block(fs_block, 0, &block_buf).await?;
            }

            written = written.checked_add(take).unwrap();
        }

        Ok(written)
    }

    let block_size = ext4.0.superblock.block_size();
    if buf.is_empty() {
        return Ok(0);
    }

    let start_block =
        FileBlockIndex::try_from(offset / block_size.to_nz_u64()).unwrap();
    let mut start_offset_in_block =
        usize::try_from(offset % block_size.to_nz_u64()).unwrap();
    let mut bytes_remaining = buf.len();
    let mut buf_pos = 0usize;
    let mut current_block = start_block;
    let mut total_written = 0usize;
    let mut extent_tree =
        file_blocks::extent_tree::ExtentTree::from_inode(inode, ext4.clone())?;

    while bytes_remaining > 0 {
        let opt_extent = extent_tree.find_extent(current_block).await?;

        match opt_extent {
            Some(extent) => {
                // extent covers a range of file blocks
                let extent_block_start = extent.block_within_file;
                let extent_block_len = u64::from(extent.num_blocks);
                let offset_in_extent =
                    current_block.checked_sub(extent_block_start).unwrap();
                // number of blocks available in this extent starting at current_block
                let avail_blocks_in_extent = usize::try_from(
                    extent_block_len
                        .checked_sub(u64::from(offset_in_extent))
                        .unwrap(),
                )
                .unwrap();

                // determine how many bytes we can handle within this extent in a single run:
                // convert bytes_remaining + start_offset_in_block... but simpler: compute how many file-blocks
                // we can process here: `run_blocks = min(avail_blocks_in_extent, blocks_covered_by_bytes_remaining)`
                let max_blocks_needed = blocks_needed_for_bytes(
                    start_offset_in_block,
                    bytes_remaining,
                    block_size.to_usize(),
                );
                // Only attempt to handle as many blocks as we have bytes for.
                let max_blocks_for_remaining_bytes = blocks_needed_for_bytes(
                    start_offset_in_block,
                    bytes_remaining,
                    block_size.to_usize(),
                );
                let run_blocks = core::cmp::min(
                    avail_blocks_in_extent,
                    core::cmp::min(
                        max_blocks_needed,
                        max_blocks_for_remaining_bytes,
                    ),
                );

                // prepare to write run_blocks starting at current_block
                let want_bytes = bytes_for_blocks(
                    run_blocks,
                    start_offset_in_block,
                    block_size.to_usize(),
                );
                let slice_len = core::cmp::min(want_bytes, bytes_remaining);

                if extent.is_initialized {
                    // case A: initialized extent -> RMW for partial block at boundaries, direct write for full blocks
                    total_written = total_written
                        .checked_add(
                            write_into_mapped_initialized_extent(
                                ext4,
                                &extent,
                                usize_from_u32(offset_in_extent),
                                run_blocks,
                                &buf[buf_pos
                                    ..buf_pos.checked_add(slice_len).unwrap()],
                                start_offset_in_block,
                                block_size.to_usize(),
                            )
                            .await?,
                        )
                        .unwrap();
                } else {
                    // case B: uninitialized (unwritten) extent
                    // For full-blocks: we can directly write blocks and then flip to initialized.
                    // For partial blocks: we must zero the rest of the block(s) we don't overwrite.
                    total_written = total_written
                        .checked_add(
                            write_into_uninitialized_extent(
                                ext4,
                                inode,
                                &extent,
                                usize_from_u32(offset_in_extent),
                                run_blocks,
                                &buf[buf_pos
                                    ..buf_pos.checked_add(slice_len).unwrap()],
                                start_offset_in_block,
                                block_size.to_usize(),
                            )
                            .await?,
                        )
                        .unwrap();
                    // this helper must split the extent and mark the written blocks initialized
                }

                // advance
                // Advance based on what we actually wrote from `buf`.
                let advanced_bytes = slice_len;
                bytes_remaining =
                    bytes_remaining.checked_sub(advanced_bytes).unwrap();
                buf_pos = buf_pos.checked_add(advanced_bytes).unwrap();
                current_block = FileBlockIndex::try_from(
                    (offset.checked_add(u64_from_usize(buf_pos)).unwrap())
                        / block_size.to_nz_u64(),
                )
                .unwrap();
                start_offset_in_block = usize::try_from(
                    (offset.checked_add(u64_from_usize(buf_pos)).unwrap())
                        % block_size.to_nz_u64(),
                )
                .unwrap();
            }
            None => {
                // case C: hole -> allocate new blocks, create initialized extents and write
                // Decide how many blocks to allocate: prefer as many contiguous full-blocks as possible.
                // Map bytes_remaining -> needed_blocks (including first partial block)
                let needed_blocks = blocks_needed_for_bytes(
                    start_offset_in_block,
                    bytes_remaining,
                    block_size.to_usize(),
                );

                // Try to allocate needed_blocks. If allocation fails for full amount, try smaller (but >0).
                let mut tried_blocks = needed_blocks;
                let start_fs_block = loop {
                    match ext4
                        .alloc_contiguous_blocks(
                            inode.index,
                            NonZeroU32::new(
                                u32::try_from(tried_blocks).unwrap(),
                            )
                            .unwrap(),
                        )
                        .await
                    {
                        Ok(start_fs) => break start_fs,
                        Err(_) => {
                            if tried_blocks == 0 {
                                return Ok(total_written);
                            }
                            #[expect(
                                clippy::arithmetic_side_effects,
                                reason = "We check for tried_blocks == 0 above"
                            )]
                            {
                                tried_blocks /= tried_blocks; // or tried_blocks - 1
                            }
                            if tried_blocks == 0 {
                                return Ok(total_written);
                            }
                        }
                    }
                };
                // Insert extent: file-blocks [current_block, current_block + tried_blocks) -> FS blocks [start_fs_block, ...]
                let new_extent = Extent::new(
                    current_block,
                    start_fs_block,
                    u16::try_from(tried_blocks).unwrap(),
                );
                extent_tree.insert_extent(new_extent).await?;
                inode.set_blocks(
                    inode
                        .blocks()
                        .checked_add(u64_from_usize(tried_blocks))
                        .unwrap(),
                );
                // Write data into the newly allocated blocks (same logic as initialized extents except we don't need to read old content)
                // If first or last block is partial, zero the unwritten parts.
                let want_bytes = bytes_for_blocks(
                    tried_blocks,
                    start_offset_in_block,
                    block_size.to_usize(),
                );
                let have_bytes = bytes_remaining;
                let slice_len = core::cmp::min(want_bytes, have_bytes);
                total_written = total_written
                    .checked_add(
                        write_into_newly_allocated_extent(
                            ext4,
                            &new_extent,
                            start_offset_in_block,
                            &buf[buf_pos
                                ..buf_pos.checked_add(slice_len).unwrap()],
                            block_size.to_usize(),
                        )
                        .await?,
                    )
                    .unwrap();

                // advance variables
                // We must advance based on how many bytes we actually wrote into the
                // newly allocated blocks. Advancing by the theoretical block capacity
                // can underflow `bytes_remaining` when `slice_len` is smaller.
                let advanced_bytes = slice_len;
                bytes_remaining =
                    bytes_remaining.checked_sub(advanced_bytes).unwrap();
                buf_pos = buf_pos.checked_add(advanced_bytes).unwrap();
                current_block = FileBlockIndex::try_from(
                    (offset.checked_add(u64_from_usize(buf_pos)).unwrap())
                        / (block_size.to_nz_u64()),
                )
                .unwrap();
                start_offset_in_block = usize::try_from(
                    (offset.checked_add(u64_from_usize(buf_pos)).unwrap())
                        % block_size.to_nz_u64(),
                )
                .unwrap();
            }
        }

        extent_tree.try_merge_adjacent(current_block).await?;
    }

    inode.set_size_in_bytes(max(
        inode.size_in_bytes(),
        offset.checked_add(u64_from_usize(total_written)).unwrap(),
    ));
    inode.set_inline_data(extent_tree.to_bytes()?);
    inode.write(ext4).await?;

    Ok(total_written)
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
    let old_size = inode.size_in_bytes();
    if new_size == old_size {
        return Ok(());
    }

    if new_size > old_size {
        inode.set_size_in_bytes(new_size);
        inode.write(ext4).await?;
    } else {
        let block_size_nz = ext4.0.superblock.block_size().to_nz_u64();
        let block_size_u64: u64 = block_size_nz.get();

        // Compute file-block range to drop: [drop_from, old_blocks)
        let old_blocks: u64 = old_size.div_ceil(block_size_u64);
        let new_blocks: u64 = new_size.div_ceil(block_size_u64);

        if new_blocks < old_blocks {
            let drop_from = FileBlockIndex::try_from(new_blocks).unwrap();
            let drop_count: u32 =
                u32::try_from(old_blocks.checked_sub(new_blocks).unwrap())
                    .map_err(|_| Ext4Error::FileTooLarge)?;

            if inode.flags().contains(InodeFlags::EXTENTS) {
                let mut extent_tree =
                    file_blocks::extent_tree::ExtentTree::from_inode(
                        inode,
                        ext4.clone(),
                    )?;
                let freed = extent_tree
                    .remove_extent_range(drop_from, drop_count)
                    .await?;

                // Persist modified extent tree in the inode before freeing blocks.
                inode.set_inline_data(extent_tree.to_bytes()?);

                for (start, len) in freed {
                    if start == 0 || len == 0 {
                        continue;
                    }
                    if let Some(nz) = NonZeroU32::new(len) {
                        // Best-effort: contiguous free within a block group.
                        // If this ever spans groups, free_blocks will error; fall back to single frees.
                        if ext4.free_blocks(start, nz).await.is_err() {
                            for i in 0..len {
                                ext4.free_block(
                                    start.checked_add(u64::from(i)).unwrap(),
                                )
                                .await?;
                            }
                        }
                    }
                }
            } else {
                let mut block_map =
                    file_blocks::block_map::BlockMap::from_inode(
                        inode,
                        ext4.clone(),
                    );
                let freed =
                    block_map.remove_range(drop_from, drop_count).await?;
                inode.set_inline_data(block_map.to_bytes());
                for blk in freed {
                    if blk != 0 {
                        ext4.free_block(blk).await?;
                    }
                }
            }
        }

        // Update size last.
        inode.set_size_in_bytes(new_size);
        inode.write(ext4).await?;
    }

    Ok(())
}
