// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
//! Exposes directory-related functionality, including reading and modifying directory entries.

use crate::Ext4;
use crate::dir_block::DirBlock;
use crate::dir_entry::{DirEntry, DirEntryName};
use crate::dir_entry_hash::HashAlg;
use crate::dir_htree::{
    advance_leaf_lookup_to_next, get_dir_entry_via_htree,
    insert_child_into_parent, split_index_path_for_new_child,
};
use crate::error::{CorruptKind, Ext4Error};
use crate::file::{truncate, write_at};
use crate::file_type::FileType;
use crate::inode::{Inode, InodeFlags, InodeIndex};
#[cfg(not(feature = "sync"))]
use crate::iters::AsyncIterator;
use crate::iters::file_blocks::FileBlocks;
use crate::iters::read_dir::ReadDir;
use crate::path::PathBuf;
use crate::sync::PtrPrimitive;
use crate::util::write_u32le;
use crate::util::{read_u16le, read_u32le, write_u16le};
use alloc::vec;
use alloc::vec::Vec;

/// Search a directory inode for an entry with the given `name`. If
/// found, return the entry's inode, otherwise return a `NotFound`
/// error.
#[maybe_async::maybe_async]
pub(crate) async fn get_dir_entry_inode_by_name(
    fs: &Ext4,
    dir_inode: &Inode,
    name: DirEntryName<'_>,
) -> Result<Inode, Ext4Error> {
    assert!(dir_inode.file_type().is_dir());

    if dir_inode.flags().contains(InodeFlags::DIRECTORY_ENCRYPTED) {
        return Err(Ext4Error::Encrypted);
    }

    if dir_inode.flags().contains(InodeFlags::DIRECTORY_HTREE) {
        let entry = get_dir_entry_via_htree(fs, dir_inode, name).await?;
        return Inode::read(fs, entry.inode).await;
    }

    // The entry's `path()` method is not called, so the value of the
    // base path does not matter.
    let path = PathBuf::empty();

    let mut iter = ReadDir::new(fs.clone(), dir_inode, path)?;
    while let Some(entry) = iter.next().await {
        let entry = entry?;
        if entry.file_name() == name {
            return Inode::read(fs, entry.inode).await;
        }
    }

    Err(Ext4Error::NotFound)
}

#[inline]
fn dir_entry_error(inode: InodeIndex) -> Ext4Error {
    CorruptKind::DirEntry(inode).into()
}

#[inline]
fn checked_add_usize(
    lhs: usize,
    rhs: usize,
    inode: InodeIndex,
) -> Result<usize, Ext4Error> {
    lhs.checked_add(rhs).ok_or_else(|| dir_entry_error(inode))
}

#[inline]
fn checked_sub_usize(
    lhs: usize,
    rhs: usize,
    inode: InodeIndex,
) -> Result<usize, Ext4Error> {
    lhs.checked_sub(rhs).ok_or_else(|| dir_entry_error(inode))
}

#[inline]
fn checked_add_u64(
    lhs: u64,
    rhs: u64,
    inode: InodeIndex,
) -> Result<u64, Ext4Error> {
    lhs.checked_add(rhs).ok_or_else(|| dir_entry_error(inode))
}

#[inline]
fn checked_mul_u64(
    lhs: u64,
    rhs: u64,
    inode: InodeIndex,
) -> Result<u64, Ext4Error> {
    lhs.checked_mul(rhs).ok_or_else(|| dir_entry_error(inode))
}

/// Add an item to a directory
///
/// This edits directory entry bytes in-place and will error with
/// [`Ext4Error::Readonly`] if it would require allocating a new block.
#[maybe_async::maybe_async]
pub(crate) async fn add_dir_entry(
    fs: &Ext4,
    dir_inode: &mut Inode,
    name: DirEntryName<'_>,
    inode: InodeIndex,
    file_type: FileType,
) -> Result<(), Ext4Error> {
    assert!(dir_inode.file_type().is_dir());

    if dir_inode.flags().contains(InodeFlags::DIRECTORY_ENCRYPTED) {
        return Err(Ext4Error::Encrypted);
    }
    if dir_inode.flags().contains(InodeFlags::DIRECTORY_HTREE) {
        return add_dir_entry_htree(fs, dir_inode, name, inode, file_type)
            .await;
    }

    // Fail if name already exists.
    if get_dir_entry_inode_by_name(fs, dir_inode, name)
        .await
        .is_ok()
    {
        return Err(Ext4Error::AlreadyExists);
    }

    let block_size = fs.0.superblock.block_size().to_usize();
    let mut file_blocks = FileBlocks::new(fs.clone(), dir_inode)?;

    let need = dir_entry_min_size(name.as_ref().len(), dir_inode.index)?;
    let mut block_buf = vec![0u8; block_size];
    let mut is_first = true;

    while let Some(block_index_res) = file_blocks.next().await {
        let block_index = block_index_res?;
        fs.read_from_block(block_index, 0, &mut block_buf).await?;

        // Walk entries in this block looking for usable slack space.
        let mut off = 0usize;
        while off < block_size {
            let inode_field = read_u32le(&block_buf, off);
            let rec_len_offset = checked_add_usize(off, 4, dir_inode.index)?;
            let rec_len = read_u16le(&block_buf, rec_len_offset);
            let rec_len_usize = usize::from(rec_len);
            let rec_end =
                checked_add_usize(off, rec_len_usize, dir_inode.index)?;

            if rec_len_usize < 8 || rec_end > block_size {
                return Err(dir_entry_error(dir_inode.index));
            }

            // `inode == 0` indicates "special" entry or unused; treat it as fully free.
            let used = if inode_field == 0 {
                0usize
            } else {
                let name_len_offset =
                    checked_add_usize(off, 6, dir_inode.index)?;
                let name_len = usize::from(block_buf[name_len_offset]);
                dir_entry_min_size(name_len, dir_inode.index)?
            };

            let required = checked_add_usize(used, need, dir_inode.index)?;
            if rec_len_usize >= required {
                // Shrink current entry to its minimal size (or keep 0 if unused),
                // and place the new entry in the leftover space.
                let new_rec_len_for_curr =
                    if inode_field == 0 { 0usize } else { used };
                let free_start = checked_add_usize(
                    off,
                    new_rec_len_for_curr,
                    dir_inode.index,
                )?;
                let free_len = checked_sub_usize(
                    rec_len_usize,
                    new_rec_len_for_curr,
                    dir_inode.index,
                )?;

                if free_len < need {
                    // Shouldn't happen due to earlier check, but keep safe.
                    off = rec_end;
                    continue;
                }

                let rec_len_to_write = if inode_field != 0 {
                    new_rec_len_for_curr
                } else {
                    rec_len_usize
                };
                write_u16le(
                    &mut block_buf,
                    rec_len_offset,
                    u16::try_from(rec_len_to_write)
                        .map_err(|_| dir_entry_error(dir_inode.index))?,
                );

                // Write the new entry.
                write_dir_entry_bytes(
                    &mut block_buf,
                    free_start,
                    free_len,
                    inode,
                    name,
                    file_type,
                )?;

                // If metadata checksums are enabled, update the directory block checksum tail.
                DirBlock {
                    fs,
                    block_index,
                    is_first,
                    dir_inode: dir_inode.index,
                    has_htree: false,
                    checksum_base: dir_inode.checksum_base().clone(),
                }
                .update_checksum(&mut block_buf)?;

                // Write the block back.
                fs.write_to_block(block_index, 0, &block_buf).await?;
                return Ok(());
            }

            off = rec_end;
        }

        is_first = false;
    }

    let mut new_block_buf = vec![0u8; block_size];

    let tail_size = if fs.has_metadata_checksums() {
        12usize
    } else {
        0usize
    };
    let usable = checked_sub_usize(block_size, tail_size, dir_inode.index)?;

    if need > usable {
        return Err(dir_entry_error(dir_inode.index));
    }

    // New entry.
    write_dir_entry_bytes(
        &mut new_block_buf,
        0,
        usable,
        inode,
        name,
        file_type,
    )?;

    if fs.has_metadata_checksums() {
        let checksum_start =
            checked_sub_usize(block_size, 12, dir_inode.index)?;
        let checksum_tail_offset =
            checked_add_usize(checksum_start, 4, dir_inode.index)?;
        write_u32le(&mut new_block_buf, checksum_start, 0);
        let tail_val = 12u32 | (0xDE << 24);
        write_u32le(&mut new_block_buf, checksum_tail_offset, tail_val);
        DirBlock {
            fs,
            block_index: 0,
            is_first: false,
            dir_inode: dir_inode.index,
            has_htree: false,
            checksum_base: dir_inode.checksum_base().clone(),
        }
        .update_checksum(&mut new_block_buf)?;
    }

    let n = write_at(fs, dir_inode, &new_block_buf, dir_inode.size_in_bytes())
        .await?;
    if n != new_block_buf.len() {
        return Err(Ext4Error::NoSpace);
    }

    dir_inode.write(fs).await?;

    Ok(())
}

/// Remove an item from a directory
///
/// This edits directory entry bytes in-place.
#[maybe_async::maybe_async]
pub(crate) async fn remove_dir_entry(
    fs: &Ext4,
    dir_inode: &mut Inode,
    name: DirEntryName<'_>,
) -> Result<(), Ext4Error> {
    assert!(dir_inode.file_type().is_dir());

    if dir_inode.flags().contains(InodeFlags::DIRECTORY_ENCRYPTED) {
        return Err(Ext4Error::Encrypted);
    }
    if dir_inode.flags().contains(InodeFlags::DIRECTORY_HTREE) {
        return remove_dir_entry_htree(fs, dir_inode, name).await;
    }

    let block_size = fs.0.superblock.block_size();
    let block_size_usize = block_size.to_usize();
    let mut file_blocks = FileBlocks::new(fs.clone(), dir_inode)?;
    let mut block_buf = vec![0u8; block_size_usize];

    let mut is_first = true;
    let mut logical_block_index = 0u64;

    while let Some(block_index_res) = file_blocks.next().await {
        let block_index = block_index_res?;
        fs.read_from_block(block_index, 0, &mut block_buf).await?;

        let mut off = 0usize;
        let mut prev_off: Option<usize> = None;

        while off < block_size_usize {
            let inode_field = read_u32le(&block_buf, off);
            let rec_len_offset = checked_add_usize(off, 4, dir_inode.index)?;
            let rec_len = read_u16le(&block_buf, rec_len_offset);
            let rec_len_usize = usize::from(rec_len);
            let rec_end =
                checked_add_usize(off, rec_len_usize, dir_inode.index)?;

            if rec_len_usize < 8 || rec_end > block_size_usize {
                return Err(dir_entry_error(dir_inode.index));
            }

            if inode_field != 0 {
                let name_len_offset =
                    checked_add_usize(off, 6, dir_inode.index)?;
                let name_len = usize::from(block_buf[name_len_offset]);
                let name_start = checked_add_usize(off, 8, dir_inode.index)?;
                let name_end =
                    checked_add_usize(name_start, name_len, dir_inode.index)?;
                if name_end > rec_end {
                    return Err(dir_entry_error(dir_inode.index));
                }

                if block_buf[name_start..name_end] == *name.as_ref() {
                    // Don't allow removing "." or "..".
                    if name.as_ref() == b"." || name.as_ref() == b".." {
                        return Err(Ext4Error::Readonly);
                    }

                    if let Some(poff) = prev_off {
                        // Merge into previous record by extending its rec_len.
                        let prev_rec_len_offset =
                            checked_add_usize(poff, 4, dir_inode.index)?;
                        let prev_rec_len =
                            read_u16le(&block_buf, prev_rec_len_offset);
                        let new_len = checked_add_usize(
                            usize::from(prev_rec_len),
                            rec_len_usize,
                            dir_inode.index,
                        )?;
                        write_u16le(
                            &mut block_buf,
                            prev_rec_len_offset,
                            u16::try_from(new_len).map_err(|_| {
                                dir_entry_error(dir_inode.index)
                            })?,
                        );
                        // Zero inode to mark removed (not strictly necessary once merged).
                        write_u32le(&mut block_buf, off, 0);
                    } else {
                        // No previous entry in this block; just mark this record unused.
                        write_u32le(&mut block_buf, off, 0);
                    }

                    // Check if this block is entirely empty.
                    let mut all_empty = true;
                    let mut verify_off = 0usize;
                    while verify_off < block_size_usize {
                        let inode_field = read_u32le(&block_buf, verify_off);
                        let verify_rec_len_offset =
                            checked_add_usize(verify_off, 4, dir_inode.index)?;
                        let rec_len =
                            read_u16le(&block_buf, verify_rec_len_offset);
                        let rec_len_usize = usize::from(rec_len);
                        if rec_len_usize == 0 {
                            break;
                        }
                        if inode_field != 0 {
                            all_empty = false;
                            break;
                        }
                        verify_off = checked_add_usize(
                            verify_off,
                            rec_len_usize,
                            dir_inode.index,
                        )?;
                    }

                    let file_blocks_count =
                        dir_inode.size_in_bytes().div_ceil(block_size.to_u64());
                    let last_file_block_index = file_blocks_count
                        .checked_sub(1)
                        .ok_or_else(|| dir_entry_error(dir_inode.index))?;

                    if all_empty
                        && logical_block_index == last_file_block_index
                        && logical_block_index > 0
                    {
                        // Truncate the file to remove the last empty block.
                        truncate(
                            fs,
                            dir_inode,
                            checked_mul_u64(
                                logical_block_index,
                                block_size.to_u64(),
                                dir_inode.index,
                            )?,
                        )
                        .await?;
                        return Ok(());
                    }

                    // If metadata checksums are enabled, update the directory block checksum tail.
                    DirBlock {
                        fs,
                        block_index,
                        is_first,
                        dir_inode: dir_inode.index,
                        has_htree: false,
                        checksum_base: dir_inode.checksum_base().clone(),
                    }
                    .update_checksum(&mut block_buf)?;

                    fs.write_to_block(block_index, 0, &block_buf).await?;
                    return Ok(());
                }
            }

            prev_off = Some(off);
            off = rec_end;
        }

        is_first = false;
        logical_block_index =
            checked_add_u64(logical_block_index, 1, dir_inode.index)?;
    }

    Err(Ext4Error::NotFound)
}

/// Initialize a newly created directory inode by writing its initial entries.
///
/// This creates the required `.` and `..` entries in the first directory block.
///
/// Notes/limitations:
/// - Only supports non-htree, non-encrypted directories.
/// - Uses [`write_at`] so blocks will be allocated as needed, and the inode size
///   will be updated and persisted.
/// - This does not modify the parent directory; callers typically still need to
///   link the new directory into the parent.
#[maybe_async::maybe_async]
pub(crate) async fn init_directory(
    fs: &Ext4,
    dir_inode: &mut Inode,
    parent_inode_index: InodeIndex,
) -> Result<(), Ext4Error> {
    if !dir_inode.file_type().is_dir() {
        return Err(Ext4Error::NotADirectory);
    }

    if dir_inode.flags().contains(InodeFlags::DIRECTORY_ENCRYPTED) {
        return Err(Ext4Error::Encrypted);
    }

    // We only support the plain (non-htree) format for initialization.
    if dir_inode.flags().contains(InodeFlags::DIRECTORY_HTREE) {
        return Err(Ext4Error::Readonly);
    }

    // Be conservative: don't try to re-init an existing directory.
    if dir_inode.size_in_bytes() != 0 {
        return Err(Ext4Error::AlreadyExists);
    }

    let block_size = fs.0.superblock.block_size().to_usize();
    let mut block_buf = vec![0u8; block_size];

    // When metadata checksums are enabled, leaf blocks include a 12-byte tail.
    // Our `DirBlock` helper will compute/update the checksum using everything
    // except the tail, so ensure entries don't overlap it.
    let tail_size = if fs.has_metadata_checksums() {
        12usize
    } else {
        0usize
    };
    let usable = checked_sub_usize(block_size, tail_size, dir_inode.index)?;

    let dot = DirEntryName::try_from(".")
        .map_err(|_| dir_entry_error(dir_inode.index))?;
    let dotdot = DirEntryName::try_from("..")
        .map_err(|_| dir_entry_error(dir_inode.index))?;

    let dot_len = dir_entry_min_size(dot.as_ref().len(), dir_inode.index)?;
    if dot_len >= usable {
        return Err(dir_entry_error(dir_inode.index));
    }

    // '.' entry.
    write_dir_entry_bytes(
        &mut block_buf,
        0,
        dot_len,
        dir_inode.index,
        dot,
        FileType::Directory,
    )?;

    // '..' entry consumes the remainder of the usable area.
    let dotdot_off = dot_len;
    let dotdot_rec_len =
        checked_sub_usize(usable, dotdot_off, dir_inode.index)?;

    write_dir_entry_bytes(
        &mut block_buf,
        dotdot_off,
        dotdot_rec_len,
        parent_inode_index,
        dotdot,
        FileType::Directory,
    )?;

    // Write checksum dir entry if needed.
    if fs.has_metadata_checksums() {
        let checksum_start =
            checked_sub_usize(block_size, 12, dir_inode.index)?;
        let checksum_tail_offset =
            checked_add_usize(checksum_start, 4, dir_inode.index)?;
        write_u32le(&mut block_buf, checksum_start, 0);
        let tail_val = 12u32 | (0xDE << 24);
        write_u32le(&mut block_buf, checksum_tail_offset, tail_val);
        // TODO: Cleanup
        // Update the checksum tail (stored in the last 4 bytes) if enabled.
        DirBlock {
            fs,
            // Not used by update_checksum; set a dummy value.
            block_index: 0,
            is_first: true,
            dir_inode: dir_inode.index,
            has_htree: false,
            checksum_base: dir_inode.checksum_base().clone(),
        }
        .update_checksum(&mut block_buf)?;
    }

    // Persist: write_at will allocate blocks and update inode size/extent tree.
    let n = write_at(fs, dir_inode, &block_buf, 0).await?;
    if n != block_buf.len() {
        return Err(Ext4Error::NoSpace);
    }

    dir_inode.set_links_count(1);
    dir_inode.write(fs).await?;

    Ok(())
}

fn dir_entry_min_size(
    name_len: usize,
    inode: InodeIndex,
) -> Result<usize, Ext4Error> {
    // ext4 dir entry header is 8 bytes; record sizes are 4-byte aligned.
    let base = checked_add_usize(8, name_len, inode)?;
    Ok(checked_add_usize(base, 3, inode)? & !3)
}

fn write_dir_entry_bytes(
    block: &mut [u8],
    off: usize,
    rec_len: usize,
    inode: InodeIndex,
    name: DirEntryName<'_>,
    file_type: FileType,
) -> Result<(), Ext4Error> {
    let need = dir_entry_min_size(name.as_ref().len(), inode)?;
    if rec_len < need {
        return Err(Ext4Error::Readonly);
    }

    let rec_end = checked_add_usize(off, rec_len, inode)?;
    if rec_end > block.len() {
        return Err(dir_entry_error(inode));
    }

    let rec_len_offset = checked_add_usize(off, 4, inode)?;
    let name_len_offset = checked_add_usize(off, 6, inode)?;
    let file_type_offset = checked_add_usize(off, 7, inode)?;
    let name_start = checked_add_usize(off, 8, inode)?;
    let name_end = checked_add_usize(name_start, name.as_ref().len(), inode)?;

    write_u32le(block, off, inode.get());
    write_u16le(
        block,
        rec_len_offset,
        u16::try_from(rec_len).map_err(|_| dir_entry_error(inode))?,
    );
    block[name_len_offset] = u8::try_from(name.as_ref().len())
        .map_err(|_| dir_entry_error(inode))?;
    block[file_type_offset] = file_type.to_dir_entry();
    block[name_start..name_end].copy_from_slice(name.as_ref());

    // Zero padding up to `rec_len`.
    for b in &mut block[name_end..rec_end] {
        *b = 0;
    }

    Ok(())
}

/// A directory, represented by its inode.
/// This provides methods for reading and modifying the directory's entries.
pub struct Dir {
    fs: Ext4,
    inode: Inode,
}

impl Dir {
    /// Create and initialize a new directory.
    #[maybe_async::maybe_async]
    pub async fn init(
        fs: Ext4,
        mut dir_inode: Inode,
        parent_inode_index: InodeIndex,
    ) -> Result<Self, Ext4Error> {
        init_directory(&fs, &mut dir_inode, parent_inode_index).await?;
        Ok(Self {
            fs,
            inode: dir_inode,
        })
    }

    /// Open a directory by inode.
    pub fn open_inode(fs: &Ext4, inode: Inode) -> Result<Self, Ext4Error> {
        if !inode.file_type().is_dir() {
            return Err(Ext4Error::NotADirectory);
        }
        Ok(Self {
            fs: fs.clone(),
            inode,
        })
    }

    /// Return an iterator over the entries in this directory.
    pub fn read_dir(&self) -> Result<ReadDir, Ext4Error> {
        ReadDir::new(self.fs.clone(), &self.inode, PathBuf::empty())
    }

    /// Return the inode for the entry with the given name in this directory.
    #[maybe_async::maybe_async]
    pub async fn get_entry(
        &self,
        name: DirEntryName<'_>,
    ) -> Result<Inode, Ext4Error> {
        get_dir_entry_inode_by_name(&self.fs, &self.inode, name).await
    }

    /// Create a new directory entry at `name` pointing to `target_inode`.
    /// Increments relevant link counts (`target_inode` always, and `self` if `target_inode` is a directory).
    ///
    /// This is similar to `link(2)`.
    ///
    /// # Errors
    ///
    /// If `links_count` of the target is `u16::MAX - 1`, an error will be returned.
    /// Likewise, an error will be returned if `links_count` of the parent is `u16::MAX - 1`,
    /// and the target is a directory.
    ///
    /// [`Ext4Error::AlreadyExists`] will be returned if an entry with the same name is already present.
    /// Encrypted directories cannot be read or modified.
    #[maybe_async::maybe_async]
    pub async fn link(
        &mut self,
        name: DirEntryName<'_>,
        target_inode: &mut Inode,
    ) -> Result<(), Ext4Error> {
        let old = target_inode.links_count();
        let new = old.checked_add(1).ok_or(Ext4Error::Readonly)?;
        target_inode.set_links_count(new);
        target_inode.write(&self.fs).await?;

        if target_inode.file_type() == FileType::Directory {
            let parent_old = self.inode.links_count();
            let parent_new =
                parent_old.checked_add(1).ok_or(Ext4Error::Readonly)?;
            self.inode.set_links_count(parent_new);
            self.inode.write(&self.fs).await?;
        }

        add_dir_entry(
            &self.fs,
            &mut self.inode,
            name,
            target_inode.index,
            target_inode.file_type(),
        )
        .await?;
        Ok(())
    }

    /// Remove a directory entry at `path`.
    ///
    /// This is similar to `unlink(2)` for non-directories.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * The entry does not exist [`Ext4Error::NotFound`]
    /// * The entry is "." or ".." [`Ext4Error::DotEntry`]
    /// * The file blocks of the inode are corrupted in some way
    #[maybe_async::maybe_async]
    pub async fn unlink(
        &mut self,
        name: DirEntryName<'_>,
        mut inode: Inode,
    ) -> Result<Option<Inode>, Ext4Error> {
        if name.0 == b"." || name.0 == b".." {
            return Err(Ext4Error::DotEntry);
        }
        let old = inode.links_count();
        inode.set_links_count(old.saturating_sub(1));
        inode.write(&self.fs).await?;
        remove_dir_entry(&self.fs, &mut self.inode, name).await?;
        if inode.links_count() == 0 {
            self.fs.delete_file(inode).await?;
            Ok(None)
        } else {
            Ok(Some(inode))
        }
    }

    /// Return the inode for this directory.
    #[must_use]
    pub fn inode(&self) -> &Inode {
        &self.inode
    }

    /// Return a mutable reference to the inode for this directory.
    #[must_use]
    pub fn inode_mut(&mut self) -> &mut Inode {
        &mut self.inode
    }
}

#[derive(Clone)]
struct HtreeLeafEntryData {
    hash: u32,
    inode: InodeIndex,
    name: Vec<u8>,
    file_type: FileType,
}

fn read_htree_leaf_entries(
    fs: &Ext4,
    dir_inode: &Inode,
    hash_alg: HashAlg,
    block: &[u8],
) -> Result<Vec<HtreeLeafEntryData>, Ext4Error> {
    let mut entries = Vec::new();
    let mut off = 0usize;
    let path = PtrPrimitive::new(PathBuf::empty());
    let seed = fs.0.superblock.htree_hash_seed();

    while off < block.len() {
        let (entry, entry_size) = DirEntry::from_bytes(
            fs.clone(),
            &block[off..],
            dir_inode.index,
            path.clone(),
        )?;
        off = checked_add_usize(off, entry_size.get(), dir_inode.index)?;

        let Some(entry) = entry else {
            continue;
        };

        let name = entry.file_name();
        entries.push(HtreeLeafEntryData {
            hash: hash_alg.hash(name, &seed),
            inode: entry.inode,
            name: name.as_ref().to_vec(),
            file_type: entry.file_type()?,
        });
    }

    Ok(entries)
}

fn htree_leaf_usable_bytes(
    fs: &Ext4,
    inode: InodeIndex,
) -> Result<usize, Ext4Error> {
    let block_size = fs.0.superblock.block_size().to_usize();
    let tail_size = if fs.has_metadata_checksums() {
        12usize
    } else {
        0usize
    };
    checked_sub_usize(block_size, tail_size, inode)
}

fn htree_leaf_entries_size(
    entries: &[HtreeLeafEntryData],
    inode: InodeIndex,
) -> Result<usize, Ext4Error> {
    let mut total = 0usize;
    for entry in entries {
        total = checked_add_usize(
            total,
            dir_entry_min_size(entry.name.len(), inode)?,
            inode,
        )?;
    }
    Ok(total)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HtreeLeafSplit {
    split_index: usize,
    separator_hash: u32,
}

fn choose_htree_split(
    entries: &[HtreeLeafEntryData],
    inode: InodeIndex,
    usable: usize,
) -> Result<Option<HtreeLeafSplit>, Ext4Error> {
    let mut total = 0usize;
    let mut entry_sizes = Vec::with_capacity(entries.len());
    for entry in entries {
        let size = dir_entry_min_size(entry.name.len(), inode)?;
        total = checked_add_usize(total, size, inode)?;
        entry_sizes.push(size);
    }

    if total <= usable {
        return Ok(None);
    }

    let half = usable / 2;
    let mut left_size = 0usize;
    let mut split_index = None;

    for (index, size) in entry_sizes.into_iter().enumerate() {
        let next_left_size = checked_add_usize(left_size, size, inode)?;
        if next_left_size > half {
            split_index = Some(index);
            break;
        }
        left_size = next_left_size;
    }

    let split_index = split_index.ok_or(Ext4Error::NoSpace)?;
    if split_index == 0 || split_index >= entries.len() {
        return Err(Ext4Error::NoSpace);
    }

    let mut separator_hash = entries[split_index].hash;
    if entries[split_index.checked_sub(1).unwrap()].hash == separator_hash {
        separator_hash =
            separator_hash.checked_add(1).ok_or(Ext4Error::NoSpace)?;
    }

    Ok(Some(HtreeLeafSplit {
        split_index,
        separator_hash,
    }))
}

fn pack_htree_leaf_block(
    fs: &Ext4,
    dir_inode: &Inode,
    entries: &[HtreeLeafEntryData],
) -> Result<Vec<u8>, Ext4Error> {
    if entries.is_empty() {
        return Err(dir_entry_error(dir_inode.index));
    }

    let block_size = fs.0.superblock.block_size().to_usize();
    let usable = htree_leaf_usable_bytes(fs, dir_inode.index)?;
    let mut block = vec![0u8; block_size];
    let mut off = 0usize;

    for (idx, entry) in entries.iter().enumerate() {
        let rec_len = if idx.checked_add(1).unwrap() == entries.len() {
            checked_sub_usize(usable, off, dir_inode.index)?
        } else {
            dir_entry_min_size(entry.name.len(), dir_inode.index)?
        };

        write_dir_entry_bytes(
            &mut block,
            off,
            rec_len,
            entry.inode,
            DirEntryName::try_from(entry.name.as_slice())
                .map_err(|_| dir_entry_error(dir_inode.index))?,
            entry.file_type,
        )?;

        off = checked_add_usize(off, rec_len, dir_inode.index)?;
    }

    if fs.has_metadata_checksums() {
        let checksum_start =
            checked_sub_usize(block_size, 12, dir_inode.index)?;
        let checksum_tail_offset =
            checked_add_usize(checksum_start, 4, dir_inode.index)?;
        write_u32le(&mut block, checksum_start, 0);
        let tail_val = 12u32 | (0xDE << 24);
        write_u32le(&mut block, checksum_tail_offset, tail_val);
        DirBlock {
            fs,
            block_index: 0,
            is_first: false,
            dir_inode: dir_inode.index,
            has_htree: true,
            checksum_base: dir_inode.checksum_base().clone(),
        }
        .update_checksum(&mut block)?;
    }

    Ok(block)
}

/// Add an item to a directory with an htree.
#[maybe_async::maybe_async]
pub(crate) async fn add_dir_entry_htree(
    fs: &Ext4,
    dir_inode: &mut Inode,
    name: DirEntryName<'_>,
    inode: InodeIndex,
    file_type: FileType,
) -> Result<(), Ext4Error> {
    assert!(dir_inode.file_type().is_dir());

    if dir_inode.flags().contains(InodeFlags::DIRECTORY_ENCRYPTED) {
        return Err(Ext4Error::Encrypted);
    }

    // Fail if name already exists.
    if get_dir_entry_inode_by_name(fs, dir_inode, name)
        .await
        .is_ok()
    {
        return Err(Ext4Error::AlreadyExists);
    }

    let block_size = fs.0.superblock.block_size().to_usize();
    let block_size_u64 = fs.0.superblock.block_size().to_nz_u64();
    let mut block_buf = vec![0u8; block_size];

    crate::dir_htree::read_root_block(fs, dir_inode, &mut block_buf).await?;
    let hash_alg = HashAlg::from_u8(block_buf[0x1c])?;
    let mut leaf_lookup =
        crate::dir_htree::find_leaf_lookup(fs, dir_inode, name, &mut block_buf)
            .await?;

    let need = dir_entry_min_size(name.as_ref().len(), dir_inode.index)?;

    let mut off = 0usize;

    while off < block_size {
        let inode_field = read_u32le(&block_buf, off);
        let rec_len_offset = checked_add_usize(off, 4, dir_inode.index)?;
        let rec_len = read_u16le(&block_buf, rec_len_offset);
        let rec_len_usize = usize::from(rec_len);
        let rec_end = checked_add_usize(off, rec_len_usize, dir_inode.index)?;

        if rec_len_usize < 8 || rec_end > block_size {
            return Err(dir_entry_error(dir_inode.index));
        }

        let used = if inode_field == 0 {
            0usize
        } else {
            let name_len_offset = checked_add_usize(off, 6, dir_inode.index)?;
            let name_len = usize::from(block_buf[name_len_offset]);
            dir_entry_min_size(name_len, dir_inode.index)?
        };

        let required = checked_add_usize(used, need, dir_inode.index)?;
        if rec_len_usize >= required {
            let new_rec_len_for_curr =
                if inode_field == 0 { 0usize } else { used };
            let free_start =
                checked_add_usize(off, new_rec_len_for_curr, dir_inode.index)?;
            let free_len = checked_sub_usize(
                rec_len_usize,
                new_rec_len_for_curr,
                dir_inode.index,
            )?;

            if free_len < need {
                off = rec_end;
                continue;
            }

            let rec_len_to_write = if inode_field != 0 {
                new_rec_len_for_curr
            } else {
                rec_len_usize
            };
            write_u16le(
                &mut block_buf,
                rec_len_offset,
                u16::try_from(rec_len_to_write)
                    .map_err(|_| dir_entry_error(dir_inode.index))?,
            );

            write_dir_entry_bytes(
                &mut block_buf,
                free_start,
                free_len,
                inode,
                name,
                file_type,
            )?;

            DirBlock {
                fs,
                block_index: leaf_lookup.leaf_absolute_block,
                is_first: false,
                dir_inode: dir_inode.index,
                has_htree: true,
                checksum_base: dir_inode.checksum_base().clone(),
            }
            .update_checksum(&mut block_buf)?;

            fs.write_to_block(leaf_lookup.leaf_absolute_block, 0, &block_buf)
                .await?;
            return Ok(());
        }

        off = rec_end;
    }

    let mut entries =
        read_htree_leaf_entries(fs, dir_inode, hash_alg, &block_buf)?;
    entries.push(HtreeLeafEntryData {
        hash: leaf_lookup.hash,
        inode,
        name: name.as_ref().to_vec(),
        file_type,
    });
    entries.sort_by(|lhs, rhs| {
        lhs.hash
            .cmp(&rhs.hash)
            .then_with(|| lhs.name.as_slice().cmp(rhs.name.as_slice()))
    });

    let usable = htree_leaf_usable_bytes(fs, dir_inode.index)?;
    if htree_leaf_entries_size(&entries, dir_inode.index)? <= usable {
        let rebuilt = pack_htree_leaf_block(fs, dir_inode, &entries)?;
        fs.write_to_block(leaf_lookup.leaf_absolute_block, 0, &rebuilt)
            .await?;
        return Ok(());
    }

    let split = choose_htree_split(&entries, dir_inode.index, usable)?
        .ok_or(Ext4Error::NoSpace)?;
    split_index_path_for_new_child(fs, dir_inode, &mut leaf_lookup).await?;

    let left_block =
        pack_htree_leaf_block(fs, dir_inode, &entries[..split.split_index])?;
    let right_block =
        pack_htree_leaf_block(fs, dir_inode, &entries[split.split_index..])?;

    let append_offset = dir_inode.size_in_bytes();
    if append_offset % block_size_u64 != 0 {
        return Err(dir_entry_error(dir_inode.index));
    }
    let new_leaf_relative_block = u32::try_from(append_offset / block_size_u64)
        .map_err(|_| Ext4Error::NoSpace)?;

    let n = write_at(fs, dir_inode, &right_block, append_offset).await?;
    if n != right_block.len() {
        return Err(Ext4Error::NoSpace);
    }
    dir_inode.write(fs).await?;

    insert_child_into_parent(
        fs,
        dir_inode,
        leaf_lookup.parent(),
        split.separator_hash,
        new_leaf_relative_block,
    )
    .await?;

    fs.write_to_block(leaf_lookup.leaf_absolute_block, 0, &left_block)
        .await?;

    Ok(())
}

/// Remove an item from a directory with an htree.
#[maybe_async::maybe_async]
pub(crate) async fn remove_dir_entry_htree(
    fs: &Ext4,
    dir_inode: &mut Inode,
    name: DirEntryName<'_>,
) -> Result<(), Ext4Error> {
    assert!(dir_inode.file_type().is_dir());

    if dir_inode.flags().contains(InodeFlags::DIRECTORY_ENCRYPTED) {
        return Err(Ext4Error::Encrypted);
    }

    let block_size = fs.0.superblock.block_size().to_usize();
    let mut block_buf = vec![0u8; block_size];

    crate::dir_htree::read_root_block(fs, dir_inode, &mut block_buf).await?;

    if name.as_ref() == b"." || name.as_ref() == b".." {
        return Err(Ext4Error::Readonly);
    }

    let mut leaf_lookup =
        crate::dir_htree::find_leaf_lookup(fs, dir_inode, name, &mut block_buf)
            .await?;

    loop {
        let leaf_absolute_block = leaf_lookup.leaf_absolute_block;
        let mut off = 0usize;
        let mut prev_off: Option<usize> = None;

        while off < block_size {
            let inode_field = read_u32le(&block_buf, off);
            let rec_len_offset = checked_add_usize(off, 4, dir_inode.index)?;
            let rec_len = read_u16le(&block_buf, rec_len_offset);
            let rec_len_usize = usize::from(rec_len);
            let rec_end =
                checked_add_usize(off, rec_len_usize, dir_inode.index)?;

            if rec_len_usize < 8 || rec_end > block_size {
                return Err(dir_entry_error(dir_inode.index));
            }

            if inode_field != 0 {
                let name_len_offset =
                    checked_add_usize(off, 6, dir_inode.index)?;
                let name_len = usize::from(block_buf[name_len_offset]);
                let name_start = checked_add_usize(off, 8, dir_inode.index)?;
                let name_end =
                    checked_add_usize(name_start, name_len, dir_inode.index)?;
                if name_end > rec_end {
                    return Err(dir_entry_error(dir_inode.index));
                }

                if block_buf[name_start..name_end] == *name.as_ref() {
                    if let Some(poff) = prev_off {
                        let prev_rec_len_offset =
                            checked_add_usize(poff, 4, dir_inode.index)?;
                        let prev_rec_len =
                            read_u16le(&block_buf, prev_rec_len_offset);
                        let new_len = checked_add_usize(
                            usize::from(prev_rec_len),
                            rec_len_usize,
                            dir_inode.index,
                        )?;
                        write_u16le(
                            &mut block_buf,
                            prev_rec_len_offset,
                            u16::try_from(new_len).map_err(|_| {
                                dir_entry_error(dir_inode.index)
                            })?,
                        );
                        write_u32le(&mut block_buf, off, 0);
                    } else {
                        write_u32le(&mut block_buf, off, 0);
                    }

                    DirBlock {
                        fs,
                        block_index: leaf_absolute_block,
                        is_first: false,
                        dir_inode: dir_inode.index,
                        has_htree: true,
                        checksum_base: dir_inode.checksum_base().clone(),
                    }
                    .update_checksum(&mut block_buf)?;

                    fs.write_to_block(leaf_absolute_block, 0, &block_buf)
                        .await?;
                    return Ok(());
                }
            }

            prev_off = Some(off);
            off = rec_end;
        }

        if !advance_leaf_lookup_to_next(
            fs,
            dir_inode,
            &mut leaf_lookup,
            &mut block_buf,
        )
        .await?
        {
            return Err(Ext4Error::NotFound);
        }
    }
}

#[cfg(feature = "std")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{load_test_disk1, load_test_disk1_rw};
    use crate::{FollowSymlinks, InodeCreationOptions, InodeMode};

    #[test]
    fn test_choose_htree_split_with_hash_collision() {
        let inode = InodeIndex::new(1).unwrap();
        let entry = |hash, byte| HtreeLeafEntryData {
            hash,
            inode,
            name: vec![byte; 200],
            file_type: FileType::Regular,
        };

        let entries = vec![
            entry(0x1234_5678, b'a'),
            entry(0x1234_5678, b'b'),
            entry(0x2234_5678, b'c'),
            entry(0x3234_5678, b'd'),
        ];
        let usable = dir_entry_min_size(200, inode).unwrap() * 3;

        let split = choose_htree_split(&entries, inode, usable)
            .unwrap()
            .unwrap();
        assert_eq!(split.split_index, 1);
        assert_eq!(split.separator_hash, 0x1234_5679);
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_get_dir_entry_inode_by_name() {
        let fs = load_test_disk1().await;
        let root_inode = fs.read_root_inode().await.unwrap();

        let lookup = |name| {
            get_dir_entry_inode_by_name(
                &fs,
                &root_inode,
                DirEntryName::try_from(name).unwrap(),
            )
        };

        // Check for a few expected entries.
        // '.' always links to self.
        let index = lookup(".").await.unwrap().index;
        assert_eq!(index, root_inode.index);
        // '..' is normally parent, but in the root dir it's just the
        // root dir again.
        let index = lookup("..").await.unwrap().index;
        assert_eq!(index, root_inode.index);
        // Don't check specific values of these since they might change
        // if the test disk is regenerated
        let res = lookup("empty_file").await;
        assert!(res.is_ok());
        let res = lookup("empty_dir").await;
        assert!(res.is_ok());

        // Check for something that does not exist.
        let err = lookup("does_not_exist").await.unwrap_err();
        assert!(matches!(err, Ext4Error::NotFound));
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_htree_root_split() {
        let fs = load_test_disk1_rw().await;
        let medium_dir = fs
            .path_to_inode(
                "/medium_dir".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        let mut dir = Dir::open_inode(&fs.0, medium_dir).unwrap();

        let block_size = fs.superblock().block_size().to_usize();
        let mut root_block = vec![0; block_size];
        crate::dir_htree::read_root_block(&fs, dir.inode(), &mut root_block)
            .await
            .unwrap();
        assert_eq!(root_block[0x1e], 0);

        let filler = "x".repeat(240);
        let mut added = Vec::new();
        let mut depth = 0u8;

        for i in 0..600 {
            let name = format!("rootsplit_{i:04}_{filler}");
            let mut new_inode = fs
                .create_inode(InodeCreationOptions {
                    file_type: FileType::Regular,
                    mode: InodeMode::S_IRUSR
                        | InodeMode::S_IWUSR
                        | InodeMode::S_IFREG,
                    uid: 0,
                    gid: 0,
                    time: Default::default(),
                    flags: InodeFlags::empty(),
                })
                .await
                .unwrap();
            dir.link(
                DirEntryName::try_from(name.as_bytes()).unwrap(),
                &mut new_inode,
            )
            .await
            .unwrap();
            added.push(name);

            crate::dir_htree::read_root_block(
                &fs,
                dir.inode(),
                &mut root_block,
            )
            .await
            .unwrap();
            depth = root_block[0x1e];
            if depth == 1 {
                break;
            }
        }

        assert_eq!(depth, 1, "root htree node never split");

        for name in &added {
            let inode = dir
                .get_entry(DirEntryName::try_from(name.as_bytes()).unwrap())
                .await
                .unwrap();
            assert_eq!(inode.file_type(), FileType::Regular);
        }

        let old_entry = dir
            .get_entry(DirEntryName::try_from("0").unwrap())
            .await
            .unwrap();
        assert_eq!(old_entry.file_type(), FileType::Regular);
    }
}
