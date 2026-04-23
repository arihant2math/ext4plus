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
use crate::dir_entry::DirEntryName;
use crate::dir_htree::get_dir_entry_via_htree;
use crate::error::{CorruptKind, Ext4Error};
use crate::file::{truncate, write_at};
use crate::file_type::FileType;
use crate::inode::{Inode, InodeFlags, InodeIndex};
#[cfg(not(feature = "sync"))]
use crate::iters::AsyncIterator;
use crate::iters::file_blocks::FileBlocks;
use crate::iters::read_dir::ReadDir;
use crate::path::PathBuf;
use crate::util::write_u32le;
use crate::util::{read_u16le, read_u32le, write_u16le};
use alloc::vec;

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

    let need = dir_entry_min_size(name.as_ref().len());
    let mut block_buf = vec![0u8; block_size];
    let mut is_first = true;

    while let Some(block_index_res) = file_blocks.next().await {
        let block_index = block_index_res?;
        fs.read_from_block(block_index, 0, &mut block_buf).await?;

        // Walk entries in this block looking for usable slack space.
        let mut off = 0usize;
        while off < block_size {
            let inode_field = read_u32le(&block_buf, off);
            let rec_len = read_u16le(&block_buf, off.checked_add(4).unwrap());
            let rec_len_usize = usize::from(rec_len);

            if rec_len_usize < 8 || off.checked_add(rec_len_usize).is_none() {
                return Err(CorruptKind::DirEntry(dir_inode.index).into());
            }
            if off.checked_add(rec_len_usize).unwrap() > block_size {
                return Err(CorruptKind::DirEntry(dir_inode.index).into());
            }

            // `inode == 0` indicates "special" entry or unused; treat it as fully free.
            let used = if inode_field == 0 {
                0usize
            } else {
                let name_len =
                    usize::from(block_buf[off.checked_add(6).unwrap()]);
                dir_entry_min_size(name_len)
            };

            if rec_len_usize >= used.checked_add(need).unwrap() {
                // Shrink current entry to its minimal size (or keep 0 if unused),
                // and place the new entry in the leftover space.
                let new_rec_len_for_curr =
                    if inode_field == 0 { 0usize } else { used };
                let free_start = off.checked_add(new_rec_len_for_curr).unwrap();
                let free_len =
                    rec_len_usize.checked_sub(new_rec_len_for_curr).unwrap();

                if free_len < need {
                    // Shouldn't happen due to earlier check, but keep safe.
                    off = off.checked_add(rec_len_usize).unwrap();
                    continue;
                }

                let rec_len = if inode_field != 0 {
                    new_rec_len_for_curr
                } else {
                    rec_len_usize
                };
                write_u16le(
                    &mut block_buf,
                    off.checked_add(4).unwrap(),
                    u16::try_from(rec_len).map_err(|_| {
                        Ext4Error::from(CorruptKind::DirEntry(dir_inode.index))
                    })?,
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

            off = off.checked_add(rec_len_usize).unwrap();
        }

        is_first = false;
    }

    let mut new_block_buf = vec![0u8; block_size];

    let tail_size = if fs.has_metadata_checksums() {
        12usize
    } else {
        0usize
    };
    let usable = block_size.checked_sub(tail_size).ok_or_else(|| {
        Ext4Error::from(CorruptKind::DirEntry(dir_inode.index))
    })?;

    if need > usable {
        return Err(CorruptKind::DirEntry(dir_inode.index).into());
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
        let checksum_start = block_size.checked_sub(12).unwrap();
        write_u32le(&mut new_block_buf, checksum_start, 0);
        let tail_val = 12u32 | (0xDE << 24);
        write_u32le(
            &mut new_block_buf,
            checksum_start.checked_add(4).unwrap(),
            tail_val,
        );
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
    let mut file_blocks = FileBlocks::new(fs.clone(), dir_inode)?;
    let mut block_buf = vec![0u8; block_size.to_usize()];

    let mut is_first = true;
    let mut logical_block_index = 0u64;

    while let Some(block_index_res) = file_blocks.next().await {
        let block_index = block_index_res?;
        fs.read_from_block(block_index, 0, &mut block_buf).await?;

        let mut off = 0usize;
        let mut prev_off: Option<usize> = None;

        while off < block_size.to_usize() {
            let inode_field = read_u32le(&block_buf, off);
            let rec_len = read_u16le(&block_buf, off.checked_add(4).unwrap());
            let rec_len_usize = usize::from(rec_len);

            if rec_len_usize < 8
                || off.checked_add(rec_len_usize).unwrap() > block_size
            {
                return Err(CorruptKind::DirEntry(dir_inode.index).into());
            }

            if inode_field != 0 {
                let name_len =
                    usize::from(block_buf[off.checked_add(6).unwrap()]);
                let name_start = off.checked_add(8).unwrap();
                let name_end = name_start.checked_add(name_len).unwrap();
                if name_end > off.checked_add(rec_len_usize).unwrap() {
                    return Err(CorruptKind::DirEntry(dir_inode.index).into());
                }

                if block_buf[name_start..name_end] == *name.as_ref() {
                    // Don't allow removing "." or "..".
                    if name.as_ref() == b"." || name.as_ref() == b".." {
                        return Err(Ext4Error::Readonly);
                    }

                    if let Some(poff) = prev_off {
                        // Merge into previous record by extending its rec_len.
                        let prev_rec_len = read_u16le(
                            &block_buf,
                            poff.checked_add(4).unwrap(),
                        );
                        let new_len = usize::from(prev_rec_len)
                            .checked_add(rec_len_usize)
                            .unwrap();
                        write_u16le(
                            &mut block_buf,
                            poff.checked_add(4).unwrap(),
                            u16::try_from(new_len).map_err(|_| {
                                Ext4Error::from(CorruptKind::DirEntry(
                                    dir_inode.index,
                                ))
                            })?,
                        );
                        // Zero inode to mark removed (not strictly necessary once merged).
                        write_u32le(&mut block_buf, off, 0);
                    } else {
                        // No previous entry in this block; just mark this record unused.
                        write_u32le(&mut block_buf, off, 0);
                    }

                    // Check if this block is entirely empty
                    let mut all_empty = true;
                    let mut verify_off = 0usize;
                    while verify_off < block_size {
                        let inode_field = read_u32le(&block_buf, verify_off);
                        let rec_len = read_u16le(
                            &block_buf,
                            verify_off.checked_add(4).unwrap(),
                        );
                        let rec_len_usize = usize::from(rec_len);
                        if rec_len_usize == 0 {
                            break;
                        }
                        if inode_field != 0 {
                            all_empty = false;
                            break;
                        }
                        verify_off =
                            verify_off.checked_add(rec_len_usize).unwrap();
                    }

                    let file_blocks_count = (dir_inode
                        .size_in_bytes()
                        .checked_add(block_size.to_u64())
                        .unwrap()
                        .checked_sub(1)
                        .unwrap())
                        / block_size.to_nz_u64();

                    if all_empty
                        && logical_block_index
                            == file_blocks_count.checked_sub(1).unwrap()
                        && logical_block_index > 0
                    {
                        // Truncate the file to remove the last empty block.
                        truncate(
                            fs,
                            dir_inode,
                            logical_block_index
                                .checked_mul(block_size.to_u64())
                                .unwrap(),
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
            off = off.checked_add(rec_len_usize).unwrap();
        }

        is_first = false;
        logical_block_index = logical_block_index.checked_add(1).unwrap();
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
    let usable = block_size.checked_sub(tail_size).ok_or_else(|| {
        Ext4Error::from(CorruptKind::DirEntry(dir_inode.index))
    })?;

    let dot = DirEntryName::try_from(".").expect("valid dir entry name");
    let dotdot = DirEntryName::try_from("..").expect("valid dir entry name");

    let dot_len = dir_entry_min_size(dot.as_ref().len());
    if dot_len >= usable {
        return Err(CorruptKind::DirEntry(dir_inode.index).into());
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
    let dotdot_rec_len = usable.checked_sub(dotdot_off).ok_or_else(|| {
        Ext4Error::from(CorruptKind::DirEntry(dir_inode.index))
    })?;

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
        let checksum_start = block_size.checked_sub(12).unwrap();
        write_u32le(&mut block_buf, checksum_start, 0);
        let tail_val = 12u32 | (0xDE << 24);
        write_u32le(
            &mut block_buf,
            checksum_start.checked_add(4).unwrap(),
            tail_val,
        );
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

fn dir_entry_min_size(name_len: usize) -> usize {
    // ext4 dir entry header is 8 bytes; record sizes are 4-byte aligned.
    let base = 8usize
        .checked_add(name_len)
        .expect("dir entry size overflow");
    (base.checked_add(3).unwrap()) & !3
}

fn write_dir_entry_bytes(
    block: &mut [u8],
    off: usize,
    rec_len: usize,
    inode: InodeIndex,
    name: DirEntryName<'_>,
    file_type: FileType,
) -> Result<(), Ext4Error> {
    let need = dir_entry_min_size(name.as_ref().len());
    if rec_len < need {
        return Err(Ext4Error::Readonly);
    }
    if off.checked_add(rec_len).unwrap() > block.len() {
        return Err(CorruptKind::DirEntry(inode).into());
    }

    write_u32le(block, off, inode.get());
    write_u16le(
        block,
        off.checked_add(4).unwrap(),
        u16::try_from(rec_len)
            .map_err(|_| Ext4Error::from(CorruptKind::DirEntry(inode)))?,
    );
    block[off.checked_add(6).unwrap()] = u8::try_from(name.as_ref().len())
        .map_err(|_| Ext4Error::from(CorruptKind::DirEntry(inode)))?;
    block[off.checked_add(7).unwrap()] = file_type.to_dir_entry();

    let name_start = off.checked_add(8).unwrap();
    let name_end = name_start.checked_add(name.as_ref().len()).unwrap();
    block[name_start..name_end].copy_from_slice(name.as_ref());

    // Zero padding up to `rec_len`.
    for b in &mut block[name_end..off.checked_add(rec_len).unwrap()] {
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
    /// * The entry is "." or ".." [`Ext4Erro::DotEntry`]
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

#[cfg(feature = "std")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::load_test_disk1;

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
    let mut block_buf = vec![0u8; block_size];

    crate::dir_htree::read_root_block(fs, dir_inode, &mut block_buf).await?;

    let (leaf_absolute_block, _leaf_relative_block) =
        crate::dir_htree::find_leaf_node(fs, dir_inode, name, &mut block_buf)
            .await?;

    let need = dir_entry_min_size(name.as_ref().len());

    let mut off = 0usize;
    let mut found_space = false;

    while off < block_size {
        let inode_field = read_u32le(&block_buf, off);
        let rec_len = read_u16le(&block_buf, off.checked_add(4).unwrap());
        let rec_len_usize = usize::from(rec_len);

        if rec_len_usize < 8 || off.checked_add(rec_len_usize).is_none() {
            return Err(CorruptKind::DirEntry(dir_inode.index).into());
        }
        if off.checked_add(rec_len_usize).unwrap() > block_size {
            return Err(CorruptKind::DirEntry(dir_inode.index).into());
        }

        let used = if inode_field == 0 {
            0usize
        } else {
            let name_len = usize::from(block_buf[off.checked_add(6).unwrap()]);
            dir_entry_min_size(name_len)
        };

        if rec_len_usize >= used.checked_add(need).unwrap() {
            let new_rec_len_for_curr =
                if inode_field == 0 { 0usize } else { used };
            let free_start = off.checked_add(new_rec_len_for_curr).unwrap();
            let free_len =
                rec_len_usize.checked_sub(new_rec_len_for_curr).unwrap();

            if free_len < need {
                off = off.checked_add(rec_len_usize).unwrap();
                continue;
            }

            if inode_field != 0 {
                write_u16le(
                    &mut block_buf,
                    off.checked_add(4).unwrap(),
                    u16::try_from(new_rec_len_for_curr).unwrap(),
                );
            } else {
                write_u16le(
                    &mut block_buf,
                    off.checked_add(4).unwrap(),
                    u16::try_from(rec_len_usize).unwrap(),
                );
            }

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
                block_index: leaf_absolute_block,
                is_first: false,
                dir_inode: dir_inode.index,
                has_htree: true,
                checksum_base: dir_inode.checksum_base().clone(),
            }
            .update_checksum(&mut block_buf)?;

            fs.write_to_block(leaf_absolute_block, 0, &block_buf)
                .await?;
            found_space = true;
            break;
        }

        off = off.checked_add(rec_len_usize).unwrap();
    }

    if !found_space {
        return Err(Ext4Error::NoSpace);
    }

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

    let (leaf_absolute_block, _leaf_relative_block) =
        crate::dir_htree::find_leaf_node(fs, dir_inode, name, &mut block_buf)
            .await?;

    let mut off = 0usize;
    let mut prev_off: Option<usize> = None;

    while off < block_size {
        let inode_field = read_u32le(&block_buf, off);
        let rec_len = read_u16le(&block_buf, off.checked_add(4).unwrap());
        let rec_len_usize = usize::from(rec_len);

        if rec_len_usize < 8
            || off.checked_add(rec_len_usize).unwrap() > block_size
        {
            return Err(CorruptKind::DirEntry(dir_inode.index).into());
        }

        if inode_field != 0 {
            let name_len = usize::from(block_buf[off.checked_add(6).unwrap()]);
            let name_start = off.checked_add(8).unwrap();
            let name_end = name_start.checked_add(name_len).unwrap();
            if name_end > off.checked_add(rec_len_usize).unwrap() {
                return Err(CorruptKind::DirEntry(dir_inode.index).into());
            }

            if block_buf[name_start..name_end] == *name.as_ref() {
                if name.as_ref() == b"." || name.as_ref() == b".." {
                    return Err(Ext4Error::Readonly);
                }

                if let Some(poff) = prev_off {
                    let prev_rec_len =
                        read_u16le(&block_buf, poff.checked_add(4).unwrap());
                    let new_len = usize::from(prev_rec_len)
                        .checked_add(rec_len_usize)
                        .unwrap();
                    write_u16le(
                        &mut block_buf,
                        poff.checked_add(4).unwrap(),
                        u16::try_from(new_len).unwrap(),
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
        off = off.checked_add(rec_len_usize).unwrap();
    }

    Err(Ext4Error::NotFound)
}
