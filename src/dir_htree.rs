// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::Ext4;
use crate::block_index::{FileBlockIndex, FsBlockIndex};
use crate::dir_block::DirBlock;
use crate::dir_entry::{DirEntry, DirEntryName};
use crate::dir_entry_hash::HashAlg;
use crate::error::{CorruptKind, Ext4Error};
use crate::extent::Extent;
use crate::inode::{Inode, InodeFlags, InodeIndex};
#[cfg(not(feature = "sync"))]
use crate::iters::AsyncIterator;
use crate::iters::extents::Extents;
use crate::iters::file_blocks::FileBlocks;
use crate::path::PathBuf;
use crate::util::{
    read_u16le, read_u32le, usize_from_u32, write_u16le, write_u32le,
};
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;

type DirHash = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HtreeNodeKind {
    Root,
    Internal,
}

impl HtreeNodeKind {
    const fn entries_offset(self) -> usize {
        match self {
            Self::Root => 0x20,
            Self::Internal => 0x8,
        }
    }

    const fn is_first(self) -> bool {
        matches!(self, Self::Root)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HtreePathEntry {
    pub(crate) kind: HtreeNodeKind,
    pub(crate) absolute_block: FsBlockIndex,
    pub(crate) relative_block: FileBlockIndex,
    pub(crate) child_entry_index: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HtreeLeafLookup {
    pub(crate) hash: u32,
    pub(crate) leaf_absolute_block: FsBlockIndex,
    pub(crate) leaf_relative_block: FileBlockIndex,
    pub(crate) path: Vec<HtreePathEntry>,
}

impl HtreeLeafLookup {
    pub(crate) fn parent(&self) -> HtreePathEntry {
        *self.path.last().unwrap()
    }
}

// Internal node of an htree.
//
// This stores a reference to the raw bytes of entries in an internal
// node (including the root node) of an htree.
//
// Each entry is eight bytes long.
//
// The first entry is a header with three fields:
// * limit (u16): the number of entries that could be present (including
//   the header entry). In other words, space has been allocated for
//   this many entries.
// * count (u16): the actual number of entries (including the header
//   entry).
// * zero_block (u32): the child block index to use when looking up
//   hashes that compare less-than the first "normal" entry's hash key.
//
// The remaining entries each contain two fields:
// * hash: the minimum hash for this block. All directory entries in
//   this block (or children of this block) have a hash greater than or
//   equal to the hash key.
// * block (u32): the child block index.
//
// The entries after the header are sorted by hash, allowing for
// efficient hash lookup with a binary search.
//
// Note that all block indices mentioned above are relative to the file,
// not the file system. E.g. index zero is the file's first block, not
// the first block in the filesystem.
//
// Example of entries in an internal node:
// 0:  122, 15, 1      (limit, count, zero_block)
// 1:  0x0d69cdd8, 15  (hash, block)
// 2:  0x1eb8a274, 7   (hash, block)
// 3:  0x31df5aa2, 12  (hash, block)
// 4:  0x418c4380, 3   (hash, block)
// [...]
// 14: 0xec5cb0ca, 10  (hash, block)
#[derive(Debug)]
struct InternalNode<'a> {
    /// Raw entry data. The header entry is included. Entries that are
    /// not in use are excluded (in other words, this includes entries
    /// up to `count`, not `limit`).
    entries: &'a [u8],

    /// Number of entries that can fit in this node, including the header.
    limit: usize,
}

impl<'a> InternalNode<'a> {
    const ENTRY_SIZE: usize = 8;

    /// Create an `InternalNode` from a root directory block.
    fn from_root_block(
        block: &'a [u8],
        inode: InodeIndex,
    ) -> Result<Self, Ext4Error> {
        Self::new(&block[0x20..], inode)
    }

    /// Create an `InternalNode` from a non-root directory block.
    fn from_non_root_block(
        block: &'a [u8],
        inode: InodeIndex,
    ) -> Result<Self, Ext4Error> {
        Self::new(&block[0x8..], inode)
    }

    /// Create an `InternalNode` from raw bytes. These bytes come from a
    /// directory block, see [`from_root_block`] and [`from_non_root_block`].
    fn new(mut bytes: &'a [u8], inode: InodeIndex) -> Result<Self, Ext4Error> {
        // At least the header entry must be present.
        if bytes.len() < Self::ENTRY_SIZE {
            return Err(CorruptKind::HtreeInternalNodeMissingHeader {
                inode,
                num_bytes: bytes.len(),
            }
            .into());
        }

        let limit = usize::from(read_u16le(bytes, 0));

        // Get number of in-use entries from the header.
        let count = usize::from(read_u16le(bytes, 2));

        // OK to unwrap: `ENTRY_SIZE` is 8 and `count` is at most
        // 2^16-1, so the result is at most 524,280. That fits in a
        // `u32`, and we assume that `usize` is at least that large.
        let end_byte: usize = Self::ENTRY_SIZE.checked_mul(count).unwrap();

        // Shrink raw data to exactly the valid length, or return an
        // error if not enough data.
        bytes = bytes.get(..end_byte).ok_or(
            CorruptKind::HtreeInternalNodeCountTooLarge {
                inode,
                count,
                num_bytes: bytes.len(),
            },
        )?;

        Ok(Self {
            entries: bytes,
            limit,
        })
    }

    /// Look up the entry at `index`. Returns `(hash, block)`.
    /// Panics if `index` is out of range.
    ///
    /// For `index` zero, the `hash` key is implicitly zero.
    fn get_entry(&self, index: usize) -> (DirHash, FileBlockIndex) {
        // OK to unwrap: `ENTRY_SIZE` is 8 and `index` is at most
        // 2^16-1, so the result is at most 524,280. That fits in a `u32`,
        // and we assume that `usize` is at least that large.
        let offset: usize = Self::ENTRY_SIZE.checked_mul(index).unwrap();

        // OK to unwrap: `offset` is at most 2^19, so the result still
        // fits in a `u32` and we assume that `usize` is at least that
        // large.
        let block_offset: usize = offset.checked_add(4).unwrap();

        let block = read_u32le(self.entries, block_offset);

        let hash = if index == 0 {
            0
        } else {
            read_u32le(self.entries, offset)
        };

        (hash, block)
    }

    /// Get the number of entries (this is based on the `count` field,
    /// not the `limit` field).
    fn num_entries(&self) -> usize {
        self.entries.len() / Self::ENTRY_SIZE
    }

    fn limit(&self) -> usize {
        self.limit
    }

    fn lookup_entry_by_hash(
        &self,
        lookup_hash: DirHash,
    ) -> Option<(usize, FileBlockIndex)> {
        // Left/right entry index.
        let mut left = 0;
        let mut right = self.num_entries().checked_sub(1)?;

        while left <= right {
            let mid = left.checked_add(right)? / 2;
            let mid_hash = self.get_entry(mid).0;
            if mid_hash <= lookup_hash {
                left = mid.checked_add(1)?;
            } else {
                right = mid.checked_sub(1)?;
            }
        }

        let index = left.checked_sub(1)?;
        Some((index, self.get_entry(index).1))
    }

    /// Perform a binary search to find the child block index for the
    /// `lookup_hash`.
    #[cfg(test)]
    fn lookup_block_by_hash(
        &self,
        lookup_hash: DirHash,
    ) -> Option<FileBlockIndex> {
        Some(self.lookup_entry_by_hash(lookup_hash)?.1)
    }
}

#[maybe_async::maybe_async]
async fn root_block_index(
    fs: &Ext4,
    inode: &Inode,
) -> Result<FsBlockIndex, Ext4Error> {
    let mut file_blocks = FileBlocks::new(fs.clone(), inode)?;
    file_blocks
        .next()
        .await
        .ok_or(CorruptKind::DirEntry(inode.index))?
}

/// Read the block containing the root node of an htree into
/// `block`. This is always the first block of the file.
#[maybe_async::maybe_async]
pub(crate) async fn read_root_block(
    fs: &Ext4,
    inode: &Inode,
    block: &mut [u8],
) -> Result<(), Ext4Error> {
    let block_index = root_block_index(fs, inode).await?;

    // Read the first block of the extent.
    let dir_block = DirBlock {
        fs,
        dir_inode: inode.index,
        block_index,
        is_first: true,
        has_htree: true,
        checksum_base: inode.checksum_base().clone(),
    };
    dir_block.read(block).await
}

/// Check if name is "." or ".." and return the corresponding entry if
/// so. These entries exist at hardcoded offsets within the root block
/// of the htree.
///
/// `block` is the raw block data of the first directory block.
///
/// If name is neither "." nor "..", returns `None`.
fn read_dot_or_dotdot(
    fs: Ext4,
    inode: &Inode,
    name: DirEntryName<'_>,
    block: &[u8],
) -> Result<Option<DirEntry>, Ext4Error> {
    let corrupt = || CorruptKind::DirEntry(inode.index).into();

    let offset = if name == "." {
        0
    } else if name == ".." {
        12
    } else {
        return Ok(None);
    };

    let (entry, _size) = DirEntry::from_bytes(
        fs,
        &block[offset..],
        inode.index,
        Arc::new(PathBuf::empty()),
    )?;
    let entry = entry.ok_or_else(corrupt)?;
    if entry.file_name() == name {
        Ok(Some(entry))
    } else {
        Err(corrupt())
    }
}

/// Find the extent within a file that includes the given child `block`.
#[maybe_async::maybe_async]
async fn find_extent_for_block(
    fs: &Ext4,
    inode: &Inode,
    block: FileBlockIndex,
) -> Result<Extent, Ext4Error> {
    let mut extents = Extents::new(fs.clone(), inode)?;
    while let Some(extent) = extents.next().await {
        let extent = extent?;

        let start = extent.block_within_file;
        let end = start
            .checked_add(u32::from(extent.num_blocks))
            .ok_or(CorruptKind::DirEntry(inode.index))?;
        if block >= start && block < end {
            return Ok(extent);
        }
    }

    Err(CorruptKind::DirEntry(inode.index).into())
}

/// Convert from a block offset within a file to an absolute block index.
#[maybe_async::maybe_async]
async fn block_from_file_block(
    fs: &Ext4,
    inode: &Inode,
    relative_block: FileBlockIndex,
) -> Result<FsBlockIndex, Ext4Error> {
    if inode.flags().contains(InodeFlags::EXTENTS) {
        let extent = find_extent_for_block(fs, inode, relative_block).await?;
        let block_within_extent = relative_block
            .checked_sub(extent.block_within_file)
            .ok_or(CorruptKind::DirEntry(inode.index))?;
        let absolute_block = extent
            .start_block
            .checked_add(u64::from(block_within_extent))
            .ok_or(CorruptKind::DirEntry(inode.index))?;
        Ok(absolute_block)
    } else {
        #[expect(clippy::allow_attributes)]
        #[allow(unused_mut)]
        let mut block_map = FileBlocks::new(fs.clone(), inode)?;
        block_map
            .nth(usize_from_u32(relative_block))
            .await
            .ok_or(CorruptKind::DirEntry(inode.index))?
    }
}

#[maybe_async::maybe_async]
async fn read_internal_node_at_path<'a>(
    fs: &Ext4,
    inode: &Inode,
    path_entry: HtreePathEntry,
    block: &'a mut [u8],
) -> Result<InternalNode<'a>, Ext4Error> {
    let dir_block = DirBlock {
        fs,
        dir_inode: inode.index,
        block_index: path_entry.absolute_block,
        is_first: path_entry.kind.is_first(),
        has_htree: true,
        checksum_base: inode.checksum_base().clone(),
    };
    dir_block.read(block).await?;

    match path_entry.kind {
        HtreeNodeKind::Root => {
            InternalNode::from_root_block(block, inode.index)
        }
        HtreeNodeKind::Internal => {
            InternalNode::from_non_root_block(block, inode.index)
        }
    }
}

fn hash_can_continue_in_next_block(hash: u32, next_hash: u32) -> bool {
    if (hash & 1) != 0 {
        true
    } else {
        (next_hash & !1) == hash
    }
}

fn scan_leaf_block_for_name(
    fs: Ext4,
    inode: &Inode,
    name: DirEntryName<'_>,
    block: &[u8],
) -> Result<Option<DirEntry>, Ext4Error> {
    let path = Arc::new(PathBuf::empty());
    let mut offset_within_block = 0usize;

    while offset_within_block < block.len() {
        let (dir_entry, entry_size) = DirEntry::from_bytes(
            fs.clone(),
            &block[offset_within_block..],
            inode.index,
            path.clone(),
        )?;
        offset_within_block = offset_within_block
            .checked_add(entry_size.get())
            .ok_or(CorruptKind::DirEntry(inode.index))?;

        let Some(dir_entry) = dir_entry else {
            continue;
        };

        if dir_entry.file_name() == name {
            return Ok(Some(dir_entry));
        }
    }

    Ok(None)
}

/// Traverse the htree to find the first leaf node that might contain
/// `name`, along with the full index path used to reach that leaf.
///
/// If multiple adjacent leaf blocks may contain the same hash due to a
/// hash collision split, the returned lookup points at the first such
/// leaf. Use [`advance_leaf_lookup_to_next`] to continue scanning.
///
/// On success, `block` will contain the leaf node's directory block
/// data.
#[maybe_async::maybe_async]
pub(crate) async fn find_leaf_lookup(
    fs: &Ext4,
    inode: &Inode,
    name: DirEntryName<'_>,
    block: &mut [u8],
) -> Result<HtreeLeafLookup, Ext4Error> {
    let hash_alg = HashAlg::from_u8(block[0x1c])?;
    let hash = hash_alg.hash(name, &fs.0.superblock.htree_hash_seed());

    // Read the htree depth from the root block. The depth is the
    // number of internal levels between the root and the leaves.
    let depth = block[0x1e];

    let root_absolute_block = root_block_index(fs, inode).await?;
    let root_node = InternalNode::from_root_block(block, inode.index)?;
    let (root_child_entry_index, mut child_block_relative) = root_node
        .lookup_entry_by_hash(hash)
        .ok_or(CorruptKind::DirEntry(inode.index))?;

    let mut path = vec![HtreePathEntry {
        kind: HtreeNodeKind::Root,
        absolute_block: root_absolute_block,
        relative_block: 0,
        child_entry_index: root_child_entry_index,
    }];

    let mut leaf_absolute_block = 0;

    for level in 0..=depth {
        let block_index =
            block_from_file_block(fs, inode, child_block_relative).await?;
        leaf_absolute_block = block_index;
        DirBlock {
            fs,
            dir_inode: inode.index,
            block_index,
            is_first: false,
            has_htree: true,
            checksum_base: inode.checksum_base().clone(),
        }
        .read(block)
        .await?;

        if level != depth {
            let inner_node =
                InternalNode::from_non_root_block(block, inode.index)?;
            let (child_entry_index, next_child_block_relative) = inner_node
                .lookup_entry_by_hash(hash)
                .ok_or(CorruptKind::DirEntry(inode.index))?;
            path.push(HtreePathEntry {
                kind: HtreeNodeKind::Internal,
                absolute_block: block_index,
                relative_block: child_block_relative,
                child_entry_index,
            });
            child_block_relative = next_child_block_relative;
        }
    }

    Ok(HtreeLeafLookup {
        hash,
        leaf_absolute_block,
        leaf_relative_block: child_block_relative,
        path,
    })
}

/// Advance an htree leaf lookup to the next leaf block that may still
/// contain `lookup.hash` due to a collision split.
#[maybe_async::maybe_async]
pub(crate) async fn advance_leaf_lookup_to_next(
    fs: &Ext4,
    inode: &Inode,
    lookup: &mut HtreeLeafLookup,
    block: &mut [u8],
) -> Result<bool, Ext4Error> {
    let block_size = fs.0.superblock.block_size().to_usize();
    let mut node_block = vec![0; block_size];

    let mut path_index = lookup
        .path
        .len()
        .checked_sub(1)
        .ok_or(CorruptKind::DirEntry(inode.index))?;

    let (next_hash, mut child_relative_block) = loop {
        let node = read_internal_node_at_path(
            fs,
            inode,
            lookup.path[path_index],
            &mut node_block,
        )
        .await?;
        let next_entry_index = lookup.path[path_index]
            .child_entry_index
            .checked_add(1)
            .ok_or(Ext4Error::NoSpace)?;

        if next_entry_index < node.num_entries() {
            lookup.path[path_index].child_entry_index = next_entry_index;
            break node.get_entry(next_entry_index);
        }

        if path_index == 0 {
            return Ok(false);
        }
        path_index = path_index.checked_sub(1).unwrap();
    };

    if !hash_can_continue_in_next_block(lookup.hash, next_hash) {
        return Ok(false);
    }

    for level_index in path_index.checked_add(1).unwrap()..lookup.path.len() {
        let absolute_block =
            block_from_file_block(fs, inode, child_relative_block).await?;
        DirBlock {
            fs,
            dir_inode: inode.index,
            block_index: absolute_block,
            is_first: false,
            has_htree: true,
            checksum_base: inode.checksum_base().clone(),
        }
        .read(&mut node_block)
        .await?;

        let node = InternalNode::from_non_root_block(&node_block, inode.index)?;
        lookup.path[level_index] = HtreePathEntry {
            kind: HtreeNodeKind::Internal,
            absolute_block,
            relative_block: child_relative_block,
            child_entry_index: 0,
        };
        child_relative_block = node.get_entry(0).1;
    }

    let leaf_absolute_block =
        block_from_file_block(fs, inode, child_relative_block).await?;
    DirBlock {
        fs,
        dir_inode: inode.index,
        block_index: leaf_absolute_block,
        is_first: false,
        has_htree: true,
        checksum_base: inode.checksum_base().clone(),
    }
    .read(block)
    .await?;

    lookup.leaf_absolute_block = leaf_absolute_block;
    lookup.leaf_relative_block = child_relative_block;
    Ok(true)
}

fn non_root_node_limit(fs: &Ext4) -> Result<usize, Ext4Error> {
    let block_size = fs.0.superblock.block_size().to_usize();
    let tail_size = if fs.has_metadata_checksums() {
        InternalNode::ENTRY_SIZE
    } else {
        0
    };
    block_size
        .checked_sub(HtreeNodeKind::Internal.entries_offset())
        .and_then(|n| n.checked_sub(tail_size))
        .map(|n| n / InternalNode::ENTRY_SIZE)
        .ok_or(CorruptKind::InvalidBlockSize.into())
}

fn make_empty_non_root_internal_block(
    fs: &Ext4,
    inode: &Inode,
) -> Result<Vec<u8>, Ext4Error> {
    let block_size = fs.0.superblock.block_size().to_usize();
    let mut block = vec![0; block_size];
    write_u16le(
        &mut block,
        4,
        u16::try_from(block_size)
            .map_err(|_| CorruptKind::DirEntry(inode.index))?,
    );
    Ok(block)
}

#[maybe_async::maybe_async]
async fn append_htree_block(
    fs: &Ext4,
    inode: &mut Inode,
    block: &[u8],
) -> Result<(FsBlockIndex, FileBlockIndex), Ext4Error> {
    let block_size = fs.0.superblock.block_size().to_usize();
    let block_size_u64 = fs.0.superblock.block_size().to_nz_u64();
    if block.len() != block_size {
        return Err(CorruptKind::DirEntry(inode.index).into());
    }

    let append_offset = inode.size_in_bytes();
    if append_offset % block_size_u64 != 0 {
        return Err(CorruptKind::DirEntry(inode.index).into());
    }

    let relative_block = u32::try_from(append_offset / block_size_u64)
        .map_err(|_| Ext4Error::NoSpace)?;
    let n = crate::file::write_at(fs, inode, block, append_offset).await?;
    if n != block.len() {
        return Err(Ext4Error::NoSpace);
    }

    let absolute_block =
        block_from_file_block(fs, inode, relative_block).await?;
    Ok((absolute_block, relative_block))
}

/// Ensure that the parent node of `lookup` has room for one more child
/// entry, splitting index blocks like `ext4_dir_idx.c` when needed.
#[maybe_async::maybe_async]
pub(crate) async fn split_index_path_for_new_child(
    fs: &Ext4,
    inode: &mut Inode,
    lookup: &mut HtreeLeafLookup,
) -> Result<(), Ext4Error> {
    let block_size = fs.0.superblock.block_size().to_usize();
    let node_limit = non_root_node_limit(fs)?;
    let mut parent_block = vec![0; block_size];

    let parent = lookup.parent();
    let parent_node =
        read_internal_node_at_path(fs, inode, parent, &mut parent_block)
            .await?;
    if parent_node.num_entries() < parent_node.limit() {
        return Ok(());
    }

    let parent_entries_offset = parent.kind.entries_offset();
    let parent_count = parent_node.num_entries();
    let corrupt = || Ext4Error::from(CorruptKind::DirEntry(inode.index));

    if parent.kind == HtreeNodeKind::Root {
        let old_child_entry_index = parent.child_entry_index;
        let parent_bytes_len = parent_count
            .checked_mul(InternalNode::ENTRY_SIZE)
            .ok_or(Ext4Error::NoSpace)?;

        let mut new_block = make_empty_non_root_internal_block(fs, inode)?;
        let new_entries_end = HtreeNodeKind::Internal
            .entries_offset()
            .checked_add(parent_bytes_len)
            .ok_or(Ext4Error::NoSpace)?;
        let old_entries_end = parent_entries_offset
            .checked_add(parent_bytes_len)
            .ok_or(Ext4Error::NoSpace)?;
        new_block[HtreeNodeKind::Internal.entries_offset()..new_entries_end]
            .copy_from_slice(
                &parent_block[parent_entries_offset..old_entries_end],
            );
        write_u16le(
            &mut new_block,
            HtreeNodeKind::Internal.entries_offset(),
            u16::try_from(node_limit).map_err(|_| corrupt())?,
        );
        write_u16le(
            &mut new_block,
            HtreeNodeKind::Internal
                .entries_offset()
                .checked_add(2)
                .ok_or(Ext4Error::NoSpace)?,
            u16::try_from(parent_count).map_err(|_| corrupt())?,
        );
        DirBlock {
            fs,
            block_index: 0,
            is_first: false,
            dir_inode: inode.index,
            has_htree: true,
            checksum_base: inode.checksum_base().clone(),
        }
        .update_checksum(&mut new_block)?;

        let (new_absolute_block, new_relative_block) =
            append_htree_block(fs, inode, &new_block).await?;

        write_u16le(
            &mut parent_block,
            parent_entries_offset
                .checked_add(2)
                .ok_or(Ext4Error::NoSpace)?,
            1,
        );
        write_u32le(
            &mut parent_block,
            parent_entries_offset
                .checked_add(4)
                .ok_or(Ext4Error::NoSpace)?,
            new_relative_block,
        );
        parent_block[0x1e] = 1;
        DirBlock {
            fs,
            block_index: parent.absolute_block,
            is_first: true,
            dir_inode: inode.index,
            has_htree: true,
            checksum_base: inode.checksum_base().clone(),
        }
        .update_checksum(&mut parent_block)?;
        fs.write_to_block(parent.absolute_block, 0, &parent_block)
            .await?;

        lookup.path[0].child_entry_index = 0;
        lookup.path.push(HtreePathEntry {
            kind: HtreeNodeKind::Internal,
            absolute_block: new_absolute_block,
            relative_block: new_relative_block,
            child_entry_index: old_child_entry_index,
        });
        return Ok(());
    }

    if lookup.path.len() != 2 {
        return Err(Ext4Error::NoSpace);
    }

    let root = lookup.path[0];
    let mut root_block = vec![0; block_size];
    let root_node =
        read_internal_node_at_path(fs, inode, root, &mut root_block).await?;
    if root_node.num_entries() >= root_node.limit() {
        return Err(Ext4Error::NoSpace);
    }

    let count_left = parent_count / 2;
    let count_right = parent_count
        .checked_sub(count_left)
        .ok_or(Ext4Error::NoSpace)?;
    let hash_right = parent_node.get_entry(count_left).0;

    let right_src_offset = parent_entries_offset
        .checked_add(
            count_left
                .checked_mul(InternalNode::ENTRY_SIZE)
                .ok_or(Ext4Error::NoSpace)?,
        )
        .ok_or(Ext4Error::NoSpace)?;
    let right_len = count_right
        .checked_mul(InternalNode::ENTRY_SIZE)
        .ok_or(Ext4Error::NoSpace)?;
    let right_src_end = right_src_offset
        .checked_add(right_len)
        .ok_or(Ext4Error::NoSpace)?;

    let mut new_block = make_empty_non_root_internal_block(fs, inode)?;
    let new_entries_end = HtreeNodeKind::Internal
        .entries_offset()
        .checked_add(right_len)
        .ok_or(Ext4Error::NoSpace)?;
    new_block[HtreeNodeKind::Internal.entries_offset()..new_entries_end]
        .copy_from_slice(&parent_block[right_src_offset..right_src_end]);
    write_u16le(
        &mut new_block,
        HtreeNodeKind::Internal.entries_offset(),
        u16::try_from(node_limit).map_err(|_| corrupt())?,
    );
    write_u16le(
        &mut new_block,
        HtreeNodeKind::Internal
            .entries_offset()
            .checked_add(2)
            .ok_or(Ext4Error::NoSpace)?,
        u16::try_from(count_right).map_err(|_| corrupt())?,
    );
    DirBlock {
        fs,
        block_index: 0,
        is_first: false,
        dir_inode: inode.index,
        has_htree: true,
        checksum_base: inode.checksum_base().clone(),
    }
    .update_checksum(&mut new_block)?;

    write_u16le(
        &mut parent_block,
        parent_entries_offset
            .checked_add(2)
            .ok_or(Ext4Error::NoSpace)?,
        u16::try_from(count_left).map_err(|_| corrupt())?,
    );
    DirBlock {
        fs,
        block_index: parent.absolute_block,
        is_first: false,
        dir_inode: inode.index,
        has_htree: true,
        checksum_base: inode.checksum_base().clone(),
    }
    .update_checksum(&mut parent_block)?;

    let (new_absolute_block, new_relative_block) =
        append_htree_block(fs, inode, &new_block).await?;
    fs.write_to_block(parent.absolute_block, 0, &parent_block)
        .await?;

    insert_child_into_parent(fs, inode, root, hash_right, new_relative_block)
        .await?;

    if lookup.path[1].child_entry_index >= count_left {
        lookup.path[0].child_entry_index = lookup.path[0]
            .child_entry_index
            .checked_add(1)
            .ok_or(Ext4Error::NoSpace)?;
        lookup.path[1] = HtreePathEntry {
            kind: HtreeNodeKind::Internal,
            absolute_block: new_absolute_block,
            relative_block: new_relative_block,
            child_entry_index: lookup.path[1]
                .child_entry_index
                .checked_sub(count_left)
                .ok_or(Ext4Error::NoSpace)?,
        };
    }

    Ok(())
}

#[maybe_async::maybe_async]
pub(crate) async fn insert_child_into_parent(
    fs: &Ext4,
    inode: &Inode,
    parent: HtreePathEntry,
    hash: u32,
    child_relative_block: FileBlockIndex,
) -> Result<(), Ext4Error> {
    let block_size = fs.0.superblock.block_size().to_usize();
    let mut block = vec![0; block_size];

    let entries_offset = parent.kind.entries_offset();
    let dir_block = DirBlock {
        fs,
        dir_inode: inode.index,
        block_index: parent.absolute_block,
        is_first: parent.kind.is_first(),
        has_htree: true,
        checksum_base: inode.checksum_base().clone(),
    };
    dir_block.read(&mut block).await?;

    let node = match parent.kind {
        HtreeNodeKind::Root => {
            InternalNode::from_root_block(&block, inode.index)?
        }
        HtreeNodeKind::Internal => {
            InternalNode::from_non_root_block(&block, inode.index)?
        }
    };

    let count = node.num_entries();
    if count >= node.limit() {
        return Err(Ext4Error::NoSpace);
    }

    let insert_index = parent
        .child_entry_index
        .checked_add(1)
        .ok_or(Ext4Error::NoSpace)?;
    let insert_offset = entries_offset
        .checked_add(
            insert_index
                .checked_mul(InternalNode::ENTRY_SIZE)
                .ok_or(Ext4Error::NoSpace)?,
        )
        .ok_or(Ext4Error::NoSpace)?;
    let old_end = entries_offset
        .checked_add(
            count
                .checked_mul(InternalNode::ENTRY_SIZE)
                .ok_or(Ext4Error::NoSpace)?,
        )
        .ok_or(Ext4Error::NoSpace)?;
    block.copy_within(
        insert_offset..old_end,
        insert_offset.checked_add(8).ok_or(Ext4Error::NoSpace)?,
    );
    write_u32le(&mut block, insert_offset, hash);
    write_u32le(
        &mut block,
        insert_offset.checked_add(4).ok_or(Ext4Error::NoSpace)?,
        child_relative_block,
    );
    write_u16le(
        &mut block,
        entries_offset.checked_add(2).ok_or(Ext4Error::NoSpace)?,
        u16::try_from(count.checked_add(1).ok_or(Ext4Error::NoSpace)?)
            .map_err(|_| Ext4Error::NoSpace)?,
    );

    dir_block.update_checksum(&mut block)?;
    fs.write_to_block(parent.absolute_block, 0, &block).await?;

    Ok(())
}

/// Find a directory entry via a directory htree. The htree is a tree of
/// nodes that use hashes for keys. The hash of `name` is used to
/// traverse this tree to a leaf node. The leaf node is an linear array
/// of directory entries; these are searched through in order to find
/// the one matching `name`.
///
/// Returns [`Ext4Error::NotFound`] if the entry doesn't exist.
///
/// Panics if the directory doesn't have an htree.
#[maybe_async::maybe_async]
pub(crate) async fn get_dir_entry_via_htree(
    fs: &Ext4,
    inode: &Inode,
    name: DirEntryName<'_>,
) -> Result<DirEntry, Ext4Error> {
    assert!(inode.flags().contains(InodeFlags::DIRECTORY_HTREE));

    let block_size = fs.0.superblock.block_size();
    let mut block = vec![0; block_size.to_usize()];

    // Read the first block of the file, which contains the root node of
    // the htree.
    read_root_block(fs, inode, &mut block).await?;

    // Handle "." and ".." entries.
    if let Some(entry) = read_dot_or_dotdot(fs.clone(), inode, name, &block)? {
        return Ok(entry);
    }

    let mut lookup = find_leaf_lookup(fs, inode, name, &mut block).await?;

    loop {
        if let Some(dir_entry) =
            scan_leaf_block_for_name(fs.clone(), inode, name, &block)?
        {
            return Ok(dir_entry);
        }

        if !advance_leaf_lookup_to_next(fs, inode, &mut lookup, &mut block)
            .await?
        {
            return Err(Ext4Error::NotFound);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "std")]
    use crate::{FollowSymlinks, Path, ReadDir};

    #[test]
    fn test_internal_node() {
        let inode = InodeIndex::new(1).unwrap();

        let mut bytes = Vec::new();
        let add_entry =
            |bytes: &mut Vec<u8>, hash: DirHash, block: FileBlockIndex| {
                bytes.extend(hash.to_le_bytes());
                bytes.extend(block.to_le_bytes());
            };
        bytes.extend(20u16.to_le_bytes()); // limit
        bytes.extend(11u16.to_le_bytes()); // count
        bytes.extend(100u32.to_le_bytes()); // block

        add_entry(&mut bytes, 2, 199);
        add_entry(&mut bytes, 4, 198);
        add_entry(&mut bytes, 6, 197);
        add_entry(&mut bytes, 8, 196);
        add_entry(&mut bytes, 10, 195);

        add_entry(&mut bytes, 12, 194);
        add_entry(&mut bytes, 14, 193);
        add_entry(&mut bytes, 16, 192);
        add_entry(&mut bytes, 18, 191);
        add_entry(&mut bytes, 20, 190);

        // Test search with an odd number of entries.
        let node = InternalNode::new(&bytes, inode).unwrap();
        assert_eq!(node.num_entries(), 11);
        assert_eq!(node.get_entry(0), (0, 100));
        assert_eq!(node.get_entry(10), (20, 190));
        assert_eq!(node.lookup_block_by_hash(0), Some(100));
        assert_eq!(node.lookup_block_by_hash(9), Some(196));
        assert_eq!(node.lookup_block_by_hash(10), Some(195));
        assert_eq!(node.lookup_block_by_hash(11), Some(195));
        assert_eq!(node.lookup_block_by_hash(12), Some(194));
        assert_eq!(node.lookup_block_by_hash(20), Some(190));
        assert_eq!(node.lookup_block_by_hash(30), Some(190));

        // Add one more entry.
        bytes[2..4].copy_from_slice(&12u16.to_le_bytes()); // count
        add_entry(&mut bytes, 30, 189);

        // Test search with an even number of entries.
        let node = InternalNode::new(&bytes, inode).unwrap();
        assert_eq!(node.num_entries(), 12);
        assert_eq!(node.lookup_block_by_hash(0), Some(100));
        assert_eq!(node.lookup_block_by_hash(9), Some(196));
        assert_eq!(node.lookup_block_by_hash(10), Some(195));
        assert_eq!(node.lookup_block_by_hash(11), Some(195));
        assert_eq!(node.lookup_block_by_hash(12), Some(194));
        assert_eq!(node.lookup_block_by_hash(20), Some(190));
        assert_eq!(node.lookup_block_by_hash(30), Some(189));
    }

    #[test]
    fn test_internal_node_errors() {
        let inode = InodeIndex::new(123).unwrap();
        assert_eq!(
            InternalNode::new(&[0; 7], inode).unwrap_err(),
            CorruptKind::HtreeInternalNodeMissingHeader {
                inode,
                num_bytes: 7,
            }
        );

        let mut bytes = Vec::new();
        bytes.extend(20u16.to_le_bytes()); // limit
        bytes.extend(2u16.to_le_bytes()); // count
        bytes.extend(456u32.to_le_bytes()); // zero block
        assert_eq!(
            InternalNode::new(&bytes, inode).unwrap_err(),
            CorruptKind::HtreeInternalNodeCountTooLarge {
                inode,
                num_bytes: 8,
                count: 2,
            }
        );
    }

    #[test]
    fn test_hash_can_continue_in_next_block() {
        assert!(hash_can_continue_in_next_block(0x1234_5678, 0x1234_5678));
        assert!(hash_can_continue_in_next_block(0x1234_5678, 0x1234_5679));
        assert!(!hash_can_continue_in_next_block(0x1234_5678, 0x2234_5678));
    }

    #[cfg(feature = "std")]
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_read_dot_or_dotdot() {
        let fs = crate::test_util::load_test_disk1().await;

        let mut block = vec![0; fs.0.superblock.block_size().to_usize()];

        // Read the root block of an htree.
        let inode = fs
            .path_to_inode("/big_dir".try_into().unwrap(), FollowSymlinks::All)
            .await
            .unwrap();
        read_root_block(&fs, &inode, &mut block).await.unwrap();

        // Get the "." entry.
        let entry = read_dot_or_dotdot(
            fs.clone(),
            &inode,
            ".".try_into().unwrap(),
            &block,
        )
        .unwrap()
        .unwrap();
        assert_eq!(entry.file_name(), ".");

        // Get the ".." entry.
        let entry = read_dot_or_dotdot(
            fs.clone(),
            &inode,
            "..".try_into().unwrap(),
            &block,
        )
        .unwrap()
        .unwrap();
        assert_eq!(entry.file_name(), "..");

        // Check that an arbitrary name returns `None`.
        assert!(
            read_dot_or_dotdot(
                fs.clone(),
                &inode,
                "somename".try_into().unwrap(),
                &block
            )
            .unwrap()
            .is_none()
        );

        // Error: the first directory entry in the root node is not ".".
        assert_eq!(block[8], b'.');
        block[8] = b'x';
        assert_eq!(
            read_dot_or_dotdot(
                fs.clone(),
                &inode,
                ".".try_into().unwrap(),
                &block,
            )
            .unwrap_err(),
            CorruptKind::DirEntry(inode.index)
        );

        // Error: invalid directory block.
        block.fill(0);
        assert_eq!(
            read_dot_or_dotdot(
                fs.clone(),
                &inode,
                ".".try_into().unwrap(),
                &block,
            )
            .unwrap_err(),
            CorruptKind::DirEntryRecordTooSmall(inode.index, 0)
        );
    }

    /// Use ReadDir to iterate over all directory entries. Check that
    /// each entry can be looked up directly via the htree.
    ///
    /// Returns the number of entries.
    #[cfg(feature = "std")]
    #[maybe_async::maybe_async]
    async fn compare_all_entries(fs: &Ext4, dir: Path<'_>) -> usize {
        let dir_inode =
            fs.path_to_inode(dir, FollowSymlinks::All).await.unwrap();
        let mut iter =
            ReadDir::new(fs.clone(), &dir_inode, PathBuf::from(dir)).unwrap();
        let mut count: usize = 0;
        while let Some(iter_entry) = iter.next().await {
            let iter_entry = iter_entry.unwrap();
            let htree_entry =
                get_dir_entry_via_htree(fs, &dir_inode, iter_entry.file_name())
                    .await
                    .unwrap();
            assert_eq!(htree_entry.file_name(), iter_entry.file_name());
            assert_eq!(htree_entry.inode, iter_entry.inode);
            count = count.checked_add(1).unwrap();
        }
        count
    }

    // TODO: Debug
    #[cfg(feature = "std")]
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    #[ignore]
    async fn test_get_dir_entry_via_htree() {
        let fs = crate::test_util::load_test_disk1().await;

        // Resolve paths in `/medium_dir` via htree.
        let medium_dir = Path::new("/medium_dir");
        let cmp = compare_all_entries(&fs, medium_dir).await;
        assert_eq!(cmp, 1_002);

        // Resolve paths in `/big_dir` via htree.
        let big_dir = Path::new("/big_dir");
        let cmp = compare_all_entries(&fs, big_dir).await;
        assert_eq!(cmp, 10_002);
    }

    // TODO: Debug
    /// Test `block_from_file_block` with a file that uses extents.
    #[cfg(feature = "std")]
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    #[ignore]
    async fn test_block_from_file_block() {
        let fs = crate::test_util::load_test_disk1().await;

        // Manually construct a simple extent tree containing two
        // extents.
        //
        // The test disk has experienced relatively few operations
        // compared to a real-world filesystem, so it doesn't have much
        // fragmentation. In particular, all of its directory tree
        // inodes currently have a single extent with a relative offset
        // of 0, which doesn't fully exercise
        // `block_from_file_block`. Create some slightly more
        // interesting extents to test here.
        let mut extents = Vec::new();
        // Node header:
        // Magic:
        extents.extend(&0xf30au16.to_le_bytes());
        // Num entries:
        extents.extend(&2u16.to_le_bytes());
        // Max entries:
        extents.extend(&2u16.to_le_bytes());
        // Depth (leaf):
        extents.extend(&0u16.to_le_bytes());
        // Padding:
        extents.extend(&0u32.to_le_bytes());
        // Extent 0:
        // Relative start block:
        extents.extend(&0u32.to_le_bytes());
        // Num blocks:
        extents.extend(&23u16.to_le_bytes());
        // Absolute start block (hi, lo):
        extents.extend(0u16.to_le_bytes());
        extents.extend(2543u32.to_le_bytes());
        // Extent 1:
        // Relative start block:
        extents.extend(&23u32.to_le_bytes());
        // Num blocks:
        extents.extend(&47u16.to_le_bytes());
        // Absolute start block (hi, lo):
        extents.extend(0u16.to_le_bytes());
        extents.extend(11u32.to_le_bytes());

        extents.resize(60usize, 0u8);

        // Grab a convenient inode and overwrite its inline data with
        // the new extent tree.
        let inode = fs
            .path_to_inode(
                "/medium_dir".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        inode.inline_data().copy_from_slice(&extents);

        // Verify the extents.
        let extents: Vec<_> = Extents::new(fs.clone(), &inode)
            .unwrap()
            .map(|e| e.unwrap())
            .await
            .collect()
            .await;
        assert_eq!(
            extents,
            [Extent::new(0, 2543, 23), Extent::new(23, 11, 47),]
        );

        // Blocks in extent 0.
        let block = block_from_file_block(&fs, &inode, 0).await;
        assert_eq!(block.unwrap(), 2543);
        let block = block_from_file_block(&fs, &inode, 1).await;
        assert_eq!(block.unwrap(), 2544);
        let block = block_from_file_block(&fs, &inode, 22).await;
        assert_eq!(block.unwrap(), 2565);

        // Blocks in extent 1.
        let block = block_from_file_block(&fs, &inode, 23).await;
        assert_eq!(block.unwrap(), 11);
        let block = block_from_file_block(&fs, &inode, 24).await;
        assert_eq!(block.unwrap(), 12);
        let block = block_from_file_block(&fs, &inode, 69).await;
        assert_eq!(block.unwrap(), 57);

        // Invalid block.
        let block = block_from_file_block(&fs, &inode, 70).await;
        assert!(block.is_err());
    }
}
