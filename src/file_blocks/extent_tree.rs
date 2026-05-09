// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::Ext4;
use crate::block_index::{FileBlockIndex, FsBlockIndex};
use crate::checksum::Checksum;
use crate::error::{CorruptKind, Ext4Error};
use crate::extent::Extent;
use crate::inode::{Inode, InodeIndex};
use crate::util::{
    read_u16le, read_u32le, u64_from_hilo, u64_to_hilo, usize_from_u32,
};
use alloc::vec;
use alloc::vec::Vec;
use core::num::NonZeroU32;

/// Find the child index to descend into for `block_index`.
///
/// Returns the index of the last internal node with `block_within_file <= block_index`.
fn find_child_index(
    nodes: &[ExtentInternalNode],
    block_index: FileBlockIndex,
) -> Option<usize> {
    let mut best_index = None;
    for (i, node) in nodes.iter().enumerate() {
        if node.block_within_file > block_index {
            break;
        }
        best_index = Some(i);
    }
    best_index
}

/// Size of each entry within an extent node (including the header
/// entry).
const ENTRY_SIZE_IN_BYTES: usize = 12;

const EXTENT_MAGIC: u16 = 0xf30a;

/// Header at the start of a node in an extent tree.
///
/// An extent tree is made up of nodes. Each node may be internal or
/// leaf. Leaf nodes contain `Extent`s. Internal nodes point at other
/// nodes.
///
/// Each node starts with a `NodeHeader` that includes the node's depth
/// (depth 0 is a leaf node) and the number of entries in the node.
#[derive(Copy, Clone)]
struct NodeHeader {
    /// Number of entries in this node, not including the header.
    num_entries: u16,

    /// Maximum number of entries in this node, not including the header.
    max_entries: u16,

    /// Depth of this node in the tree. Zero means it's a leaf node. The
    /// maximum depth is five.
    depth: u16,

    /// The generation number of this node. Used by lustre
    generation: u32,
}

/// Returns `(n + 1) * ENTRY_SIZE_IN_BYTES`.
///
/// The maximum value this returns is 786432.
fn add_one_mul_entry_size(n: u16) -> usize {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "u16::MAX + 1 is 65536, and 65536 * 12 fits in u32"
    )]
    {
        usize_from_u32((u32::from(n) + 1) * 12)
    }
}

fn checked_num_entries(
    len: usize,
    inode: InodeIndex,
) -> Result<u16, Ext4Error> {
    u16::try_from(len).map_err(|_| CorruptKind::ExtentNodeSize(inode).into())
}

fn checked_entry_end(
    offset: usize,
    inode: InodeIndex,
) -> Result<usize, Ext4Error> {
    offset
        .checked_add(ENTRY_SIZE_IN_BYTES)
        .ok_or(CorruptKind::ExtentNotEnoughData(inode).into())
}

fn extent_end(
    extent: &Extent,
    inode: InodeIndex,
) -> Result<FileBlockIndex, Ext4Error> {
    extent
        .block_within_file
        .checked_add(FileBlockIndex::from(extent.num_blocks))
        .ok_or(CorruptKind::ExtentBlock(inode).into())
}

fn extent_physical_end(
    extent: &Extent,
    inode: InodeIndex,
) -> Result<FsBlockIndex, Ext4Error> {
    extent
        .start_block
        .checked_add(FsBlockIndex::from(extent.num_blocks))
        .ok_or(CorruptKind::ExtentBlock(inode).into())
}

impl NodeHeader {
    /// Size of the node, including the header.
    fn node_size_in_bytes(&self) -> usize {
        add_one_mul_entry_size(self.num_entries)
    }

    /// Offset of the node's extent data.
    ///
    /// Per `add_one_mul_entry_size`, the maximum value this returns is
    /// 786432.
    fn checksum_offset(&self) -> usize {
        add_one_mul_entry_size(self.max_entries)
    }
}

impl NodeHeader {
    /// Read a `NodeHeader` from a byte slice.
    fn from_bytes(data: &[u8], inode: InodeIndex) -> Result<Self, Ext4Error> {
        if data.len() < ENTRY_SIZE_IN_BYTES {
            return Err(CorruptKind::ExtentNotEnoughData(inode).into());
        }

        let eh_magic = read_u16le(data, 0);
        let eh_entries = read_u16le(data, 2);
        let eh_max = read_u16le(data, 4);
        let eh_depth = read_u16le(data, 6);
        let eh_generation = read_u32le(data, 8);

        if eh_magic != EXTENT_MAGIC {
            return Err(CorruptKind::ExtentMagic(inode).into());
        }

        if eh_depth > 5 {
            return Err(CorruptKind::ExtentDepth(inode).into());
        }

        Ok(Self {
            depth: eh_depth,
            num_entries: eh_entries,
            max_entries: eh_max,
            generation: eh_generation,
        })
    }

    fn to_bytes(self) -> [u8; ENTRY_SIZE_IN_BYTES] {
        let mut bytes = [0u8; ENTRY_SIZE_IN_BYTES];
        bytes[0..2].copy_from_slice(&EXTENT_MAGIC.to_le_bytes());
        bytes[2..4].copy_from_slice(&self.num_entries.to_le_bytes());
        bytes[4..6].copy_from_slice(&self.max_entries.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.depth.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.generation.to_le_bytes());
        bytes
    }
}

#[derive(Copy, Clone)]
struct ExtentInternalNode {
    /// Offset of the block within the file.
    pub(crate) block_within_file: FileBlockIndex,

    /// This is the actual block within the filesystem.
    pub(crate) block: FsBlockIndex,
}

impl ExtentInternalNode {
    pub(crate) fn from_bytes(
        data: &[u8],
        inode: InodeIndex,
    ) -> Result<Self, Ext4Error> {
        if data.len() < ENTRY_SIZE_IN_BYTES {
            return Err(CorruptKind::ExtentNotEnoughData(inode).into());
        }

        let ei_block = read_u32le(data, 0);
        let (ei_start_lo, ei_start_hi) =
            (read_u32le(data, 4), read_u16le(data, 8));
        let ei_start = u64_from_hilo(u32::from(ei_start_hi), ei_start_lo);

        Ok(Self {
            block_within_file: ei_block,
            block: ei_start,
        })
    }

    pub(crate) fn to_bytes(self) -> Result<[u8; 12], Ext4Error> {
        let mut bytes = [0u8; 12];
        bytes[0..4].copy_from_slice(&self.block_within_file.to_le_bytes());
        let (ei_start_hi, ei_start_lo) = u64_to_hilo(self.block);
        let ei_start_hi = u16::try_from(ei_start_hi)
            .map_err(|_| CorruptKind::ExtentBlockOverflow(self.block))?;
        bytes[4..8].copy_from_slice(&ei_start_lo.to_le_bytes());
        bytes[8..10].copy_from_slice(&ei_start_hi.to_le_bytes());
        // The last two bytes are unused.
        Ok(bytes)
    }
}

#[derive(Clone)]
enum ExtentNodeEntries {
    Internal(Vec<ExtentInternalNode>),
    Leaf(Vec<Extent>),
}

impl ExtentNodeEntries {
    fn from_bytes(
        data: &[u8],
        header: &NodeHeader,
        inode: InodeIndex,
    ) -> Result<Self, Ext4Error> {
        if header.depth == 0 {
            let mut entries = Vec::with_capacity(usize_from_u32(u32::from(
                header.num_entries,
            )));
            for i in 0..header.num_entries {
                let offset = add_one_mul_entry_size(i);
                let entry_end = checked_entry_end(offset, inode)?;
                let entry = Extent::from_bytes(
                    data.get(offset..entry_end)
                        .ok_or(CorruptKind::ExtentNotEnoughData(inode))?,
                );
                entries.push(entry);
            }
            Ok(Self::Leaf(entries))
        } else {
            let mut entries = Vec::with_capacity(usize_from_u32(u32::from(
                header.num_entries,
            )));
            for i in 0..header.num_entries {
                let offset = add_one_mul_entry_size(i);
                let entry_end = checked_entry_end(offset, inode)?;
                let entry = ExtentInternalNode::from_bytes(
                    data.get(offset..entry_end)
                        .ok_or(CorruptKind::ExtentNotEnoughData(inode))?,
                    inode,
                )?;
                entries.push(entry);
            }
            Ok(Self::Internal(entries))
        }
    }
}

#[derive(Clone)]
pub(crate) struct ExtentNode {
    block: Option<FsBlockIndex>,
    header: NodeHeader,
    entries: ExtentNodeEntries,
}

impl ExtentNode {
    fn from_bytes(
        block: Option<FsBlockIndex>,
        data: &[u8],
        inode: InodeIndex,
        checksum_base: Checksum,
        ext4: &Ext4,
    ) -> Result<Self, Ext4Error> {
        let header = NodeHeader::from_bytes(data, inode)?;
        let node_size_in_bytes = header.node_size_in_bytes();
        if node_size_in_bytes > ext4.0.superblock.block_size() {
            return Err(CorruptKind::ExtentNodeSize(inode).into());
        }
        if data.len() < node_size_in_bytes {
            return Err(CorruptKind::ExtentNotEnoughData(inode).into());
        }

        let entries = ExtentNodeEntries::from_bytes(
            &data[..node_size_in_bytes],
            &header,
            inode,
        )?;

        if ext4.has_metadata_checksums() {
            let checksum_offset = header.checksum_offset();
            let checksum_end = checksum_offset
                .checked_add(4)
                .ok_or(CorruptKind::ExtentNodeSize(inode))?;
            if checksum_end > ext4.0.superblock.block_size() {
                return Err(CorruptKind::ExtentNodeSize(inode).into());
            }
            if data.len() < checksum_end {
                return Err(CorruptKind::ExtentNotEnoughData(inode).into());
            }
            let expected_checksum = read_u32le(data, checksum_offset);
            let mut checksum = checksum_base.clone();
            checksum.update(&data[..checksum_offset]);
            if checksum.finalize() != expected_checksum {
                return Err(CorruptKind::ExtentChecksum(inode).into());
            }
        }
        Ok(Self {
            block,
            header,
            entries,
        })
    }

    pub(crate) fn to_bytes(
        &self,
        checksum_base: Option<Checksum>,
    ) -> Result<Vec<u8>, Ext4Error> {
        let capacity = if checksum_base.is_some() {
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "checksum_offset is at most 786432 bytes"
            )]
            {
                self.header.checksum_offset() + 4
            }
        } else {
            self.header.node_size_in_bytes()
        };
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(&self.header.to_bytes());
        match &self.entries {
            ExtentNodeEntries::Leaf(extents) => {
                for extent in extents {
                    bytes.extend_from_slice(&extent.to_bytes()?);
                }
            }
            ExtentNodeEntries::Internal(internal_nodes) => {
                for internal_node in internal_nodes {
                    bytes.extend_from_slice(&internal_node.to_bytes()?);
                }
            }
        }
        if let Some(checksum_base) = checksum_base {
            let mut checksum = checksum_base.clone();
            checksum.update(&bytes);
            bytes.extend_from_slice(&checksum.finalize().to_le_bytes());
        }
        Ok(bytes)
    }

    pub(crate) fn push_extent(&mut self, extent: Extent) -> Result<(), ()> {
        match &mut self.entries {
            ExtentNodeEntries::Leaf(extents) => {
                if extents.len()
                    >= usize_from_u32(u32::from(self.header.max_entries))
                {
                    return Err(());
                }
                extents.push(extent);
                self.header.num_entries =
                    u16::try_from(extents.len()).map_err(|_| ())?;
                Ok(())
            }
            ExtentNodeEntries::Internal(_) => Err(()),
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn write(
        &self,
        ext4: &Ext4,
        checksum_base: Option<&Checksum>,
    ) -> Result<(), Ext4Error> {
        if let Some(block) = self.block {
            let bytes = self.to_bytes(checksum_base.cloned())?;
            ext4.write_to_block(block, 0, &bytes).await?;
        }
        Ok(())
    }
}

/// Iterator of an inode's extent tree.
pub(crate) struct ExtentTree {
    ext4: Ext4,
    inode: InodeIndex,
    node: ExtentNode,
    checksum_base: Checksum,
}

impl ExtentTree {
    pub(crate) fn initialize(
        inode: &Inode,
        ext4: Ext4,
    ) -> Result<Self, Ext4Error> {
        // TODO: linux claims some initial blocks for the extent tree
        Ok(Self {
            ext4,
            inode: inode.index,
            node: ExtentNode {
                block: None,
                header: NodeHeader {
                    num_entries: 0,
                    max_entries: 4,
                    depth: 0,
                    generation: 0,
                },
                entries: ExtentNodeEntries::Leaf(vec![]),
            },
            checksum_base: inode.checksum_base().clone(),
        })
    }

    pub(crate) fn to_bytes(&self) -> Result<[u8; 60], Ext4Error> {
        let bytes = self.node.to_bytes(None)?;
        let mut result = [0u8; 60];
        result[..bytes.len()].copy_from_slice(&bytes);
        Ok(result)
    }

    pub(crate) fn from_inode(
        inode: &Inode,
        ext4: Ext4,
    ) -> Result<Self, Ext4Error> {
        let header = NodeHeader::from_bytes(&inode.inline_data(), inode.index)?;
        let entries = ExtentNodeEntries::from_bytes(
            &inode.inline_data(),
            &header,
            inode.index,
        )?;
        if header.max_entries != 4 {
            return Err(CorruptKind::ExtentNodeSize(inode.index).into());
        }
        Ok(Self {
            ext4,
            inode: inode.index,
            node: ExtentNode {
                block: None,
                header,
                entries,
            },
            checksum_base: inode.checksum_base().clone(),
        })
    }

    fn root_max_entries(&self) -> u16 {
        4
    }

    fn block_max_entries(&self) -> Result<u16, Ext4Error> {
        let block_size = self.ext4.0.superblock.block_size().to_usize();
        let usable = block_size
            .checked_sub(ENTRY_SIZE_IN_BYTES)
            .ok_or(CorruptKind::ExtentNodeSize(self.inode))?;
        u16::try_from(usable / ENTRY_SIZE_IN_BYTES)
            .map_err(|_| CorruptKind::ExtentNodeSize(self.inode).into())
    }

    #[maybe_async::maybe_async]
    async fn read_extent_node(
        &self,
        block: FsBlockIndex,
    ) -> Result<ExtentNode, Ext4Error> {
        let data = self.ext4.read_block(block).await?;
        ExtentNode::from_bytes(
            Some(block),
            &data,
            self.inode,
            self.checksum_base.clone(),
            &self.ext4,
        )
    }

    #[maybe_async::maybe_async]
    async fn collect_extents(&self) -> Result<Vec<Extent>, Ext4Error> {
        let mut out = Vec::new();
        let mut stack = vec![self.node.clone()];

        while let Some(node) = stack.pop() {
            match node.entries {
                ExtentNodeEntries::Leaf(extents) => out.extend(extents),
                ExtentNodeEntries::Internal(internal_nodes) => {
                    let mut children = Vec::with_capacity(internal_nodes.len());
                    for internal_node in internal_nodes {
                        children.push(
                            self.read_extent_node(internal_node.block).await?,
                        );
                    }
                    while let Some(child) = children.pop() {
                        stack.push(child);
                    }
                }
            }
        }

        Ok(out)
    }

    #[maybe_async::maybe_async]
    async fn collect_metadata_blocks(
        &self,
    ) -> Result<Vec<FsBlockIndex>, Ext4Error> {
        let mut out = Vec::new();
        let mut stack = vec![self.node.clone()];

        while let Some(node) = stack.pop() {
            match node.entries {
                ExtentNodeEntries::Leaf(_) => {}
                ExtentNodeEntries::Internal(internal_nodes) => {
                    let mut children = Vec::with_capacity(internal_nodes.len());
                    for internal_node in internal_nodes {
                        out.push(internal_node.block);
                        children.push(
                            self.read_extent_node(internal_node.block).await?,
                        );
                    }
                    while let Some(child) = children.pop() {
                        stack.push(child);
                    }
                }
            }
        }

        Ok(out)
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn metadata_block_count(&self) -> Result<u32, Ext4Error> {
        u32::try_from(self.collect_metadata_blocks().await?.len())
            .map_err(|_| CorruptKind::ExtentNodeSize(self.inode).into())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn free_metadata_blocks(&self) -> Result<u32, Ext4Error> {
        let blocks = self.collect_metadata_blocks().await?;
        for block in &blocks {
            self.ext4.free_block(*block).await?;
        }
        u32::try_from(blocks.len())
            .map_err(|_| CorruptKind::ExtentNodeSize(self.inode).into())
    }

    fn required_metadata_blocks(
        &self,
        num_extents: usize,
    ) -> Result<usize, Ext4Error> {
        let root_max = usize::from(self.root_max_entries());
        if num_extents <= root_max {
            return Ok(0);
        }

        let block_max = usize::from(self.block_max_entries()?);
        let mut total = num_extents.div_ceil(block_max);
        let mut nodes_at_level = total;
        while nodes_at_level > root_max {
            nodes_at_level = nodes_at_level.div_ceil(block_max);
            total = total
                .checked_add(nodes_at_level)
                .ok_or(Ext4Error::NoSpace)?;
        }
        Ok(total)
    }

    fn normalize_extents(
        &self,
        extents: &mut Vec<Extent>,
    ) -> Result<bool, Ext4Error> {
        extents.sort_by_key(|extent| extent.block_within_file);

        let original = extents.clone();
        let mut normalized = Vec::with_capacity(extents.len());
        for extent in original.iter().copied() {
            if let Some(prev) = normalized.last_mut() {
                let prev_end = extent_end(prev, self.inode)?;
                if extent.block_within_file < prev_end {
                    return Err(CorruptKind::ExtentBlock(self.inode).into());
                }
                if Self::can_merge(prev, &extent) {
                    prev.num_blocks = prev
                        .num_blocks
                        .checked_add(extent.num_blocks)
                        .ok_or(CorruptKind::ExtentBlock(self.inode))?;
                    continue;
                }
            }
            normalized.push(extent);
        }

        let changed = normalized != original;
        *extents = normalized;
        Ok(changed)
    }

    fn build_tree_from_extents(
        &self,
        extents: Vec<Extent>,
        metadata_blocks: &[FsBlockIndex],
    ) -> Result<(ExtentNode, Vec<ExtentNode>), Ext4Error> {
        let root_max_entries = self.root_max_entries();
        let block_max_entries = self.block_max_entries()?;
        let root_max_entries_usize = usize::from(root_max_entries);
        let block_max_entries_usize = usize::from(block_max_entries);

        if extents.len() <= root_max_entries_usize {
            return Ok((
                ExtentNode {
                    block: None,
                    header: NodeHeader {
                        num_entries: checked_num_entries(
                            extents.len(),
                            self.inode,
                        )?,
                        max_entries: root_max_entries,
                        depth: 0,
                        generation: 0,
                    },
                    entries: ExtentNodeEntries::Leaf(extents),
                },
                Vec::new(),
            ));
        }

        let mut next_block_index = 0usize;
        let mut written_nodes = Vec::new();
        let mut level: Vec<(FileBlockIndex, FsBlockIndex, u16)> = Vec::new();

        for chunk in extents.chunks(block_max_entries_usize) {
            let block = *metadata_blocks
                .get(next_block_index)
                .ok_or(Ext4Error::NoSpace)?;
            next_block_index = next_block_index
                .checked_add(1)
                .ok_or(CorruptKind::ExtentNodeSize(self.inode))?;

            let chunk_vec = chunk.to_vec();
            let first_block = chunk_vec[0].block_within_file;
            let node = ExtentNode {
                block: Some(block),
                header: NodeHeader {
                    num_entries: checked_num_entries(
                        chunk_vec.len(),
                        self.inode,
                    )?,
                    max_entries: block_max_entries,
                    depth: 0,
                    generation: 0,
                },
                entries: ExtentNodeEntries::Leaf(chunk_vec),
            };
            written_nodes.push(node);
            level.push((first_block, block, 0));
        }

        loop {
            if level.len() <= root_max_entries_usize {
                let depth = level[0]
                    .2
                    .checked_add(1)
                    .ok_or(CorruptKind::ExtentDepth(self.inode))?;
                if depth > 5 {
                    return Err(CorruptKind::ExtentDepth(self.inode).into());
                }
                return Ok((
                    ExtentNode {
                        block: None,
                        header: NodeHeader {
                            num_entries: checked_num_entries(
                                level.len(),
                                self.inode,
                            )?,
                            max_entries: root_max_entries,
                            depth,
                            generation: 0,
                        },
                        entries: ExtentNodeEntries::Internal(
                            level
                                .iter()
                                .map(|(first_block, block, _depth)| {
                                    ExtentInternalNode {
                                        block_within_file: *first_block,
                                        block: *block,
                                    }
                                })
                                .collect(),
                        ),
                    },
                    written_nodes,
                ));
            }

            let child_depth = level[0].2;
            let node_depth = child_depth
                .checked_add(1)
                .ok_or(CorruptKind::ExtentDepth(self.inode))?;
            if node_depth > 5 {
                return Err(CorruptKind::ExtentDepth(self.inode).into());
            }

            let mut next_level = Vec::new();
            for chunk in level.chunks(block_max_entries_usize) {
                let block = *metadata_blocks
                    .get(next_block_index)
                    .ok_or(Ext4Error::NoSpace)?;
                next_block_index = next_block_index
                    .checked_add(1)
                    .ok_or(CorruptKind::ExtentNodeSize(self.inode))?;

                let first_block = chunk[0].0;
                let node = ExtentNode {
                    block: Some(block),
                    header: NodeHeader {
                        num_entries: checked_num_entries(
                            chunk.len(),
                            self.inode,
                        )?,
                        max_entries: block_max_entries,
                        depth: node_depth,
                        generation: 0,
                    },
                    entries: ExtentNodeEntries::Internal(
                        chunk
                            .iter()
                            .map(|(child_first_block, child_block, _depth)| {
                                ExtentInternalNode {
                                    block_within_file: *child_first_block,
                                    block: *child_block,
                                }
                            })
                            .collect(),
                    ),
                };
                written_nodes.push(node);
                next_level.push((first_block, block, node_depth));
            }
            level = next_level;
        }
    }

    #[maybe_async::maybe_async]
    async fn rebuild_from_extents(
        &mut self,
        mut extents: Vec<Extent>,
    ) -> Result<(), Ext4Error> {
        self.normalize_extents(&mut extents)?;

        let old_metadata_blocks = self.collect_metadata_blocks().await?;
        let required_metadata_blocks =
            self.required_metadata_blocks(extents.len())?;

        let mut metadata_blocks = old_metadata_blocks.clone();
        while metadata_blocks.len() < required_metadata_blocks {
            metadata_blocks.push(self.ext4.alloc_block(self.inode).await?);
        }

        let (root, written_nodes) = self.build_tree_from_extents(
            extents,
            &metadata_blocks[..required_metadata_blocks],
        )?;

        let checksum_base = self
            .ext4
            .has_metadata_checksums()
            .then(|| self.checksum_base.clone());
        for node in &written_nodes {
            node.write(&self.ext4, checksum_base.as_ref()).await?;
        }

        for block in old_metadata_blocks
            .iter()
            .skip(required_metadata_blocks)
            .copied()
        {
            self.ext4.free_block(block).await?;
        }

        self.node = root;
        Ok(())
    }

    /// Get the extent that contains the given block index, if any.
    #[maybe_async::maybe_async]
    pub(crate) async fn find_extent(
        &self,
        block_index: FileBlockIndex,
    ) -> Result<Option<Extent>, Ext4Error> {
        let mut node = self.node.clone();
        loop {
            match &node.entries {
                ExtentNodeEntries::Leaf(extents) => {
                    for extent in extents {
                        let extent_end = extent_end(extent, self.inode)?;
                        if block_index >= extent.block_within_file
                            && block_index < extent_end
                        {
                            return Ok(Some(*extent));
                        }
                    }
                    return Ok(None);
                }
                ExtentNodeEntries::Internal(internal_nodes) => {
                    let next_node_index =
                        match find_child_index(internal_nodes, block_index) {
                            Some(i) => i,
                            None => return Ok(None),
                        };
                    let next_node_block = internal_nodes[next_node_index].block;
                    let next_node_data =
                        self.ext4.read_block(next_node_block).await?;
                    node = ExtentNode::from_bytes(
                        Some(next_node_block),
                        &next_node_data,
                        self.inode,
                        self.checksum_base.clone(),
                        &self.ext4,
                    )?;
                }
            }
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn get_block(
        &self,
        block_index: FileBlockIndex,
    ) -> Result<FsBlockIndex, Ext4Error> {
        if let Some(extent) = self.find_extent(block_index).await? {
            let offset_within_extent = block_index
                .checked_sub(extent.block_within_file)
                .ok_or(CorruptKind::ExtentBlock(self.inode))?;
            extent
                .start_block
                .checked_add(FsBlockIndex::from(offset_within_extent))
                .ok_or(CorruptKind::ExtentBlock(self.inode).into())
        } else {
            Ok(0)
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn allocate_block(
        &mut self,
        block_index: FileBlockIndex,
        inode_index: InodeIndex,
    ) -> Result<(FsBlockIndex, u32), Ext4Error> {
        if let Some(extent) = self.find_extent(block_index).await? {
            let offset_within_extent = block_index
                .checked_sub(extent.block_within_file)
                .ok_or(CorruptKind::ExtentBlock(self.inode))?;
            return Ok((
                extent
                    .start_block
                    .checked_add(FsBlockIndex::from(offset_within_extent))
                    .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                0,
            ));
        }

        let metadata_before = self.metadata_block_count().await?;
        let new_block = self.ext4.alloc_block(inode_index).await?;

        let extent = Extent {
            block_within_file: block_index,
            num_blocks: 1,
            start_block: new_block,
            is_initialized: true,
        };

        if let Err(e) = self.insert_extent(extent).await {
            self.ext4.free_block(new_block).await?;
            return Err(e);
        }

        let metadata_after = self.metadata_block_count().await?;
        Ok((new_block, metadata_after.saturating_sub(metadata_before)))
    }

    /// Find the previous/next extents that border a block.
    ///
    /// Extents cover half-open ranges: `[start, start + num_blocks)`.
    ///
    /// Returns:
    /// - If `block_index` lies inside an extent, returns `(Some(extent), Some(extent))`.
    /// - Otherwise, `prev` is the last extent with `end <= block_index` and `next` is the first
    ///   extent with `start > block_index`.
    #[maybe_async::maybe_async]
    async fn find_prev_next(
        &self,
        block_index: FileBlockIndex,
    ) -> Result<(Option<Extent>, Option<Extent>), Ext4Error> {
        fn leaf_prev_next(
            extents: &[Extent],
            block_index: FileBlockIndex,
            inode: InodeIndex,
        ) -> Result<(Option<Extent>, Option<Extent>), Ext4Error> {
            let mut prev: Option<Extent> = None;
            let mut next: Option<Extent> = None;

            for extent in extents {
                let start = extent.block_within_file;
                let end = extent_end(extent, inode)?;

                if block_index >= start && block_index < end {
                    return Ok((Some(*extent), Some(*extent)));
                }

                if end <= block_index {
                    prev = Some(*extent);
                    continue;
                }

                if start > block_index {
                    next = Some(*extent);
                    break;
                }
            }

            Ok((prev, next))
        }

        #[maybe_async::maybe_async]
        async fn leftmost_leaf_first_extent(
            tree: &ExtentTree,
            mut node: ExtentNode,
        ) -> Result<Option<Extent>, Ext4Error> {
            loop {
                match &node.entries {
                    ExtentNodeEntries::Leaf(extents) => {
                        return Ok(extents.first().copied());
                    }
                    ExtentNodeEntries::Internal(internal_nodes) => {
                        if internal_nodes.is_empty() {
                            return Ok(None);
                        }
                        let next_node_block = internal_nodes[0].block;
                        let next_node_data =
                            tree.ext4.read_block(next_node_block).await?;
                        node = ExtentNode::from_bytes(
                            Some(next_node_block),
                            &next_node_data,
                            tree.inode,
                            tree.checksum_base.clone(),
                            &tree.ext4,
                        )?;
                    }
                }
            }
        }

        #[maybe_async::maybe_async]
        async fn rightmost_leaf_last_extent(
            tree: &ExtentTree,
            mut node: ExtentNode,
        ) -> Result<Option<Extent>, Ext4Error> {
            loop {
                match &node.entries {
                    ExtentNodeEntries::Leaf(extents) => {
                        return Ok(extents.last().copied());
                    }
                    ExtentNodeEntries::Internal(internal_nodes) => {
                        if internal_nodes.is_empty() {
                            return Ok(None);
                        }
                        let Some(next_node) = internal_nodes.last() else {
                            return Ok(None);
                        };
                        let next_node_block = next_node.block;
                        let next_node_data =
                            tree.ext4.read_block(next_node_block).await?;
                        node = ExtentNode::from_bytes(
                            Some(next_node_block),
                            &next_node_data,
                            tree.inode,
                            tree.checksum_base.clone(),
                            &tree.ext4,
                        )?;
                    }
                }
            }
        }

        // Descend to the leaf that would contain `block_index`, tracking the path of internal
        // nodes so we can find the adjacent leaf if the neighbor is not in this leaf.
        let mut node = self.node.clone();
        let mut internal_path: Vec<(ExtentNode, usize)> = Vec::new();
        loop {
            match &node.entries {
                ExtentNodeEntries::Leaf(extents) => {
                    if extents.is_empty() {
                        return Ok((None, None));
                    }

                    let (mut prev, mut next) =
                        leaf_prev_next(extents, block_index, self.inode)?;

                    // If we found the containing extent, we’re done.
                    if matches!((&prev, &next), (Some(p), Some(n)) if p == n) {
                        return Ok((prev, next));
                    }

                    // If a neighbor is missing, attempt to find it in an adjacent leaf by walking
                    // up the internal path and choosing a sibling subtree.
                    if prev.is_none() {
                        // Find previous leaf: go up until we can take a left sibling.
                        let mut i = internal_path.len();
                        while i > 0 {
                            let Some(prev_i) = i.checked_sub(1) else {
                                break;
                            };
                            i = prev_i;
                            let (parent, child_index) =
                                internal_path[i].clone();
                            if let ExtentNodeEntries::Internal(internal_nodes) =
                                &parent.entries
                            {
                                if let Some(left_sibling_index) =
                                    child_index.checked_sub(1)
                                {
                                    let sibling_block = internal_nodes
                                        [left_sibling_index]
                                        .block;
                                    let sibling_data = self
                                        .ext4
                                        .read_block(sibling_block)
                                        .await?;
                                    let sibling_node = ExtentNode::from_bytes(
                                        Some(sibling_block),
                                        &sibling_data,
                                        self.inode,
                                        self.checksum_base.clone(),
                                        &self.ext4,
                                    )?;
                                    prev = rightmost_leaf_last_extent(
                                        self,
                                        sibling_node,
                                    )
                                    .await?;
                                    break;
                                }
                            }
                        }
                    }

                    if next.is_none() {
                        // Find next leaf: go up until we can take a right sibling.
                        let mut i = internal_path.len();
                        while i > 0 {
                            let Some(prev_i) = i.checked_sub(1) else {
                                break;
                            };
                            i = prev_i;
                            let (parent, child_index) =
                                internal_path[i].clone();
                            if let ExtentNodeEntries::Internal(internal_nodes) =
                                &parent.entries
                            {
                                if let Some(right_sibling_index) =
                                    child_index.checked_add(1)
                                {
                                    if right_sibling_index
                                        < internal_nodes.len()
                                    {
                                        let sibling_block = internal_nodes
                                            [right_sibling_index]
                                            .block;
                                        let sibling_data = self
                                            .ext4
                                            .read_block(sibling_block)
                                            .await?;
                                        let sibling_node =
                                            ExtentNode::from_bytes(
                                                Some(sibling_block),
                                                &sibling_data,
                                                self.inode,
                                                self.checksum_base.clone(),
                                                &self.ext4,
                                            )?;
                                        next = leftmost_leaf_first_extent(
                                            self,
                                            sibling_node,
                                        )
                                        .await?;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    return Ok((prev, next));
                }
                ExtentNodeEntries::Internal(internal_nodes) => {
                    let next_node_index =
                        match find_child_index(internal_nodes, block_index) {
                            Some(i) => i,
                            None => {
                                // Per `find_extent`, if there is no internal node key <= block, we
                                // treat as “not found”. We can still provide `next` by taking the first
                                // extent in the leftmost subtree.
                                let next =
                                    leftmost_leaf_first_extent(self, node)
                                        .await?;
                                return Ok((None, next));
                            }
                        };

                    internal_path.push((node.clone(), next_node_index));

                    let next_node_block = internal_nodes[next_node_index].block;
                    let next_node_data =
                        self.ext4.read_block(next_node_block).await?;
                    node = ExtentNode::from_bytes(
                        Some(next_node_block),
                        &next_node_data,
                        self.inode,
                        self.checksum_base.clone(),
                        &self.ext4,
                    )?;
                }
            }
        }
    }

    /// Insert a new extent. The new extent must not overlap existing extents.
    #[maybe_async::maybe_async]
    pub(crate) async fn insert_extent(
        &mut self,
        new_extent: Extent,
    ) -> Result<(), Ext4Error> {
        let mut extents = self.collect_extents().await?;
        extents.push(new_extent);
        self.rebuild_from_extents(extents).await
    }

    /// Remove all extents that overlap file-block range [start, start+num_blocks)
    /// and return any freed [`FsBlockIndex`] ranges.
    #[maybe_async::maybe_async]
    pub(crate) async fn remove_extent_range(
        &mut self,
        start: FileBlockIndex,
        num_blocks: u32,
    ) -> Result<Vec<(FsBlockIndex, u32)>, Ext4Error> {
        if num_blocks == 0 {
            return Ok(Vec::new());
        }

        let end = start.checked_add(num_blocks).ok_or(Ext4Error::NoSpace)?;
        let mut extents = self.collect_extents().await?;
        let mut remaining = Vec::with_capacity(extents.len());
        let mut freed = Vec::new();
        let mut changed = false;

        for extent in extents.drain(..) {
            let extent_start = extent.block_within_file;
            let extent_end = extent_end(&extent, self.inode)?;

            if extent_end <= start || extent_start >= end {
                remaining.push(extent);
                continue;
            }

            changed = true;
            let remove_start = core::cmp::max(extent_start, start);
            let remove_end = core::cmp::min(extent_end, end);
            let remove_len = remove_end
                .checked_sub(remove_start)
                .ok_or(CorruptKind::ExtentBlock(self.inode))?;
            let remove_phys_start = extent
                .start_block
                .checked_add(FsBlockIndex::from(
                    remove_start
                        .checked_sub(extent_start)
                        .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                ))
                .ok_or(CorruptKind::ExtentBlock(self.inode))?;
            freed.push((remove_phys_start, remove_len));

            if extent_start < remove_start {
                let left_len = remove_start
                    .checked_sub(extent_start)
                    .ok_or(CorruptKind::ExtentBlock(self.inode))?;
                remaining.push(Extent {
                    block_within_file: extent_start,
                    start_block: extent.start_block,
                    num_blocks: u16::try_from(left_len)
                        .map_err(|_| CorruptKind::ExtentBlock(self.inode))?,
                    is_initialized: extent.is_initialized,
                });
            }

            if extent_end > remove_end {
                let right_len = extent_end
                    .checked_sub(remove_end)
                    .ok_or(CorruptKind::ExtentBlock(self.inode))?;
                remaining.push(Extent {
                    block_within_file: remove_end,
                    start_block: extent
                        .start_block
                        .checked_add(FsBlockIndex::from(
                            remove_end
                                .checked_sub(extent_start)
                                .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                        ))
                        .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                    num_blocks: u16::try_from(right_len)
                        .map_err(|_| CorruptKind::ExtentBlock(self.inode))?,
                    is_initialized: extent.is_initialized,
                });
            }
        }

        if changed {
            self.rebuild_from_extents(remaining).await?;
        }

        Ok(freed)
    }

    /// Split an existing extent so that there is a boundary at `split_block_within_file`.
    #[maybe_async::maybe_async]
    async fn split_extent_at(
        &mut self,
        split_block_within_file: FileBlockIndex,
    ) -> Result<(), Ext4Error> {
        let mut extents = self.collect_extents().await?;
        let mut rebuilt = Vec::with_capacity(
            extents
                .len()
                .checked_add(1)
                .ok_or(CorruptKind::ExtentNodeSize(self.inode))?,
        );
        let mut did_split = false;

        for extent in extents.drain(..) {
            let extent_start = extent.block_within_file;
            let extent_end = extent_end(&extent, self.inode)?;

            if split_block_within_file <= extent_start
                || split_block_within_file >= extent_end
            {
                rebuilt.push(extent);
                continue;
            }

            let left_len = split_block_within_file
                .checked_sub(extent_start)
                .ok_or(CorruptKind::ExtentBlock(self.inode))?;
            let right_len = extent_end
                .checked_sub(split_block_within_file)
                .ok_or(CorruptKind::ExtentBlock(self.inode))?;
            rebuilt.push(Extent {
                block_within_file: extent_start,
                start_block: extent.start_block,
                num_blocks: u16::try_from(left_len)
                    .map_err(|_| CorruptKind::ExtentBlock(self.inode))?,
                is_initialized: extent.is_initialized,
            });
            rebuilt.push(Extent {
                block_within_file: split_block_within_file,
                start_block: extent
                    .start_block
                    .checked_add(FsBlockIndex::from(left_len))
                    .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                num_blocks: u16::try_from(right_len)
                    .map_err(|_| CorruptKind::ExtentBlock(self.inode))?,
                is_initialized: extent.is_initialized,
            });
            did_split = true;
        }

        if !did_split {
            if self.find_extent(split_block_within_file).await?.is_some() {
                return Ok(());
            }
            return Err(CorruptKind::ExtentBlock(self.inode).into());
        }

        self.rebuild_from_extents(rebuilt).await
    }

    /// Mark a (contiguous) file-block range as initialized.
    #[maybe_async::maybe_async]
    async fn mark_initialized(
        &mut self,
        start: FileBlockIndex,
        num_blocks: u32,
    ) -> Result<(), Ext4Error> {
        if num_blocks == 0 {
            return Ok(());
        }

        let end = start.checked_add(num_blocks).ok_or(Ext4Error::NoSpace)?;
        let mut extents = self.collect_extents().await?;
        let mut rebuilt = Vec::with_capacity(
            extents
                .len()
                .checked_mul(3)
                .ok_or(CorruptKind::ExtentNodeSize(self.inode))?,
        );
        let mut changed = false;

        for extent in extents.drain(..) {
            let extent_start = extent.block_within_file;
            let extent_end = extent_end(&extent, self.inode)?;

            if extent_end <= start || extent_start >= end {
                rebuilt.push(extent);
                continue;
            }

            let overlap_start = core::cmp::max(extent_start, start);
            let overlap_end = core::cmp::min(extent_end, end);

            if extent_start < overlap_start {
                let left_len = overlap_start
                    .checked_sub(extent_start)
                    .ok_or(CorruptKind::ExtentBlock(self.inode))?;
                rebuilt.push(Extent {
                    block_within_file: extent_start,
                    start_block: extent.start_block,
                    num_blocks: u16::try_from(left_len)
                        .map_err(|_| CorruptKind::ExtentBlock(self.inode))?,
                    is_initialized: extent.is_initialized,
                });
            }

            let mid_len = overlap_end
                .checked_sub(overlap_start)
                .ok_or(CorruptKind::ExtentBlock(self.inode))?;
            rebuilt.push(Extent {
                block_within_file: overlap_start,
                start_block: extent
                    .start_block
                    .checked_add(FsBlockIndex::from(
                        overlap_start
                            .checked_sub(extent_start)
                            .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                    ))
                    .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                num_blocks: u16::try_from(mid_len)
                    .map_err(|_| CorruptKind::ExtentBlock(self.inode))?,
                is_initialized: true,
            });
            changed |= !extent.is_initialized;

            if extent_end > overlap_end {
                let right_len = extent_end
                    .checked_sub(overlap_end)
                    .ok_or(CorruptKind::ExtentBlock(self.inode))?;
                rebuilt.push(Extent {
                    block_within_file: overlap_end,
                    start_block: extent
                        .start_block
                        .checked_add(FsBlockIndex::from(
                            overlap_end
                                .checked_sub(extent_start)
                                .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                        ))
                        .ok_or(CorruptKind::ExtentBlock(self.inode))?,
                    num_blocks: u16::try_from(right_len)
                        .map_err(|_| CorruptKind::ExtentBlock(self.inode))?,
                    is_initialized: extent.is_initialized,
                });
            }
        }

        if changed {
            self.rebuild_from_extents(rebuilt).await?;
        }
        Ok(())
    }

    /// Try to merge adjacency-eligible extents and rebuild the tree if needed.
    #[maybe_async::maybe_async]
    pub(crate) async fn try_merge_adjacent(
        &mut self,
        _hint_block: FileBlockIndex,
    ) -> Result<(), Ext4Error> {
        let mut extents = self.collect_extents().await?;
        if self.normalize_extents(&mut extents)? {
            self.rebuild_from_extents(extents).await?;
        }
        Ok(())
    }

    fn can_merge(left: &Extent, right: &Extent) -> bool {
        let Some(left_end) = left
            .block_within_file
            .checked_add(FileBlockIndex::from(left.num_blocks))
        else {
            return false;
        };
        if left_end != right.block_within_file {
            return false;
        }

        let Some(left_phys_end) = left
            .start_block
            .checked_add(FsBlockIndex::from(left.num_blocks))
        else {
            return false;
        };
        if left_phys_end != right.start_block {
            return false;
        }

        if left.is_initialized != right.is_initialized {
            return false;
        }

        let Some(combined_len) =
            u32::from(left.num_blocks).checked_add(u32::from(right.num_blocks))
        else {
            return false;
        };
        if combined_len > 32768 {
            return false;
        }

        true
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use crate::block_index::FileBlockIndex;
    use crate::block_index::FsBlockIndex;
    use crate::file_blocks::extent_tree::ExtentTree;
    use crate::inode::Inode;
    use crate::test_util::{load_test_disk1_rw, load_test_disk1_rw_no_fsck};

    use super::{
        CorruptKind, ENTRY_SIZE_IN_BYTES, Ext4, Ext4Error, ExtentInternalNode,
        ExtentNode, ExtentNodeEntries, NodeHeader,
    };
    use crate::error::Corrupt;
    use maybe_async::maybe_async;
    use std::num::NonZeroU32;

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_extent_tree() {
        let fs = load_test_disk1_rw().await;
        let root_inode = fs.read_root_inode().await.unwrap();
        let mut tree =
            ExtentTree::from_inode(&root_inode, fs.0.clone()).unwrap();
        let extent = tree.find_extent(0).await.unwrap().unwrap();
        assert_eq!(extent.block_within_file, 0);
    }

    #[maybe_async::maybe_async]
    async fn root_inode_as_extent_tree(ext4: &Ext4) -> Inode {
        ext4.read_root_inode().await.unwrap()
    }

    /// Build a depth-1 extent tree (internal root -> 2 leaf nodes) written to disk.
    ///
    /// Layout:
    /// - leaf0: contains one extent covering file blocks [0, 2)
    /// - leaf1: contains one extent covering file blocks [10, 12)
    /// - root: internal node with two entries keyed at 0 and 10 pointing to the leaf blocks.
    #[maybe_async::maybe_async]
    async fn build_depth1_tree(
        ext4: &Ext4,
        inode: &Inode,
    ) -> (ExtentTree, FsBlockIndex, FsBlockIndex) {
        use crate::extent::Extent;

        let checksum_base = inode.checksum_base().clone();
        let checksum_base_opt =
            ext4.has_metadata_checksums().then(|| checksum_base.clone());

        // Allocate blocks for the 2 leaf nodes.
        let leaf0_block = ext4
            .alloc_contiguous_blocks(inode.index, NonZeroU32::new(1).unwrap())
            .await
            .unwrap();
        let leaf1_block = ext4
            .alloc_contiguous_blocks(inode.index, NonZeroU32::new(1).unwrap())
            .await
            .unwrap();

        // Construct leaf nodes.
        let leaf0 = ExtentNode {
            block: Some(leaf0_block),
            header: NodeHeader {
                num_entries: 1,
                max_entries: 4,
                depth: 0,
                generation: 0,
            },
            entries: ExtentNodeEntries::Leaf(vec![Extent {
                block_within_file: 0,
                start_block: 100,
                num_blocks: 2,
                is_initialized: true,
            }]),
        };
        let leaf1 = ExtentNode {
            block: Some(leaf1_block),
            header: NodeHeader {
                num_entries: 1,
                max_entries: 4,
                depth: 0,
                generation: 0,
            },
            entries: ExtentNodeEntries::Leaf(vec![Extent {
                block_within_file: 10,
                start_block: 200,
                num_blocks: 2,
                is_initialized: true,
            }]),
        };

        ext4.write_to_block(
            leaf0_block,
            0,
            &leaf0.to_bytes(checksum_base_opt.clone()).unwrap(),
        )
        .await
        .unwrap();
        ext4.write_to_block(
            leaf1_block,
            0,
            &leaf1.to_bytes(checksum_base_opt.clone()).unwrap(),
        )
        .await
        .unwrap();

        // Construct an internal root node that points to the two leaf blocks.
        let root = ExtentNode {
            block: None,
            header: NodeHeader {
                num_entries: 2,
                max_entries: 4,
                depth: 1,
                generation: 0,
            },
            entries: ExtentNodeEntries::Internal(vec![
                ExtentInternalNode {
                    block_within_file: 0,
                    block: leaf0_block,
                },
                ExtentInternalNode {
                    block_within_file: 10,
                    block: leaf1_block,
                },
            ]),
        };

        let tree = ExtentTree {
            ext4: ext4.clone(),
            inode: inode.index,
            node: root,
            checksum_base,
        };

        (tree, leaf0_block, leaf1_block)
    }

    /// Build a simple depth-2 extent tree written to disk.
    ///
    /// Layout:
    /// - leaf0: contains one extent covering file blocks [0, 2)
    /// - leaf1: contains one extent covering file blocks [10, 12)
    /// - internal: contains two entries keyed at 0 and 10 pointing to the leaf blocks.
    /// - root: internal node with one entry keyed at 0 pointing to the internal block.
    #[maybe_async::maybe_async]
    async fn build_depth2_tree(
        ext4: &Ext4,
        inode: &Inode,
    ) -> (ExtentTree, FsBlockIndex, FsBlockIndex) {
        use crate::extent::Extent;

        let checksum_base = inode.checksum_base().clone();
        let checksum_base_opt =
            ext4.has_metadata_checksums().then(|| checksum_base.clone());

        // Allocate blocks for the 2 leaf nodes and 1 internal node.
        let leaf0_block = ext4
            .alloc_contiguous_blocks(inode.index, NonZeroU32::new(1).unwrap())
            .await
            .unwrap();
        let leaf1_block = ext4
            .alloc_contiguous_blocks(inode.index, NonZeroU32::new(1).unwrap())
            .await
            .unwrap();
        let internal0_block = ext4
            .alloc_contiguous_blocks(inode.index, NonZeroU32::new(1).unwrap())
            .await
            .unwrap();

        // Construct leaf nodes.
        let leaf0 = ExtentNode {
            block: Some(leaf0_block),
            header: NodeHeader {
                num_entries: 1,
                max_entries: 4,
                depth: 0,
                generation: 0,
            },
            entries: ExtentNodeEntries::Leaf(vec![Extent {
                block_within_file: 0,
                start_block: 100,
                num_blocks: 2,
                is_initialized: true,
            }]),
        };
        let leaf1 = ExtentNode {
            block: Some(leaf1_block),
            header: NodeHeader {
                num_entries: 1,
                max_entries: 4,
                depth: 0,
                generation: 0,
            },
            entries: ExtentNodeEntries::Leaf(vec![Extent {
                block_within_file: 10,
                start_block: 200,
                num_blocks: 2,
                is_initialized: true,
            }]),
        };

        let internal0 = ExtentNode {
            block: Some(internal0_block),
            header: NodeHeader {
                num_entries: 2,
                max_entries: 4,
                depth: 1,
                generation: 1,
            },
            entries: ExtentNodeEntries::Internal(vec![
                ExtentInternalNode {
                    block_within_file: 0,
                    block: leaf0_block,
                },
                ExtentInternalNode {
                    block_within_file: 10,
                    block: leaf1_block,
                },
            ]),
        };

        ext4.write_to_block(
            leaf0_block,
            0,
            &leaf0.to_bytes(checksum_base_opt.clone()).unwrap(),
        )
        .await
        .unwrap();
        ext4.write_to_block(
            leaf1_block,
            0,
            &leaf1.to_bytes(checksum_base_opt.clone()).unwrap(),
        )
        .await
        .unwrap();
        ext4.write_to_block(
            internal0_block,
            0,
            &internal0.to_bytes(checksum_base_opt.clone()).unwrap(),
        )
        .await
        .unwrap();

        // Construct an internal root node that points to the two leaf blocks.
        let root = ExtentNode {
            block: None,
            header: NodeHeader {
                num_entries: 1,
                max_entries: 4,
                depth: 2,
                generation: 0,
            },
            entries: ExtentNodeEntries::Internal(vec![ExtentInternalNode {
                block_within_file: 0,
                block: internal0_block,
            }]),
        };

        let tree = ExtentTree {
            ext4: ext4.clone(),
            inode: inode.index,
            node: root,
            checksum_base,
        };

        (tree, leaf0_block, leaf1_block)
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_extent_tree_internal_nodes_find_extent_and_get_block() {
        let fs = load_test_disk1_rw().await;
        let ext4 = fs.0.clone();
        let inode = root_inode_as_extent_tree(&fs).await;

        for (tree, _leaf0, _leaf1) in [
            build_depth1_tree(&ext4, &inode).await,
            build_depth2_tree(&ext4, &inode).await,
        ] {
            // Within leaf0 extent.
            let e0 = tree.find_extent(0).await.unwrap().unwrap();
            assert_eq!(e0.block_within_file, 0);
            let block = tree.get_block(0).await.unwrap();
            assert_eq!(block, 100);
            let block = tree.get_block(1).await.unwrap();
            assert_eq!(block, 101);
            let extent = tree.find_extent(2).await;
            assert_eq!(extent.unwrap(), None);

            // Hole before leaf1.
            let extent = tree.find_extent(9).await.unwrap();
            assert_eq!(extent, None);

            // Within leaf1 extent.
            let e1 = tree.find_extent(10).await.unwrap().unwrap();
            assert_eq!(e1.block_within_file, 10);
            let block = tree.get_block(10).await.unwrap();
            assert_eq!(block, 200);
            let block = tree.get_block(11).await.unwrap();
            assert_eq!(block, 201);
            let extent = tree.find_extent(12).await.unwrap();
            assert_eq!(extent, None);
        }
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_extent_tree_internal_nodes_selection_boundary_conditions() {
        let fs = load_test_disk1_rw().await;
        let ext4 = fs.0.clone();
        let inode = root_inode_as_extent_tree(&fs).await;

        let (tree, _leaf0, _leaf1) = build_depth1_tree(&ext4, &inode).await;

        // Querying before the first internal key should behave like `find_extent`: returns None.
        let extent = tree.find_extent(u32::MAX).await.unwrap();
        assert_eq!(extent, None);
        // block_index < 0 is not possible; instead validate that blocks smaller than first key 0
        // are handled via the 0 key. (0 selects child 0)
        let extent = tree.find_extent(0).await.unwrap();
        assert!(extent.is_some());

        // Exactly at the second internal key should descend into leaf1.
        let e = tree.find_extent(10).await.unwrap().unwrap();
        assert_eq!(e.block_within_file, 10);
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_extent_tree_internal_nodes_checksum_mismatch_is_detected() {
        let fs = load_test_disk1_rw_no_fsck().await;
        let ext4 = fs.clone();
        if !ext4.has_metadata_checksums() {
            // If the test image doesn't have metadata checksums enabled, there's nothing to test.
            return;
        }

        let inode = root_inode_as_extent_tree(&fs).await;
        let (tree, leaf0_block, _leaf1_block) =
            build_depth1_tree(&ext4, &inode).await;

        // Corrupt one byte in leaf0 so its checksum no longer matches.
        let mut bytes = ext4.read_block(leaf0_block).await.unwrap();
        // Flip a byte in the extent payload (not the checksum itself) so we avoid accidental fixups.
        bytes[ENTRY_SIZE_IN_BYTES] ^= 0x01;
        ext4.write_to_block(leaf0_block, 0, &bytes).await.unwrap();

        // Accessing an extent that forces reading leaf0 should return an error.
        let err = tree.find_extent(0).await.unwrap_err();
        if err != CorruptKind::ExtentChecksum(tree.inode) {
            panic!("unexpected error: {err:?}");
        }
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_insert_extent_grows_into_internal_tree() {
        use crate::extent::Extent;

        let fs = load_test_disk1_rw().await;
        let ext4 = fs.0.clone();
        let inode = root_inode_as_extent_tree(&fs).await;
        let mut tree = ExtentTree::initialize(&inode, ext4).unwrap();

        for i in 0..5u32 {
            tree.insert_extent(Extent {
                block_within_file: i * 10,
                start_block: 100 + u64::from(i) * 10,
                num_blocks: 1,
                is_initialized: true,
            })
            .await
            .unwrap();
        }

        assert_eq!(tree.node.header.depth, 1);
        let metadata_blocks = tree.metadata_block_count().await.unwrap();
        assert_eq!(metadata_blocks, 1);
        for i in 0..5u32 {
            let extent = tree.find_extent(i * 10).await.unwrap().unwrap();
            assert_eq!(extent.start_block, 100 + u64::from(i) * 10);
        }
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_remove_extent_range_rebuilds_internal_tree() {
        use crate::extent::Extent;

        let fs = load_test_disk1_rw().await;
        let ext4 = fs.0.clone();
        let inode = root_inode_as_extent_tree(&fs).await;
        let mut tree = ExtentTree::initialize(&inode, ext4).unwrap();

        for i in 0..5u32 {
            tree.insert_extent(Extent {
                block_within_file: i * 10,
                start_block: 100 + u64::from(i) * 10,
                num_blocks: 1,
                is_initialized: true,
            })
            .await
            .unwrap();
        }

        let freed = tree.remove_extent_range(20, 1).await.unwrap();
        assert_eq!(freed, vec![(120, 1)]);
        assert_eq!(tree.node.header.depth, 0);
        let metadata_blocks = tree.metadata_block_count().await.unwrap();
        assert_eq!(metadata_blocks, 0);
        let removed = tree.find_extent(20).await.unwrap();
        assert_eq!(removed, None);
        let block0 = tree.get_block(0).await.unwrap();
        let block10 = tree.get_block(10).await.unwrap();
        let block30 = tree.get_block(30).await.unwrap();
        let block40 = tree.get_block(40).await.unwrap();
        assert_eq!(block0, 100);
        assert_eq!(block10, 110);
        assert_eq!(block30, 130);
        assert_eq!(block40, 140);
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_merge_adjacent() {
        use crate::extent::Extent;

        let fs = load_test_disk1_rw().await;
        let ext4 = fs.0.clone();
        let inode = root_inode_as_extent_tree(&fs).await;

        let mut tree = ExtentTree::from_inode(&inode, ext4.clone()).unwrap();

        // Start with an empty leaf (root)
        tree.node.entries = ExtentNodeEntries::Leaf(vec![
            Extent {
                block_within_file: 0,
                start_block: 100,
                num_blocks: 5,
                is_initialized: true,
            },
            Extent {
                block_within_file: 5,
                start_block: 105,
                num_blocks: 5,
                is_initialized: true,
            },
        ]);
        tree.node.header.num_entries = 2;

        // Merge logical [0,5) and [5,10) -> [0,10)
        tree.try_merge_adjacent(0).await.unwrap();

        let ExtentNodeEntries::Leaf(extents) = &tree.node.entries else {
            panic!("expected leaf");
        };
        assert_eq!(extents.len(), 1);
        assert_eq!(extents[0].block_within_file, 0);
        assert_eq!(extents[0].num_blocks, 10);
        assert_eq!(extents[0].start_block, 100);

        // Try merge unmergeable (physically non-contiguous)
        tree.node.entries = ExtentNodeEntries::Leaf(vec![
            Extent {
                block_within_file: 0,
                start_block: 100,
                num_blocks: 5,
                is_initialized: true,
            },
            Extent {
                block_within_file: 5,
                start_block: 200, // Gap
                num_blocks: 5,
                is_initialized: true,
            },
        ]);
        tree.node.header.num_entries = 2;

        tree.try_merge_adjacent(0).await.unwrap();
        let ExtentNodeEntries::Leaf(extents) = &tree.node.entries else {
            panic!("expected leaf");
        };
        assert_eq!(extents.len(), 2);
    }
}
