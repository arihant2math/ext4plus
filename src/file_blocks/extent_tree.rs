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
    // OK to unwrap: the maximum value of `n` is `2^16-1`, so the
    // maximum value of this sum is `2^16`. That fits in a `u32`, and we
    // assume `usize` is at least as big as a `u32`.
    let n_plus_one = usize::from(n).checked_add(1).unwrap();
    // OK to unwrap: `n_plus_one` is at most `2^16` and
    // `ENTRY_SIZE_IN_BYTES` is 12, so the maximum product is 786432,
    // which fits in a `u32`. We assume `usize` is at least as big as a
    // `u32`.
    n_plus_one.checked_mul(ENTRY_SIZE_IN_BYTES).unwrap()
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

    pub(crate) fn to_bytes(self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..4].copy_from_slice(&self.block_within_file.to_le_bytes());
        let (ei_start_hi, ei_start_lo) = u64_to_hilo(self.block);
        let ei_start_hi =
            u16::try_from(ei_start_hi).expect("block must fit in 48 bits");
        bytes[4..8].copy_from_slice(&ei_start_lo.to_le_bytes());
        bytes[8..10].copy_from_slice(&ei_start_hi.to_le_bytes());
        // The last two bytes are unused.
        bytes
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
                let entry = Extent::from_bytes(
                    &data[offset..offset + ENTRY_SIZE_IN_BYTES],
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
                let entry = ExtentInternalNode::from_bytes(
                    &data[offset..offset + ENTRY_SIZE_IN_BYTES],
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
        let entries = ExtentNodeEntries::from_bytes(data, &header, inode)?;
        if ext4.has_metadata_checksums() {
            let checksum_offset = header.checksum_offset();
            if data.len() < checksum_offset + 4 {
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

    pub(crate) fn to_bytes(&self, checksum_base: Option<Checksum>) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.header.checksum_offset() + 4);
        bytes.extend_from_slice(&self.header.to_bytes());
        match &self.entries {
            ExtentNodeEntries::Leaf(extents) => {
                for extent in extents {
                    bytes.extend_from_slice(&extent.to_bytes());
                }
            }
            ExtentNodeEntries::Internal(internal_nodes) => {
                for internal_node in internal_nodes {
                    bytes.extend_from_slice(&internal_node.to_bytes());
                }
            }
        }
        if let Some(checksum_base) = checksum_base {
            let mut checksum = checksum_base.clone();
            checksum.update(&bytes);
            bytes.extend_from_slice(&checksum.finalize().to_le_bytes());
        }
        bytes
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
                self.header.num_entries += 1;
                Ok(())
            }
            ExtentNodeEntries::Internal(_) => Err(()),
        }
    }

    pub(crate) async fn write(&self, ext4: &Ext4) -> Result<(), Ext4Error> {
        if let Some(block) = self.block {
            let bytes = self.to_bytes(None);
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
        ext4: Ext4,
        inode: &Inode,
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

    pub(crate) fn to_bytes(&self) -> [u8; 60] {
        let bytes = self.node.to_bytes(None);
        let mut result = [0u8; 60];
        result[..bytes.len()].copy_from_slice(&bytes);
        result
    }

    pub(crate) fn from_inode(
        ext4: Ext4,
        inode: &Inode,
    ) -> Result<Self, Ext4Error> {
        let header = NodeHeader::from_bytes(&inode.inline_data(), inode.index)?;
        let entries = ExtentNodeEntries::from_bytes(
            &inode.inline_data(),
            &header,
            inode.index,
        )?;
        assert_eq!(header.max_entries, 4);
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

    /// Get the extent that contains the given block index, if any.
    pub(crate) async fn find_extent(
        &self,
        block_index: FileBlockIndex,
    ) -> Result<Option<Extent>, Ext4Error> {
        let mut node = self.node.clone();
        loop {
            match &node.entries {
                ExtentNodeEntries::Leaf(extents) => {
                    for extent in extents {
                        if block_index >= extent.block_within_file
                            && block_index
                                < extent.block_within_file
                                    + FileBlockIndex::from(extent.num_blocks)
                        {
                            return Ok(Some(*extent));
                        }
                    }
                    return Ok(None);
                }
                ExtentNodeEntries::Internal(internal_nodes) => {
                    // Internal nodes are sorted by `block_within_file`.
                    // Find the last internal node whose `block_within_file` is less than or equal to `block_index`.
                    let mut next_node_index = None;
                    for (i, internal_node) in internal_nodes.iter().enumerate()
                    {
                        if internal_node.block_within_file > block_index {
                            break;
                        }
                        next_node_index = Some(i);
                    }
                    let next_node_index = match next_node_index {
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

    pub(crate) async fn get_block(
        &self,
        block_index: FileBlockIndex,
    ) -> Result<Option<FsBlockIndex>, Ext4Error> {
        if let Some(extent) = self.find_extent(block_index).await? {
            let offset_within_extent = block_index - extent.block_within_file;
            Ok(Some(
                extent.start_block + FsBlockIndex::from(offset_within_extent),
            ))
        } else {
            Ok(None)
        }
    }

    async fn last_allocated_extent(
        &self,
    ) -> Result<Option<(Vec<ExtentNode>, Extent)>, Ext4Error> {
        let mut node = self.node.clone();
        let mut path = Vec::new();
        loop {
            path.push(node.clone());
            match &node.entries {
                ExtentNodeEntries::Leaf(extents) => {
                    // TODO: avoid unwrapping
                    if extents.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some((path, extents.last().copied().unwrap())));
                }
                ExtentNodeEntries::Internal(internal_nodes) => {
                    if internal_nodes.is_empty() {
                        return Ok(None);
                    }
                    let next_node_block = internal_nodes.last().unwrap().block;
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

    pub(crate) async fn allocate(
        &mut self,
        start: FileBlockIndex,
        amount: NonZeroU32,
        initialized: bool,
    ) -> Result<(), Ext4Error> {
        // Find the rightmost leaf node.
        // If there's space, allocate the new extent there.
        // Otherwise, panic for now
        let last_allocated = self.last_allocated_extent().await?;
        if let Some((path, last_extent)) = last_allocated {
            if last_extent.block_within_file + u32::from(last_extent.num_blocks)
                >= start
            {
                panic!("can't allocate overlapping extent");
            }
            if !last_extent.is_initialized && initialized {
                panic!(
                    "can't allocate initialized extent after uninitialized extent"
                );
            }
            if let ExtentNodeEntries::Leaf(extents) =
                &path.last().unwrap().entries
            {
                if extents.len()
                    < usize_from_u32(u32::from(self.node.header.max_entries))
                {
                    let start_block = self
                        .ext4
                        .alloc_contiguous_blocks(self.inode, amount)
                        .await?;
                    if initialized {
                        self.ext4.clear_blocks(start_block, amount).await?;
                    }
                    self.node
                        .push_extent(Extent {
                            block_within_file: start,
                            start_block,
                            num_blocks: u16::try_from(amount.get()).unwrap(),
                            is_initialized: initialized,
                        })
                        .unwrap();
                    return Ok(());
                }
            } else {
                unreachable!()
            }
            todo!()
        } else {
            let start_block = self
                .ext4
                .alloc_contiguous_blocks(self.inode, amount)
                .await?;
            if initialized {
                self.ext4.clear_blocks(start_block, amount).await?;
            }
            self.node
                .push_extent(Extent {
                    block_within_file: start,
                    start_block,
                    num_blocks: u16::try_from(amount.get()).unwrap(),
                    is_initialized: initialized,
                })
                .unwrap();
        }
        Ok(())
    }

    pub(crate) async fn extend(
        &mut self,
        start: FileBlockIndex,
        amount: NonZeroU32,
    ) -> Result<(), Ext4Error> {
        if let Some(extent) = self.find_extent(start).await? {
            todo!()
        }
        self.allocate(start, amount, true).await
    }

    /// Find the previous/next extents that border a block.
    ///
    /// Extents cover half-open ranges: `[start, start + num_blocks)`.
    ///
    /// Returns:
    /// - If `block_index` lies inside an extent, returns `(Some(extent), Some(extent))`.
    /// - Otherwise, `prev` is the last extent with `end <= block_index` and `next` is the first
    ///   extent with `start > block_index`.
    async fn find_prev_next(
        &self,
        block_index: FileBlockIndex,
    ) -> Result<(Option<Extent>, Option<Extent>), Ext4Error> {
        /// Pick the child index to descend into for `block_index`.
        ///
        /// Mirrors the selection logic in `find_extent`: chooses the last key `<= block_index`.
        fn child_index_for(
            internal_nodes: &[ExtentInternalNode],
            block_index: FileBlockIndex,
        ) -> Option<usize> {
            let mut next_node_index = None;
            for (i, internal_node) in internal_nodes.iter().enumerate() {
                if internal_node.block_within_file > block_index {
                    break;
                }
                next_node_index = Some(i);
            }
            next_node_index
        }

        fn leaf_prev_next(
            extents: &[Extent],
            block_index: FileBlockIndex,
        ) -> (Option<Extent>, Option<Extent>) {
            let mut prev: Option<Extent> = None;
            let mut next: Option<Extent> = None;

            for extent in extents {
                let start = extent.block_within_file;
                let end = start + FileBlockIndex::from(extent.num_blocks);

                if block_index >= start && block_index < end {
                    return (Some(*extent), Some(*extent));
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

            (prev, next)
        }

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
                        let next_node_block =
                            internal_nodes.last().unwrap().block;
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
                        leaf_prev_next(extents, block_index);

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
                            i -= 1;
                            let (parent, child_index) =
                                internal_path[i].clone();
                            if let ExtentNodeEntries::Internal(internal_nodes) =
                                &parent.entries
                            {
                                if child_index > 0 {
                                    let sibling_block =
                                        internal_nodes[child_index - 1].block;
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
                            i -= 1;
                            let (parent, child_index) =
                                internal_path[i].clone();
                            if let ExtentNodeEntries::Internal(internal_nodes) =
                                &parent.entries
                            {
                                if child_index + 1 < internal_nodes.len() {
                                    let sibling_block =
                                        internal_nodes[child_index + 1].block;
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

                    return Ok((prev, next));
                }
                ExtentNodeEntries::Internal(internal_nodes) => {
                    let next_node_index =
                        match child_index_for(internal_nodes, block_index) {
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
    /// This will error if the new extent overlaps existing extents.
    pub(crate) async fn insert_extent(
        &mut self,
        new_extent: Extent,
    ) -> Result<(), Ext4Error> {
        // Only handle the simplest case for now: the root node is an inline leaf.
        if self.node.header.depth != 0 {
            todo!()
        }

        let ExtentNodeEntries::Leaf(extents) = &mut self.node.entries else {
            unreachable!();
        };

        // Empty tree: just insert.
        if extents.is_empty() {
            if 1 > usize_from_u32(u32::from(self.node.header.max_entries)) {
                return Err(Ext4Error::NoSpace);
            }
            extents.push(new_extent);
            self.node.header.num_entries = 1;
            return Ok(());
        }

        let new_start = new_extent.block_within_file;
        let new_end = new_start + FileBlockIndex::from(new_extent.num_blocks);

        // Find insertion index (keep sorted by file-block start).
        let mut insert_at = extents.len();
        for (i, e) in extents.iter().enumerate() {
            if e.block_within_file > new_start {
                insert_at = i;
                break;
            }
        }

        // Check overlap with previous extent.
        if insert_at > 0 {
            let prev = extents[insert_at - 1];
            let prev_start = prev.block_within_file;
            let prev_end = prev_start + FileBlockIndex::from(prev.num_blocks);
            if new_start < prev_end {
                return Err(CorruptKind::ExtentBlock(self.inode).into());
            }
        }

        // Check overlap with next extent.
        if insert_at < extents.len() {
            let next = extents[insert_at];
            let next_start = next.block_within_file;
            if new_end > next_start {
                return Err(CorruptKind::ExtentBlock(self.inode).into());
            }
        }

        // If we can't merge with a neighbor, we need an extra slot.
        let can_merge_left = if insert_at > 0 {
            let left = extents[insert_at - 1];
            let left_end =
                left.block_within_file + FileBlockIndex::from(left.num_blocks);
            let left_phys_end = left.start_block
                + FsBlockIndex::from(u32::from(left.num_blocks));
            left_end == new_start
                && left_phys_end == new_extent.start_block
                && left.is_initialized == new_extent.is_initialized
        } else {
            false
        };

        let can_merge_right = if insert_at < extents.len() {
            let right = extents[insert_at];
            let phys_end = new_extent.start_block
                + FsBlockIndex::from(u32::from(new_extent.num_blocks));
            new_end == right.block_within_file
                && phys_end == right.start_block
                && right.is_initialized == new_extent.is_initialized
        } else {
            false
        };

        let max_entries =
            usize_from_u32(u32::from(self.node.header.max_entries));
        let needs_new_slot = !can_merge_left && !can_merge_right;
        if needs_new_slot && extents.len() >= max_entries {
            return Err(Ext4Error::NoSpace);
        }

        // Apply: merge where possible, otherwise insert.
        if can_merge_left {
            // Extend left to include new_extent.
            // Copy out the right neighbor (if any) before we take a mutable borrow of `extents`.
            let right_for_possible_merge = if can_merge_right {
                Some(extents[insert_at])
            } else {
                None
            };

            let left = &mut extents[insert_at - 1];
            let new_len = (u32::from(left.num_blocks))
                .checked_add(u32::from(new_extent.num_blocks))
                .ok_or(Ext4Error::NoSpace)?;
            left.num_blocks = u16::try_from(new_len)
                .map_err(|_| CorruptKind::ExtentBlock(self.inode))?;

            // Maybe also merge with right neighbor now.
            if let Some(right) = right_for_possible_merge {
                // After merging into left, right is still at index insert_at.
                let left_phys_end = left.start_block
                    + FsBlockIndex::from(u32::from(left.num_blocks));
                let left_end = left.block_within_file
                    + FileBlockIndex::from(left.num_blocks);
                if left_end == right.block_within_file
                    && left_phys_end == right.start_block
                    && left.is_initialized == right.is_initialized
                {
                    let combined = (u32::from(left.num_blocks))
                        .checked_add(u32::from(right.num_blocks))
                        .ok_or(Ext4Error::NoSpace)?;
                    left.num_blocks = u16::try_from(combined)
                        .map_err(|_| CorruptKind::ExtentBlock(self.inode))?;
                    extents.remove(insert_at);
                }
            }
        } else if can_merge_right {
            // Merge into right by extending it to the left.
            let right = &mut extents[insert_at];
            right.block_within_file = new_start;
            right.start_block = new_extent.start_block;
            let new_len = (u32::from(right.num_blocks))
                .checked_add(u32::from(new_extent.num_blocks))
                .ok_or(Ext4Error::NoSpace)?;
            right.num_blocks = u16::try_from(new_len)
                .map_err(|_| CorruptKind::ExtentBlock(self.inode))?;
        } else {
            extents.insert(insert_at, new_extent);
        }

        self.node.header.num_entries = u16::try_from(extents.len()).unwrap();
        Ok(())
    }

    /// Remove all extents that overlap file-block range [start, start+num_blocks)
    /// and return any freed FsBlockIndex ranges (so caller can free blocks).
    async fn remove_extent_range(
        &mut self,
        start: FileBlockIndex,
        num_blocks: u32,
    ) -> Result<Vec<(FsBlockIndex, u32)>, Ext4Error> {
        todo!()
    }

    /// Split an existing extent so that there's a boundary at `split_block_within_file`.
    /// If split_block equals extent.block_within_file or end, it's a no-op.
    /// Returns Err if extent not found.
    async fn split_extent_at(
        &mut self,
        split_block_within_file: FileBlockIndex,
    ) -> Result<(), Ext4Error> {
        /// Choose the child index to descend into for `block_index`.
        /// Mirrors `find_extent` selection: last key `<= block_index`.
        fn child_index_for(
            internal_nodes: &[ExtentInternalNode],
            block_index: FileBlockIndex,
        ) -> Option<usize> {
            let mut next_node_index = None;
            for (i, internal_node) in internal_nodes.iter().enumerate() {
                if internal_node.block_within_file > block_index {
                    break;
                }
                next_node_index = Some(i);
            }
            next_node_index
        }

        /// Split an extent within a leaf vector.
        /// Returns (did_split, leaf_first_extent_may_have_changed).
        fn split_in_leaf(
            inode: InodeIndex,
            max_entries: u16,
            extents: &mut Vec<Extent>,
            split_block_within_file: FileBlockIndex,
        ) -> Result<(bool, bool), Ext4Error> {
            // Find the extent containing the split point.
            let mut extent_index: Option<usize> = None;
            for (i, extent) in extents.iter().enumerate() {
                let start = extent.block_within_file;
                let end = start + FileBlockIndex::from(extent.num_blocks);
                if split_block_within_file >= start
                    && split_block_within_file < end
                {
                    extent_index = Some(i);
                    break;
                }
            }
            let Some(i) = extent_index else {
                return Err(CorruptKind::ExtentBlock(inode).into());
            };

            let old = extents[i];
            let start = old.block_within_file;
            let end = start + FileBlockIndex::from(old.num_blocks);

            // No-op at boundaries.
            if split_block_within_file == start
                || split_block_within_file == end
            {
                return Ok((false, false));
            }

            if extents.len() >= usize_from_u32(u32::from(max_entries)) {
                return Err(Ext4Error::NoSpace);
            }

            let left_len_u32 = split_block_within_file - start;
            let right_len_u32 = end - split_block_within_file;
            let left_len: u16 = u16::try_from(left_len_u32)
                .map_err(|_| CorruptKind::ExtentBlock(inode))?;
            let right_len: u16 = u16::try_from(right_len_u32)
                .map_err(|_| CorruptKind::ExtentBlock(inode))?;

            let left = Extent {
                block_within_file: old.block_within_file,
                start_block: old.start_block,
                num_blocks: left_len,
                is_initialized: old.is_initialized,
            };
            let right = Extent {
                block_within_file: split_block_within_file,
                start_block: old.start_block + FsBlockIndex::from(left_len_u32),
                num_blocks: right_len,
                is_initialized: old.is_initialized,
            };

            extents[i] = left;
            extents.insert(i.checked_add(1).unwrap(), right);

            Ok((true, i == 0))
        }

        // Root inline leaf: update in-place.
        if self.node.header.depth == 0 {
            let ExtentNodeEntries::Leaf(extents) = &mut self.node.entries
            else {
                unreachable!();
            };
            let (did_split, _first_changed) = split_in_leaf(
                self.inode,
                self.node.header.max_entries,
                extents,
                split_block_within_file,
            )?;
            if did_split {
                self.node.header.num_entries =
                    u16::try_from(extents.len()).unwrap();
            }
            return Ok(());
        }

        // Non-root: descend to leaf, keeping a path to allow parent key updates.
        // Each path entry is (node, chosen_child_index_within_node).
        let mut node = self.node.clone();
        let mut path: Vec<(ExtentNode, usize)> = Vec::new();

        loop {
            match &node.entries {
                ExtentNodeEntries::Leaf(_) => break,
                ExtentNodeEntries::Internal(internal_nodes) => {
                    let next_node_index = match child_index_for(
                        internal_nodes,
                        split_block_within_file,
                    ) {
                        Some(i) => i,
                        None => {
                            return Err(
                                CorruptKind::ExtentBlock(self.inode).into()
                            );
                        }
                    };
                    path.push((node.clone(), next_node_index));
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

        // `node` is now the leaf node that should contain the extent.
        let leaf_block =
            node.block.ok_or(CorruptKind::ExtentBlock(self.inode))?;

        let old_first = match &node.entries {
            ExtentNodeEntries::Leaf(extents) => extents.first().copied(),
            _ => None,
        };

        // Split while holding a mutable borrow, but extract any info we need before writing.
        let (did_split, first_changed, new_first) = {
            let ExtentNodeEntries::Leaf(ref mut leaf_extents) = node.entries
            else {
                unreachable!();
            };

            let (did_split, first_changed) = split_in_leaf(
                self.inode,
                node.header.max_entries,
                leaf_extents,
                split_block_within_file,
            )?;

            if !did_split {
                (false, false, None)
            } else {
                node.header.num_entries =
                    u16::try_from(leaf_extents.len()).unwrap();
                (true, first_changed, leaf_extents.first().copied())
            }
        };

        if !did_split {
            return Ok(());
        }

        node.write(&self.ext4).await?;

        // If the first extent in the leaf changed, we may need to update the key in the parent.
        // In ext4, internal node keys are the first file-block in the child subtree.
        if first_changed {
            match (old_first, new_first) {
                (Some(old_f), Some(new_f))
                    if old_f.block_within_file != new_f.block_within_file =>
                {
                    // Update the parent's entry for this leaf.
                    if let Some((mut parent, child_index)) = path.pop() {
                        if let ExtentNodeEntries::Internal(
                            ref mut internal_nodes,
                        ) = parent.entries
                        {
                            internal_nodes[child_index].block_within_file =
                                new_f.block_within_file;
                        }
                        parent.write(&self.ext4).await?;

                        // If we updated index 0, that may affect the parent's first key, so bubble up.
                        let mut changed_block_within_file = if child_index == 0
                        {
                            Some(new_f.block_within_file)
                        } else {
                            None
                        };

                        // Bubble key changes to ancestors if we changed the first entry.
                        while let (
                            Some(new_key),
                            Some((mut ancestor, ancestor_child_index)),
                        ) = (changed_block_within_file, path.pop())
                        {
                            if let ExtentNodeEntries::Internal(
                                ref mut internal_nodes,
                            ) = ancestor.entries
                            {
                                internal_nodes[ancestor_child_index]
                                    .block_within_file = new_key;
                            }
                            ancestor.write(&self.ext4).await?;

                            changed_block_within_file =
                                if ancestor_child_index == 0 {
                                    Some(new_key)
                                } else {
                                    None
                                };
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Mark a (contiguous) file-block range as initialized. Internally this may
    /// split extents and flip `is_initialized` flags for affected extents/blocks.
    async fn mark_initialized(
        &mut self,
        start: FileBlockIndex,
        num_blocks: u32,
    ) -> Result<(), Ext4Error> {
        todo!()
    }

    /// Try to merge adjacency-eligible extents (same start_block+num or both initialized/uninitialized adjacent)
    /// starting at `hint_block` to reduce fragmentation.
    pub(crate) async fn try_merge_adjacent(
        &mut self,
        hint_block: FileBlockIndex,
    ) -> Result<(), Ext4Error> {
        // TODO: implement
        Ok(())
    }
}
