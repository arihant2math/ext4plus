// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

mod block_map;
mod extents_blocks;

use alloc::vec::Vec;

use crate::block_index::FsBlockIndex;
use crate::inode::{Inode, InodeFlags};
#[cfg(not(feature = "sync"))]
use crate::iters::AsyncIterator;
use crate::{Ext4, Ext4Error};
use block_map::BlockMap;
use extents_blocks::ExtentsBlocks;

// This enum is separate from `FileBlocks` to keep the implementation
// details private to this module; members of an enum cannot be more
// private than the enum itself.
#[allow(clippy::large_enum_variant)]
enum FileBlocksInner {
    ExtentsBlocks(ExtentsBlocks),
    BlockMap(BlockMap),
}

/// Iterator over blocks in a file.
///
/// The iterator produces absolute block indices. A block index of zero
/// indicates a hole.
pub(crate) struct FileBlocks(FileBlocksInner);

impl FileBlocks {
    pub(crate) fn new(fs: Ext4, inode: &Inode) -> Result<Self, Ext4Error> {
        if inode.flags().contains(InodeFlags::EXTENTS) {
            Ok(Self(FileBlocksInner::ExtentsBlocks(ExtentsBlocks::new(
                fs, inode,
            )?)))
        } else {
            Ok(Self(FileBlocksInner::BlockMap(BlockMap::new(fs, inode)?)))
        }
    }

    /// Free all blocks used by this extent tree.
    #[maybe_async::maybe_async]
    pub(crate) async fn free_all(
        mut self,
        ext4: &Ext4,
    ) -> Result<(), Ext4Error> {
        // TODO: Can be more optimal
        let mut blocks = Vec::new();
        while let Some(block) = self.next().await {
            let block = block?;
            blocks.push(block);
        }
        for block in blocks {
            if block == 0 {
                continue;
            }
            ext4.free_block(block).await?;
        }
        Ok(())
    }
}

#[cfg(not(feature = "sync"))]
impl AsyncIterator for FileBlocks {
    type Item = Result<FsBlockIndex, Ext4Error>;

    async fn next(&mut self) -> Option<Result<FsBlockIndex, Ext4Error>> {
        match self {
            Self(FileBlocksInner::ExtentsBlocks(iter)) => iter.next().await,
            Self(FileBlocksInner::BlockMap(iter)) => iter.next().await,
        }
    }
}

#[cfg(feature = "sync")]
impl Iterator for FileBlocks {
    type Item = Result<FsBlockIndex, Ext4Error>;

    fn next(&mut self) -> Option<Result<FsBlockIndex, Ext4Error>> {
        match self {
            Self(FileBlocksInner::ExtentsBlocks(iter)) => iter.next(),
            Self(FileBlocksInner::BlockMap(iter)) => iter.next(),
        }
    }
}
