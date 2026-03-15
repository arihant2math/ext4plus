#![allow(unused)]

use crate::{Ext4, Ext4Error, Inode, InodeFlags};

use crate::block_index::{FileBlockIndex, FsBlockIndex};

pub(crate) mod block_map;
pub(crate) mod extent_tree;

pub(crate) enum FileBlocks {
    BlockMap(block_map::BlockMap),
    ExtentTree(extent_tree::ExtentTree),
}

impl FileBlocks {
    pub(crate) fn initialize(
        inode: &Inode,
        ext4: Ext4,
    ) -> Result<Self, Ext4Error> {
        if inode.flags().contains(InodeFlags::EXTENTS) {
            Ok(Self::ExtentTree(extent_tree::ExtentTree::initialize(
                inode, ext4,
            )?))
        } else {
            Ok(Self::BlockMap(block_map::BlockMap::initialize(ext4)))
        }
    }

    pub(crate) fn from_inode(
        inode: &Inode,
        ext4: Ext4,
    ) -> Result<Self, Ext4Error> {
        if inode.flags().contains(InodeFlags::EXTENTS) {
            Ok(Self::ExtentTree(extent_tree::ExtentTree::from_inode(
                inode, ext4,
            )?))
        } else {
            Ok(Self::BlockMap(block_map::BlockMap::from_inode(inode, ext4)))
        }
    }

    pub(crate) fn to_bytes(&self) -> Result<[u8; 60], Ext4Error> {
        match self {
            Self::ExtentTree(extent_tree) => extent_tree.to_bytes(),
            Self::BlockMap(block_map) => Ok(block_map.to_bytes()),
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn get_block(
        &self,
        block_index: FileBlockIndex,
    ) -> Result<FsBlockIndex, Ext4Error> {
        match self {
            Self::ExtentTree(extent_tree) => {
                extent_tree.get_block(block_index).await
            }
            Self::BlockMap(block_map) => block_map.get_block(block_index).await,
        }
    }
}
