use crate::block_index::{FileBlockIndex, FsBlockIndex};
use crate::features::FilesystemFeature;
use crate::{Ext4, Ext4Error, IncompatibleFeatures, Inode, InodeFlags};

pub(crate) mod block_map;
pub(crate) mod extent_tree;
mod utils;

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
                let Some(extent) = extent_tree.find_extent(block_index).await?
                else {
                    return Ok(0);
                };
                if !extent.is_initialized {
                    return Ok(0);
                }
                extent_tree.get_block(block_index).await
            }
            Self::BlockMap(block_map) => block_map.get_block(block_index).await,
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn write_at(
        &mut self,
        inode: &mut Inode,
        buf: &[u8],
        offset: u64,
    ) -> Result<usize, Ext4Error> {
        if inode.flags().contains(InodeFlags::IMMUTABLE) {
            return Err(Ext4Error::Readonly);
        }

        match self {
            Self::ExtentTree(extent_tree) => {
                extent_tree.write_at(inode, buf, offset).await
            }
            Self::BlockMap(block_map) => {
                block_map.write_at(inode, buf, offset).await
            }
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn truncate(
        &mut self,
        inode: &mut Inode,
        new_size: u64,
    ) -> Result<(), Ext4Error> {
        match self {
            Self::ExtentTree(extent_tree) => {
                extent_tree.truncate(inode, new_size).await
            }
            Self::BlockMap(block_map) => {
                block_map.truncate(inode, new_size).await
            }
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn claim_uninitialized_blocks(
        &mut self,
        inode: &mut Inode,
        start_block: u32,
        num_blocks: u32,
    ) -> Result<(), Ext4Error> {
        if inode.flags().contains(InodeFlags::IMMUTABLE) {
            return Err(Ext4Error::Readonly);
        }

        match self {
            Self::ExtentTree(extent_tree) => {
                extent_tree
                    .claim_uninitialized_blocks(inode, start_block, num_blocks)
                    .await
            }
            Self::BlockMap(_) => Err(Ext4Error::UnsupportedOperation(
                FilesystemFeature::Incompatible(IncompatibleFeatures::EXTENTS),
            )),
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn free_uninitialized_blocks(
        &mut self,
        inode: &mut Inode,
        start_block: u32,
        num_blocks: u32,
    ) -> Result<(), Ext4Error> {
        if inode.flags().contains(InodeFlags::IMMUTABLE) {
            return Err(Ext4Error::Readonly);
        }

        match self {
            Self::ExtentTree(extent_tree) => {
                extent_tree
                    .free_uninitialized_blocks(inode, start_block, num_blocks)
                    .await
            }
            Self::BlockMap(_) => Err(Ext4Error::UnsupportedOperation(
                FilesystemFeature::Incompatible(IncompatibleFeatures::EXTENTS),
            )),
        }
    }
}
