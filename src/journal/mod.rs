// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

mod block_header;
mod block_map;
mod commit_block;
mod descriptor_block;
mod revocation_block;
mod superblock;

use crate::Ext4;
use crate::block_index::FsBlockIndex;
use crate::checksum::Checksum;
use crate::error::{CorruptKind, Ext4Error};
use crate::file_blocks::FileBlocks;
use crate::inode::Inode;
use crate::journal::block_header::JournalBlockHeader;
use crate::journal::block_map::load_block_map;
use crate::journal::descriptor_block::{
    DescriptorBlockTagIter, validate_descriptor_block_checksum,
};
use crate::sync::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use superblock::JournalSuperblock;

#[derive(Debug)]
pub(crate) struct FsJournal {
    inode: Inode,
    superblock: JournalSuperblock,
    block_map: block_map::BlockMap,
    current_users: Vec<u32>,
    file_blocks: FileBlocks,

}

impl FsJournal {
    fn map_block_index(&self, block_index: FsBlockIndex) -> FsBlockIndex {
        self.block_map.get(&block_index).copied().unwrap_or(block_index)
    }

    #[maybe_async::maybe_async]
    async fn init_rw(&mut self, fs: &Ext4) -> Result<(), Ext4Error> {
        let mut current_txn = BTreeMap::new();
        let mut current_sequence_number = self.superblock.sequence;
        let mut block = vec![0; fs.0.superblock.block_size().to_usize()];
        let mut data_block = vec![0; fs.0.superblock.block_size().to_usize()];
        let mut block_id = self.superblock.start_block;
        while block_id < u32::try_from(self.inode.fs_blocks(fs)?).unwrap() {
            let block_index = self.file_blocks.get_block(block_id).await?;
            fs.read_from_block(block_index, 0, &mut block).await?;
            let Some(header) = JournalBlockHeader::read_bytes(&block) else {
                return Ok(());
            };
            if header.sequence != current_sequence_number {
                return Err(CorruptKind::JournalSequence.into());
            }
            if matches!(
                header.block_type,
                block_header::JournalBlockType::DESCRIPTOR
            ) {
                validate_descriptor_block_checksum(&self.superblock, &block)?;
                let tags = DescriptorBlockTagIter::new(
                    &block[JournalBlockHeader::SIZE..],
                );

                for tag in tags {
                    let tag = tag?;

                    block_id += 1;
                    let block_index =
                        self.file_blocks.get_block(block_id).await?;

                    // Check the data block checksum.
                    let mut checksum = Checksum::new();
                    checksum.update(self.superblock.uuid.as_bytes());
                    checksum.update_u32_be(current_sequence_number);
                    fs.read_from_block(block_index, 0, &mut data_block).await?;
                    checksum.update(&data_block);
                    if checksum.finalize() != tag.checksum {
                        return Err(
                            CorruptKind::JournalDescriptorTagChecksum.into()
                        );
                    }

                    current_txn.insert(tag.block_index, block_index);
                }
            } else if matches!(
                header.block_type,
                block_header::JournalBlockType::COMMIT
            ) {
                todo!()
            }
        }
        Ok(())
    }

    fn write_all(&mut self, fs: &Ext4) -> Result<(), Ext4Error> {
        todo!()
    }
}

struct MemJournal {
    block_map: block_map::BlockMap,
}

impl MemJournal {
    pub(crate) fn map_block_index(
        &self,
        block_index: FsBlockIndex,
    ) -> FsBlockIndex {
        self.block_map
            .get(&block_index)
            .copied()
            .unwrap_or(block_index)
    }
}

enum JournalInner {
    FsBacked(FsJournal),
    MemoryBacked(MemJournal),
}

impl JournalInner {
    fn map_block_index(&self, block_index: FsBlockIndex) -> FsBlockIndex {
        match self {
            Self::FsBacked(fs_journal) => {
                fs_journal.map_block_index(block_index)
            }
            Self::MemoryBacked(mem_journal) => {
                mem_journal.map_block_index(block_index)
            }
        }
    }

    #[maybe_async::maybe_async]
    async fn init_rw(&mut self, fs: &Ext4) -> Result<(), Ext4Error> {
        match self {
            Self::FsBacked(fs_journal) => fs_journal.init_rw(fs).await,
            Self::MemoryBacked(_) => Ok(()),
        }
    }
}

pub(crate) struct Journal {
    inner: Arc<RwLock<JournalInner>>,
}

impl Journal {
    /// Create an empty journal.
    pub(crate) fn mem() -> Self {
        Self {
            inner: Arc::new(RwLock::new(JournalInner::MemoryBacked(MemJournal {
                block_map: block_map::BlockMap::new(),
            }))),
        }
    }

    /// Load a journal from the filesystem.
    ///
    /// If the filesystem has no journal, an empty journal is returned.
    ///
    /// Note: ext4 is all little-endian, except for the journal, which
    /// is all big-endian.
    #[maybe_async::maybe_async]
    pub(crate) async fn load(fs: &Ext4) -> Result<Self, Ext4Error> {
        let Some(journal_inode) = fs.0.superblock.journal_inode() else {
            // Return an empty journal if this filesystem does not have
            // a journal.
            return Ok(Self::mem());
        };

        let journal_inode = Inode::read(fs, journal_inode).await?;
        let superblock = JournalSuperblock::load(fs, &journal_inode).await?;
        let block_map = load_block_map(fs, &superblock, &journal_inode).await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(JournalInner::FsBacked(FsJournal {
                superblock,
                block_map,
                current_users: vec![],
                file_blocks: FileBlocks::from_inode(
                    &journal_inode,
                    fs.clone(),
                )?,
                inode: journal_inode,
            }))),
        })
    }

    /// Write all pending to the journal to the filesystem, and clear the journal.
    #[maybe_async::maybe_async]
    pub(crate) async fn init_rw(&mut self, fs: &Ext4) -> Result<(), Ext4Error> {
        self.inner.write().await.init_rw(fs).await
    }

    /// Map from an absolute block index to a block in the journal.
    ///
    /// If the journal does not contain a replacement for the input
    /// block, the input block is returned.
    #[maybe_async::maybe_async]
    pub(crate) async fn map_block_index(
        &self,
        block_index: FsBlockIndex,
    ) -> FsBlockIndex {
        self.inner.read().await.map_block_index(block_index)
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use crate::test_util::load_compressed_filesystem;
    use std::sync::Arc;

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_journal() {
        let mut fs =
            load_compressed_filesystem("test_disk_4k_block_journal.bin.zst")
                .await;

        let test_dir = "/dir500";

        // With the journal in place, this directory exists.
        let exists = fs.exists(test_dir).await.unwrap();
        assert!(exists);
    }
}
