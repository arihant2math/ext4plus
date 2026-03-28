// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! This crate provides read-only access to [ext4] filesystems. It also
//! works with [ext2] filesystems.
//!
//! The main entry point is the [`Ext4`] struct.
//!
//! [ext2]: https://en.wikipedia.org/wiki/Ext2
//! [ext4]: https://en.wikipedia.org/wiki/Ext4
//!
//! # Example
//!
//! This example reads the filesystem data from a byte vector, then
//! looks at files and directories in the filesystem.
//!
//! ```ignore
//! use ext4plus::prelude::{AsyncIterator, Ext4, Ext4Error, Metadata};
//!
//! #[tokio::main]
//! async fn in_memory_example(fs_data: Vec<u8>) -> Result<(), Ext4Error> {
//!     let fs = Ext4::load(Box::new(fs_data)).await.unwrap();
//!
//!     let path = "/some/file";
//!
//!     // Read a file's contents.
//!     let file_data: Vec<u8> = fs.read(path).await?;
//!
//!     // Read a file's contents as a string.
//!     let file_str: String = fs.read_to_string(path).await?;
//!
//!     // Check whether a path exists.
//!     let exists: bool = fs.exists(path).await?;
//!
//!     // Get metadata (file type, permissions, etc).
//!     let metadata: Metadata = fs.metadata(path).await?;
//!
//!     // Print each entry in a directory.
//!     for entry in fs.read_dir("/some/dir").await?.collect::<Vec<_>>().await {
//!         let entry = entry?;
//!         println!("{}", entry.path().display());
//!     }
//!
//!     Ok(())
//! }
//! ```
//! # Loading a filesystem
//!
//! Call [`Ext4::load`] to load a filesystem. The source data can be
//! anything that implements the [`Ext4Read`] trait. The simplest form
//! of source data is a `Vec<u8>` containing the whole filesystem.
//!
//! If the `std` feature is enabled, [`Ext4Read`] is implemented for
//! [`std::fs::File`].
//!
//! For other cases, implement [`Ext4Read`] for your data source. This
//! trait has a single method which reads bytes into a byte slice.
//!
//! Note that the underlying data should never be changed while the
//! filesystem is in use.
//!
//! # Paths
//!
//! Paths in the filesystem are represented by [`Path`] and
//! [`PathBuf`]. These types are similar to the types of the same names
//! in [`std::path`].
//!
//! Functions that take a path as input accept a variety of types
//! including strings.
//!
//! # Errors
//!
//! Most functions return [`Ext4Error`] on failure. This type is broadly
//! similar to [`std::io::Error`], with a few notable additions:
//! * Errors that come from the underlying reader are returned as
//!   [`Ext4Error::Io`].
//! * If the filesystem is corrupt in some way, [`Ext4Error::Corrupt`]
//!   is returned.
//! * If the filesystem can't be read due to a limitation of the
//!   library, [`Ext4Error::Incompatible`] is returned. Please file a
//!   bug if you encounter an incompatibility so we know to
//!   prioritize a fix!
//!
//! Some functions list specific errors that may occur. These lists are
//! not exhaustive; calling code should be prepared to handle other
//! errors such as [`Ext4Error::Io`].

#![cfg_attr(not(any(feature = "std", test)), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(
    clippy::arithmetic_side_effects,
    clippy::as_conversions,
    clippy::must_use_candidate,
    clippy::use_self
)]
#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![allow(clippy::while_let_on_iterator)]

extern crate alloc;
extern crate core;

mod bitmap;
mod block_group;
mod block_index;
mod block_size;
mod checksum;
pub mod dir;
mod dir_block;
mod dir_entry;
mod dir_entry_hash;
mod dir_htree;
pub mod error;
mod extent;
mod features;
pub mod file;
mod file_blocks;
mod file_type;
mod format;
pub mod inode;
pub mod iters;
mod journal;
mod label;
mod mem_io_error;
mod metadata;
pub mod path;
pub mod prelude;
mod reader;
mod resolve;
pub mod superblock;
pub mod sync;
mod util;
mod uuid;
mod writer;

#[cfg(all(test, feature = "std"))]
mod test_util;

use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use bitmap::BitmapHandle;
use block_group::{BlockGroupDescriptor, BlockGroupIndex};
use block_index::FsBlockIndex;
use core::fmt::{self, Debug, Formatter};
use core::num::NonZeroU32;
use core::num::NonZeroU64;
use core::time::Duration;
use dir::Dir;
use error::{CorruptKind, Ext4Error};
use features::ReadOnlyCompatibleFeatures;
use file::{File, write_at};
use inode::{
    Inode, InodeCreationOptions, InodeFlags, InodeIndex, InodeMode,
    get_inode_block_group_location,
};
use iters::file_blocks::FileBlocks;
use journal::Journal;
use path::{Path, PathBuf};
use superblock::Superblock;
use util::{u64_from_usize, usize_from_u32};

pub use dir_entry::{DirEntry, DirEntryName, DirEntryNameError};
pub use features::IncompatibleFeatures;
pub use file_type::FileType;
pub use format::BytesDisplay;
pub use iters::read_dir::ReadDir;
pub use label::Label;
pub use mem_io_error::MemIoError;
pub use metadata::Metadata;
pub use reader::Ext4Read;
pub use resolve::FollowSymlinks;
pub use uuid::Uuid;
pub use writer::Ext4Write;

struct Ext4Inner {
    superblock: Superblock,
    block_group_descriptors: Vec<BlockGroupDescriptor>,
    journal: Journal,

    /// Reader providing access to the underlying storage.
    ///
    /// Stored as `Box<dyn Ext4Read>` rather than a generic type to make
    /// the `Ext4` type more convenient to pass around for users of the API.
    reader: Box<dyn Ext4Read>,
    /// Optional writer providing write access to the underlying storage.
    writer: Option<Box<dyn Ext4Write>>,
}

/// Read-only access to an [ext4] filesystem.
///
/// [ext4]: https://en.wikipedia.org/wiki/Ext4
#[derive(Clone)]
pub struct Ext4(Arc<Ext4Inner>);

impl Ext4 {
    /// Load an `Ext4` instance from the given `reader`.
    ///
    /// This reads and validates the superblock, block group
    /// descriptors, and journal. No other data is read.
    #[maybe_async::maybe_async]
    pub async fn load(reader: Box<dyn Ext4Read>) -> Result<Self, Ext4Error> {
        Self::load_with_writer(reader, None).await
    }

    /// Load an `Ext4` instance from the given `reader` and `writer`.
    ///
    /// This reads and validates the superblock, block group
    /// descriptors, and journal. No other data is read or written.
    #[maybe_async::maybe_async]
    pub async fn load_with_writer(
        mut reader: Box<dyn Ext4Read>,
        mut writer: Option<Box<dyn Ext4Write>>,
    ) -> Result<Self, Ext4Error> {
        // The first 1024 bytes are reserved for "weird" stuff like x86
        // boot sectors.
        let superblock_start = 1024;
        let mut data = vec![0; Superblock::SIZE_IN_BYTES_ON_DISK];
        reader
            .read(superblock_start, &mut data)
            .await
            .map_err(Ext4Error::Io)?;

        let superblock = Superblock::from_bytes(&data)?;

        if superblock.read_only() {
            writer = None;
        }
        let mut fs = Self(Arc::new(Ext4Inner {
            block_group_descriptors: BlockGroupDescriptor::read_all(
                &superblock,
                &mut *reader,
            )
            .await?,
            reader,
            writer,
            superblock,
            // Initialize with an empty journal, because loading the
            // journal requires a valid `Ext4` object.
            journal: Journal::empty(),
        }));

        // Load the actual journal, if present.
        let journal = Journal::load(&fs).await?;
        // OK to unwrap: the journal is stored in an `Arc`, but we haven't cloned it yet, so we have unique access to it.
        Arc::get_mut(&mut fs.0).unwrap().journal = journal;

        Ok(fs)
    }

    #[cfg(all(feature = "std", target_family = "unix"))]
    /// Load an [`Ext4`] instance from a file at the given path.
    #[maybe_async::maybe_async]
    pub async fn load_from_path<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Self, Ext4Error> {
        let file = std::fs::File::open(path)
            .map_err(|err| Ext4Error::Io(Box::new(err)))?;
        Self::load(Box::new(file)).await
    }

    /// Get the filesystem label.
    #[must_use]
    pub fn label(&self) -> &Label {
        self.0.superblock.label()
    }

    /// Get the filesystem UUID.
    #[must_use]
    pub fn uuid(&self) -> Uuid {
        self.0.superblock.uuid()
    }

    /// Return true if the filesystem has metadata checksums enabled,
    /// false otherwise.
    fn has_metadata_checksums(&self) -> bool {
        self.0
            .superblock
            .read_only_compatible_features()
            .contains(ReadOnlyCompatibleFeatures::METADATA_CHECKSUMS)
    }

    /// Get a reference to the superblock.
    #[must_use]
    pub fn superblock(&self) -> &Superblock {
        &self.0.superblock
    }

    /// Read the inode of the root `/` directory.
    #[maybe_async::maybe_async]
    pub async fn read_root_inode(&self) -> Result<Inode, Ext4Error> {
        // OK to unwrap: infallible
        let root_inode_index = InodeIndex::new(2).unwrap();
        Inode::read(self, root_inode_index).await
    }

    /// Read data from a block.
    ///
    /// `block_index`: an absolute block within the filesystem.
    ///
    /// `offset_within_block`: the byte offset within the block to start
    /// reading from.
    ///
    /// `dst`: byte buffer to read into. This also controls the length
    /// of the read.
    ///
    /// The first 1024 bytes of the filesystem are reserved for
    /// non-filesystem data. Reads are not allowed there.
    ///
    /// The read cannot cross block boundaries. This implies that:
    /// * `offset_within_block < block_size`
    /// * `offset_within_block + dst.len() <= block_size`
    ///
    /// If any of these conditions are violated, a `CorruptKind::BlockRead`
    /// error is returned.
    #[maybe_async::maybe_async]
    async fn read_from_block(
        &self,
        original_block_index: FsBlockIndex,
        offset_within_block: u32,
        dst: &mut [u8],
    ) -> Result<(), Ext4Error> {
        let block_index = self.0.journal.map_block_index(original_block_index);

        let err = || {
            Ext4Error::from(CorruptKind::BlockRead {
                block_index,
                original_block_index,
                offset_within_block,
                read_len: dst.len(),
            })
        };

        // The first 1024 bytes are reserved for non-filesystem
        // data. This conveniently allows for something like a null
        // pointer check.
        if block_index == 0 && offset_within_block < 1024 {
            return Err(err());
        }

        // Check the block index.
        if block_index >= self.0.superblock.blocks_count() {
            return Err(err());
        }

        // The start of the read must be less than the block size.
        let block_size = self.0.superblock.block_size();
        if offset_within_block >= block_size {
            return Err(err());
        }

        // The end of the read must be less than or equal to the block size.
        let read_end = usize_from_u32(offset_within_block)
            .checked_add(dst.len())
            .ok_or_else(err)?;
        if read_end > block_size {
            return Err(err());
        }

        // Read the block
        self.0
            .reader
            .read(
                block_index
                    .checked_mul(self.0.superblock.block_size().to_u64())
                    .ok_or(CorruptKind::InvalidBlockSize)?
                    .checked_add(u64::from(offset_within_block))
                    .ok_or(CorruptKind::InvalidBlockSize)?,
                dst,
            )
            .await
            .map_err(Ext4Error::Io)?;
        Ok(())
    }

    /// Read a whole block
    #[maybe_async::maybe_async]
    async fn read_block(
        &self,
        original_block_index: FsBlockIndex,
    ) -> Result<Vec<u8>, Ext4Error> {
        let block_size = self.0.superblock.block_size();
        let mut block = vec![0; block_size.to_usize()];
        self.read_from_block(original_block_index, 0, &mut block)
            .await?;
        Ok(block)
    }

    /// Write data to a block.
    #[maybe_async::maybe_async]
    async fn write_to_block(
        &self,
        original_block_index: FsBlockIndex,
        offset_within_block: u32,
        src: &[u8],
    ) -> Result<(), Ext4Error> {
        let block_index = self.0.journal.map_block_index(original_block_index);

        let err = || {
            Ext4Error::from(CorruptKind::BlockWrite {
                block_index,
                original_block_index,
                offset_within_block,
                write_len: src.len(),
            })
        };
        // The first 1024 bytes are reserved for non-filesystem
        // data. This conveniently allows for something like a null
        // pointer check.
        if block_index == 0 && offset_within_block < 1024 {
            return Err(err());
        }

        // Check the block index.
        if block_index >= self.0.superblock.blocks_count() {
            return Err(err());
        }

        // The start of the write must be less than the block size.
        let block_size = self.0.superblock.block_size();
        if offset_within_block >= block_size {
            return Err(err());
        }

        // The end of the write must be less than or equal to the block size.
        let write_end = usize_from_u32(offset_within_block)
            .checked_add(src.len())
            .ok_or_else(err)?;
        if write_end > block_size {
            return Err(err());
        }

        // Write through to underlying storage
        if let Some(writer) = &self.0.writer {
            writer
                .write(
                    block_index
                        .checked_mul(self.0.superblock.block_size().to_u64())
                        .ok_or(CorruptKind::InvalidBlockSize)?
                        .checked_add(u64::from(offset_within_block))
                        .ok_or(CorruptKind::InvalidBlockSize)?,
                    src,
                )
                .await
                .map_err(Ext4Error::Io)?;
        } else {
            return Err(Ext4Error::Readonly);
        }
        Ok(())
    }

    fn get_block_group_descriptor(
        &self,
        block_group_index: BlockGroupIndex,
    ) -> &BlockGroupDescriptor {
        assert!(
            usize_from_u32(block_group_index)
                < self.0.block_group_descriptors.len(),
            "Block group index out of bounds: {block_group_index}"
        );
        &self.0.block_group_descriptors[usize_from_u32(block_group_index)]
    }

    fn get_block_bitmap_handle(
        &self,
        block_group_index: BlockGroupIndex,
    ) -> BitmapHandle {
        let block_group = self.get_block_group_descriptor(block_group_index);
        BitmapHandle::new(block_group.block_bitmap_block(), false)
    }

    fn get_inode_bitmap_handle(
        &self,
        block_group_index: BlockGroupIndex,
    ) -> BitmapHandle {
        let block_group = self.get_block_group_descriptor(block_group_index);
        BitmapHandle::new(block_group.inode_bitmap_block(), true)
    }

    #[maybe_async::maybe_async]
    async fn update_block_bitmap_checksum(
        &self,
        block_group_index: BlockGroupIndex,
        bitmap_handle: BitmapHandle,
    ) -> Result<(), Ext4Error> {
        let checksum =
            bitmap_handle.calc_checksum(self, block_group_index).await?;
        let block_group = self.get_block_group_descriptor(block_group_index);
        block_group.set_block_bitmap_checksum(checksum);
        block_group.write(self).await?;
        Ok(())
    }

    #[maybe_async::maybe_async]
    async fn update_inode_bitmap_checksum(
        &self,
        block_group_index: BlockGroupIndex,
        bitmap_handle: BitmapHandle,
    ) -> Result<(), Ext4Error> {
        let checksum =
            bitmap_handle.calc_checksum(self, block_group_index).await?;
        let block_group = self.get_block_group_descriptor(block_group_index);
        block_group.set_inode_bitmap_checksum(checksum);
        block_group.write(self).await?;
        Ok(())
    }

    #[expect(unused)]
    /// Query the bitmap to check if a block is in use.
    #[maybe_async::maybe_async]
    async fn query_block(
        &self,
        block_index: FsBlockIndex,
    ) -> Result<bool, Ext4Error> {
        let (block_group_index, block_offset) =
            self.block_block_group_location(block_index)?;
        let bitmap_handle = self.get_block_bitmap_handle(block_group_index);
        bitmap_handle.query(block_offset, self).await
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn alloc_inode(
        &self,
        inode_type: FileType,
    ) -> Result<InodeIndex, Ext4Error> {
        let mut bg_id = 0;
        let mut bg_count = self.0.superblock.num_block_groups();
        let mut rewind = false;
        while bg_id <= bg_count {
            if bg_id == bg_count {
                if rewind {
                    break;
                }
                bg_count = bg_id;
                bg_id = 0;
                rewind = true;
                continue;
            }

            let bg = self.get_block_group_descriptor(bg_id);

            let free_inodes = bg.free_inodes_count();
            let used_dirs = bg.used_dirs_count();

            if free_inodes > 0 {
                let inode_bitmap_handle = self.get_inode_bitmap_handle(bg_id);
                let Some(inode_num) = inode_bitmap_handle
                    .find_first(
                        false,
                        ..self.0.superblock.inodes_per_block_group().get(),
                        self,
                    )
                    .await?
                else {
                    continue;
                };
                inode_bitmap_handle.set(inode_num, true, self).await?;
                self.update_inode_bitmap_checksum(bg_id, inode_bitmap_handle)
                    .await?;
                bg.set_free_inodes_count(free_inodes.checked_sub(1).unwrap());
                if self
                    .0
                    .superblock
                    .inodes_per_block_group()
                    .get()
                    .checked_sub(bg.unused_inodes_count())
                    .ok_or(
                        CorruptKind::BlockGroupDescriptorTooManyUnusedInodes {
                            block_group_num: bg_id,
                            num_unused_inodes: bg.unused_inodes_count(),
                        },
                    )?
                    .checked_sub(1)
                    .ok_or(
                        CorruptKind::BlockGroupDescriptorTooManyUnusedInodes {
                            block_group_num: bg_id,
                            num_unused_inodes: bg.unused_inodes_count(),
                        },
                    )?
                    <= inode_num
                {
                    bg.set_unused_inodes_count(
                        self.0
                            .superblock
                            .inodes_per_block_group()
                            .get()
                            .checked_sub(inode_num)
                            .unwrap()
                            .checked_sub(1)
                            .unwrap(),
                    );
                }

                if matches!(inode_type, FileType::Directory) {
                    bg.set_used_dirs_count(used_dirs.saturating_add(1));
                }
                bg.write(self).await?;
                let total_free_inodes = self.0.superblock.free_inodes_count();
                self.0.superblock.set_free_inodes_count(
                    total_free_inodes.checked_sub(1).unwrap(),
                );
                self.0.superblock.write(self).await?;

                return Ok(InodeIndex::try_from(
                    inode_num
                        .checked_add(
                            self.0
                                .superblock
                                .inodes_per_block_group()
                                .get()
                                .checked_mul(bg_id)
                                .unwrap(),
                        )
                        .unwrap()
                        .checked_add(1)
                        .unwrap(),
                )
                .unwrap());
            }

            // Will never overflow
            bg_id = bg_id.saturating_add(1);
        }
        Err(Ext4Error::NoSpace)
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn free_inode(
        &self,
        inode: Inode,
    ) -> Result<(), Ext4Error> {
        let (block_group_index, inode_offset) =
            get_inode_block_group_location(&self.0.superblock, inode.index)?;
        let inode_bitmap_handle =
            self.get_inode_bitmap_handle(block_group_index);
        inode_bitmap_handle.set(inode_offset, false, self).await?;
        self.update_inode_bitmap_checksum(
            block_group_index,
            inode_bitmap_handle,
        )
        .await?;
        // Set number of free inodes in block group
        let bg = self.get_block_group_descriptor(block_group_index);
        let free_inodes = bg.free_inodes_count();
        bg.set_free_inodes_count(free_inodes.saturating_add(1));
        if inode.file_type().is_dir() {
            let used_dirs = bg.used_dirs_count();
            bg.set_used_dirs_count(used_dirs.saturating_sub(1));
        }
        bg.write(self).await?;
        // Set number of free inodes in superblock
        let total_free_inodes = self.0.superblock.free_inodes_count();
        self.0
            .superblock
            .set_free_inodes_count(total_free_inodes.saturating_add(1));
        self.0.superblock.write(self).await?;
        Ok(())
    }

    pub(crate) fn block_block_group_location(
        &self,
        block_index: FsBlockIndex,
    ) -> Result<(BlockGroupIndex, u32), Ext4Error> {
        let blocks_per_group =
            NonZeroU64::from(self.0.superblock.blocks_per_group());
        let relative_block_index = block_index
            .checked_sub(u64::from(self.0.superblock.first_data_block()))
            .ok_or(CorruptKind::FirstDataBlock(
                self.0.superblock.first_data_block(),
            ))?;
        let block_group_index = relative_block_index / blocks_per_group;
        let block_offset = relative_block_index % blocks_per_group;
        Ok((
            // TODO: Wrong error?
            BlockGroupIndex::try_from(block_group_index)
                .map_err(|_| CorruptKind::TooManyBlockGroups)?,
            u32::try_from(block_offset).unwrap(),
        ))
    }

    #[expect(unused)]
    #[maybe_async::maybe_async]
    pub(crate) async fn alloc_block_num(
        &self,
        block: FsBlockIndex,
    ) -> Result<(), Ext4Error> {
        let (block_group_index, block_offset) =
            self.block_block_group_location(block)?;
        let block_bitmap_handle =
            self.get_block_bitmap_handle(block_group_index);
        if block_bitmap_handle.query(block_offset, self).await? {
            return Err(Ext4Error::AlreadyExists);
        }
        block_bitmap_handle.set(block_offset, true, self).await?;
        self.update_block_bitmap_checksum(
            block_group_index,
            block_bitmap_handle,
        )
        .await?;
        // Set number of free blocks in block group
        let bg = self.get_block_group_descriptor(block_group_index);
        let free_blocks = bg.free_blocks_count();
        bg.set_free_blocks_count(free_blocks.checked_sub(1).unwrap());
        bg.write(self).await?;
        self.0.superblock.dec_free_blocks_count(1);
        self.0.superblock.write(self).await?;
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn alloc_block(
        &self,
        inode_index: InodeIndex,
    ) -> Result<FsBlockIndex, Ext4Error> {
        let mut bg_id = (inode_index.get() - 1)
            / self.0.superblock.inodes_per_block_group();
        let mut bg_count = self.0.superblock.num_block_groups();
        let mut rewind = false;
        while bg_id <= bg_count {
            if bg_id == bg_count {
                if rewind {
                    break;
                }
                bg_count = bg_id;
                bg_id = 0;
                rewind = true;
                continue;
            }

            let bg = self.get_block_group_descriptor(bg_id);

            let free_blocks = bg.free_blocks_count();

            // TODO: capture with NonZeroU32 and subtract 1 without panic
            if free_blocks > 0 {
                let block_bitmap_handle = self.get_block_bitmap_handle(bg_id);
                let Some(block_num) =
                    block_bitmap_handle.find_first(false, .., self).await?
                else {
                    continue;
                };
                block_bitmap_handle.set(block_num, true, self).await?;
                self.update_block_bitmap_checksum(bg_id, block_bitmap_handle)
                    .await?;
                bg.set_free_blocks_count(
                    free_blocks.checked_sub(1u32).unwrap(),
                );
                bg.write(self).await?;
                self.0.superblock.dec_free_blocks_count(1);
                self.0.superblock.write(self).await?;

                // Zero out the new block
                let block_index = u64::from(bg_id)
                    .checked_mul(
                        NonZeroU64::from(self.0.superblock.blocks_per_group())
                            .get(),
                    )
                    .unwrap()
                    .checked_add(u64::from(block_num))
                    .unwrap()
                    .checked_add(u64::from(
                        self.0.superblock.first_data_block(),
                    ))
                    .unwrap();

                return Ok(block_index);
            }

            // Will never overflow
            bg_id = bg_id.saturating_add(1);
        }
        Err(Ext4Error::NoSpace)
    }

    /// Tries to allocate `num_blocks` contiguous blocks.
    #[maybe_async::maybe_async]
    pub(crate) async fn alloc_contiguous_blocks(
        &self,
        inode_index: InodeIndex,
        num_blocks: NonZeroU32,
    ) -> Result<FsBlockIndex, Ext4Error> {
        let mut bg_id = (inode_index.get() - 1)
            / self.0.superblock.inodes_per_block_group();
        let mut bg_count = self.0.superblock.num_block_groups();
        let mut rewind = false;
        while bg_id <= bg_count {
            if bg_id == bg_count {
                if rewind {
                    break;
                }
                bg_count = bg_id;
                bg_id = 0;
                rewind = true;
                continue;
            }

            let bg = self.get_block_group_descriptor(bg_id);

            let free_blocks = bg.free_blocks_count();

            if free_blocks >= num_blocks.get() {
                let block_bitmap_handle = self.get_block_bitmap_handle(bg_id);
                let Some(block_num) = block_bitmap_handle
                    .find_first_n(num_blocks.into(), false, .., self)
                    .await?
                else {
                    continue;
                };
                for i in 0..num_blocks.get() {
                    block_bitmap_handle
                        .set(block_num.checked_add(i).unwrap(), true, self)
                        .await?;
                }
                self.update_block_bitmap_checksum(bg_id, block_bitmap_handle)
                    .await?;
                bg.set_free_blocks_count(
                    free_blocks.checked_sub(num_blocks.get()).unwrap(),
                );
                bg.write(self).await?;
                self.0
                    .superblock
                    .dec_free_blocks_count(u64::from(num_blocks.get()));
                self.0.superblock.write(self).await?;
                let block_index = (u64::from(bg_id)
                    .checked_mul(
                        NonZeroU64::from(self.0.superblock.blocks_per_group())
                            .get(),
                    )
                    .unwrap())
                .checked_add(u64::from(block_num))
                .unwrap()
                .checked_add(u64::from(self.0.superblock.first_data_block()))
                .unwrap();
                return Ok(block_index);
            }
            bg_id = bg_id.saturating_add(1);
        }
        Err(Ext4Error::NoSpace)
    }

    /// Tries to allocate `num_blocks` contiguous blocks
    /// If it can't find `num_blocks` contiguous blocks, it allocates as many as possible instead
    #[expect(unused)]
    #[maybe_async::maybe_async]
    pub(crate) async fn try_alloc_contiguous_blocks(
        &self,
        inode_index: InodeIndex,
        num_blocks: NonZeroU32,
    ) -> Result<(FsBlockIndex, NonZeroU32), Ext4Error> {
        // TODO: very inefficient on full disk
        for i in (0..num_blocks.get()).rev() {
            let i = NonZeroU32::new(i).ok_or(Ext4Error::NoSpace)?;
            if let Ok(block_index) =
                self.alloc_contiguous_blocks(inode_index, i).await
            {
                return Ok((block_index, i));
            }
        }
        Err(Ext4Error::NoSpace)
    }

    #[expect(unused)]
    #[maybe_async::maybe_async]
    pub(crate) async fn clear_block(
        &self,
        block_index: FsBlockIndex,
    ) -> Result<(), Ext4Error> {
        let zeroes = vec![0; self.0.superblock.block_size().to_usize()];
        self.write_to_block(block_index, 0, &zeroes).await
    }

    #[expect(unused)]
    #[maybe_async::maybe_async]
    pub(crate) async fn clear_blocks(
        &self,
        block_index: FsBlockIndex,
        num_blocks: NonZeroU32,
    ) -> Result<(), Ext4Error> {
        let zeroes = vec![0; self.0.superblock.block_size().to_usize()];
        for i in 0..num_blocks.get() {
            self.write_to_block(
                block_index.checked_add(u64::from(i)).unwrap(),
                0,
                &zeroes,
            )
            .await?;
        }
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn free_block(
        &self,
        block_index: FsBlockIndex,
    ) -> Result<(), Ext4Error> {
        assert_ne!(block_index, 0);
        let (block_group_index, block_offset) =
            self.block_block_group_location(block_index)?;
        let block_bitmap_handle =
            self.get_block_bitmap_handle(block_group_index);
        block_bitmap_handle.set(block_offset, false, self).await?;
        self.update_block_bitmap_checksum(
            block_group_index,
            block_bitmap_handle,
        )
        .await?;
        // Set number of free blocks in block group
        let bg = self.get_block_group_descriptor(block_group_index);
        let free_blocks = bg.free_blocks_count();
        bg.set_free_blocks_count(free_blocks.saturating_add(1));
        bg.write(self).await?;
        self.0.superblock.inc_free_blocks_count(1);
        self.0.superblock.write(self).await?;
        Ok(())
    }

    /// Frees `num_blocks` contiguous blocks starting at `block_index`.
    #[maybe_async::maybe_async]
    pub(crate) async fn free_blocks(
        &self,
        block_index: FsBlockIndex,
        num_blocks: NonZeroU32,
    ) -> Result<(), Ext4Error> {
        assert_ne!(block_index, 0);
        let (block_group_index, block_offset) =
            self.block_block_group_location(block_index)?;
        let block_bitmap_handle =
            self.get_block_bitmap_handle(block_group_index);
        for i in 0..num_blocks.get() {
            block_bitmap_handle
                .set(block_offset.checked_add(i).unwrap(), false, self)
                .await?;
        }
        self.update_block_bitmap_checksum(
            block_group_index,
            block_bitmap_handle,
        )
        .await?;
        // Set number of free blocks in block group
        let bg = self.get_block_group_descriptor(block_group_index);
        let free_blocks = bg.free_blocks_count();
        bg.set_free_blocks_count(
            free_blocks.checked_add(num_blocks.get()).unwrap(),
        );
        bg.write(self).await?;
        self.0
            .superblock
            .inc_free_blocks_count(u64::from(num_blocks.get()));
        self.0.superblock.write(self).await?;
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn delete_file(
        &self,
        inode: Inode,
    ) -> Result<(), Ext4Error> {
        let blocks = FileBlocks::new(self.clone(), &inode)?;
        blocks.free_all(self).await?;
        self.free_inode(inode).await
    }

    /// Create a new inode of the given type, and return its index.
    #[maybe_async::maybe_async]
    pub async fn create_inode(
        &self,
        options: InodeCreationOptions,
    ) -> Result<Inode, Ext4Error> {
        // TODO: for the purposes of fsck, it is proper to write inode data, then mark as used
        let inode_index = self.alloc_inode(options.file_type).await?;
        Inode::create(inode_index, options, self).await
    }

    /// Read the entire contents of a file into a `Vec<u8>`.
    ///
    /// Holes are filled with zero.
    ///
    /// Fails with `FileTooLarge` if the size of the file is too large
    /// to fit in a [`usize`].
    #[maybe_async::maybe_async]
    pub async fn read_inode_file(
        &self,
        inode: &Inode,
    ) -> Result<Vec<u8>, Ext4Error> {
        // Get the file size and initialize the output vector.
        let file_size_in_bytes = usize::try_from(inode.size_in_bytes())
            .map_err(|_| Ext4Error::FileTooLarge)?;
        let mut dst = vec![0; file_size_in_bytes];

        // Use `File` to read the data in chunks.
        let mut file = File::open_inode(self, inode.clone())?;
        let mut remaining = dst.as_mut();
        loop {
            let bytes_read = file.read_bytes(remaining).await?;
            if bytes_read == 0 {
                break;
            }
            remaining = &mut remaining[bytes_read..];
        }
        Ok(dst)
    }

    /// Follow a path to get an inode.
    #[maybe_async::maybe_async]
    pub async fn path_to_inode(
        &self,
        path: Path<'_>,
        follow: FollowSymlinks,
    ) -> Result<Inode, Ext4Error> {
        resolve::resolve_path(self, path, follow).await.map(|v| v.0)
    }

    /// Create a symbolic link at `path` pointing to `target`.
    #[maybe_async::maybe_async]
    pub async fn symlink(
        &self,
        parent_dir: &Dir,
        name: DirEntryName<'_>,
        target: PathBuf,
        uid: u32,
        gid: u32,
        time: Duration,
    ) -> Result<Inode, Ext4Error> {
        let mut inode = self
            .create_inode(InodeCreationOptions {
                file_type: FileType::Symlink,
                mode: InodeMode::S_IFLNK,
                uid,
                gid,
                time,
                flags: InodeFlags::empty(),
            })
            .await?;
        if target.as_ref().len() <= 60 {
            // Fast symlink: store the target in the inode itself.
            let mut target_bytes = [0; 60];
            target_bytes[..target.as_ref().len()]
                .copy_from_slice(target.as_ref());
            inode.set_inline_data(target_bytes);
            inode.set_size_in_bytes(u64_from_usize(target.as_ref().len()));
            inode.set_flags(inode.flags().difference(InodeFlags::EXTENTS));
            inode.write(self).await?;
        } else {
            // Slow symlink: store the target in a data block.
            let target_bytes = target.as_ref();
            write_at(self, &mut inode, target_bytes, 0).await?;
        }
        parent_dir.link(name, &mut inode).await?;
        Ok(inode)
    }
}

/// These methods mirror the [`std::fs`][stdfs] API.
///
/// [stdfs]: https://doc.rust-lang.org/std/fs/index.html
impl Ext4 {
    /// Get the canonical, absolute form of a path with all intermediate
    /// components normalized and symbolic links resolved.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * `path` does not exist.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn canonicalize<'p, P>(
        &self,
        path: P,
    ) -> Result<PathBuf, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        let path = path.try_into().map_err(|_| Ext4Error::MalformedPath)?;
        resolve::resolve_path(self, path, FollowSymlinks::All)
            .await
            .map(|v| v.1)
    }

    /// Open the file at `path`.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * `path` does not exist.
    /// * `path` is a directory or special file type.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn open<'p, P>(&self, path: P) -> Result<File, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        File::open(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }

    /// Read the entire contents of a file as raw bytes.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * `path` does not exist.
    /// * `path` is a directory or special file type.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn read<'p, P>(&self, path: P) -> Result<Vec<u8>, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        #[maybe_async::maybe_async]
        async fn inner(
            fs: &Ext4,
            path: Path<'_>,
        ) -> Result<Vec<u8>, Ext4Error> {
            let inode = fs.path_to_inode(path, FollowSymlinks::All).await?;

            if inode.file_type().is_dir() {
                return Err(Ext4Error::IsADirectory);
            }
            if !inode.file_type().is_regular_file() {
                return Err(Ext4Error::IsASpecialFile);
            }

            fs.read_inode_file(&inode).await
        }

        inner(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }

    /// Read the entire contents of a file as a string.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * `path` does not exist.
    /// * `path` is a directory or special file type.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn read_to_string<'p, P>(
        &self,
        path: P,
    ) -> Result<String, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        #[maybe_async::maybe_async]
        async fn inner(fs: &Ext4, path: Path<'_>) -> Result<String, Ext4Error> {
            let content = fs.read(path).await?;
            String::from_utf8(content).map_err(|_| Ext4Error::NotUtf8)
        }

        inner(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }

    /// Get the target of a symbolic link.
    ///
    /// The final component of `path` must be a symlink. If the path
    /// contains any symlinks in components prior to the end, they will
    /// be fully resolved as normal.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * The final component of `path` is not a symlink.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn read_link<'p, P>(&self, path: P) -> Result<PathBuf, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        #[maybe_async::maybe_async]
        async fn inner(
            fs: &Ext4,
            path: Path<'_>,
        ) -> Result<PathBuf, Ext4Error> {
            let inode = fs
                .path_to_inode(path, FollowSymlinks::ExcludeFinalComponent)
                .await?;
            inode.symlink_target(fs).await
        }

        inner(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }

    /// Get an iterator over the entries in a directory.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * `path` does not exist.
    /// * `path` is not a directory.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn read_dir<'p, P>(&self, path: P) -> Result<ReadDir, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        #[maybe_async::maybe_async]
        async fn inner(
            fs: &Ext4,
            path: Path<'_>,
        ) -> Result<ReadDir, Ext4Error> {
            let inode = fs.path_to_inode(path, FollowSymlinks::All).await?;

            if !inode.file_type().is_dir() {
                return Err(Ext4Error::NotADirectory);
            }

            ReadDir::new(fs.clone(), &inode, path.into())
        }

        inner(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }

    /// Check if `path` exists.
    ///
    /// Returns `Ok(true)` if `path` exists, or `Ok(false)` if it does
    /// not exist.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn exists<'p, P>(&self, path: P) -> Result<bool, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        #[maybe_async::maybe_async]
        async fn inner(fs: &Ext4, path: Path<'_>) -> Result<bool, Ext4Error> {
            match fs.path_to_inode(path, FollowSymlinks::All).await {
                Ok(_) => Ok(true),
                Err(Ext4Error::NotFound) => Ok(false),
                Err(err) => Err(err),
            }
        }

        inner(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }

    /// Get [`Metadata`] for `path`.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * `path` does not exist.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn metadata<'p, P>(&self, path: P) -> Result<Metadata, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        #[maybe_async::maybe_async]
        async fn inner(
            fs: &Ext4,
            path: Path<'_>,
        ) -> Result<Metadata, Ext4Error> {
            let inode = fs.path_to_inode(path, FollowSymlinks::All).await?;
            Ok(inode.metadata())
        }

        inner(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }

    /// Get [`Metadata`] for `path`.
    ///
    /// If the final component of `path` is a symlink, information about
    /// the symlink itself will be returned, not the symlink's
    /// targets. Any other symlink components of `path` are resolved as
    /// normal.
    ///
    /// # Errors
    ///
    /// An error will be returned if:
    /// * `path` is not absolute.
    /// * `path` does not exist.
    ///
    /// This is not an exhaustive list of errors, see the
    /// [crate documentation](crate#errors).
    #[maybe_async::maybe_async]
    pub async fn symlink_metadata<'p, P>(
        &self,
        path: P,
    ) -> Result<Metadata, Ext4Error>
    where
        P: TryInto<Path<'p>>,
    {
        #[maybe_async::maybe_async]
        async fn inner(
            fs: &Ext4,
            path: Path<'_>,
        ) -> Result<Metadata, Ext4Error> {
            let inode = fs
                .path_to_inode(path, FollowSymlinks::ExcludeFinalComponent)
                .await?;
            Ok(inode.metadata())
        }

        inner(self, path.try_into().map_err(|_| Ext4Error::MalformedPath)?)
            .await
    }
}

impl Debug for Ext4 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Exclude the reader field, which does not impl Debug. Even if
        // it did, it could be annoying to print out (e.g. if the reader
        // is a Vec it might contain many megabytes of data).
        f.debug_struct("Ext4")
            .field("superblock", &self.0.superblock)
            .field("block_group_descriptors", &self.0.block_group_descriptors)
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "std")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::load_test_disk1_rw_no_fsck;
    use test_util::load_test_disk1;

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_load_errors() {
        // Not enough data.
        let err = Ext4::load(Box::new(vec![])).await.unwrap_err();
        assert!(matches!(err, Ext4Error::Io(_)));

        // Invalid superblock.
        let err = Ext4::load(Box::new(vec![0; 2048])).await.unwrap_err();
        assert_eq!(err, CorruptKind::SuperblockMagic);

        // Not enough data to read the block group descriptors.
        let mut fs_data = vec![0; 2048];
        fs_data[1024..2048]
            .copy_from_slice(include_bytes!("../test_data/raw_superblock.bin"));
        let err = Ext4::load(Box::new(fs_data.clone())).await.unwrap_err();
        assert!(matches!(err, Ext4Error::Io(_)));

        // Invalid block group descriptor checksum.
        fs_data.resize(3048usize, 0u8);
        let err = Ext4::load(Box::new(fs_data.clone())).await.unwrap_err();
        assert_eq!(err, CorruptKind::BlockGroupDescriptorChecksum(0));
    }

    /// Test that loading the data from
    /// https://github.com/nicholasbishop/ext4-view-rs/issues/280 does not
    /// panic.
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_invalid_ext4_data() {
        // Fill in zeros for the first 1024 bytes, then add the test data.
        let mut data = vec![0; 1024];
        data.extend(include_bytes!("../test_data/not_ext4.bin"));

        let err = Ext4::load(Box::new(data)).await.unwrap_err();
        assert_eq!(err, CorruptKind::InvalidBlockSize);
    }

    fn block_read_error(
        block_index: FsBlockIndex,
        offset_within_block: u32,
        read_len: usize,
    ) -> CorruptKind {
        CorruptKind::BlockRead {
            block_index,
            original_block_index: block_index,
            offset_within_block,
            read_len,
        }
    }

    /// Test that reading from the first 1024 bytes of the file fails.
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_read_from_block_first_1024() {
        let fs = load_test_disk1().await;
        let mut dst = vec![0; 1];
        let err = fs.read_from_block(0, 1023, &mut dst).await.unwrap_err();
        assert_eq!(err, block_read_error(0, 1023, 1),);
    }

    /// Test that reading past the last block of the file fails.
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_read_from_block_past_file_end() {
        let fs = load_test_disk1().await;
        let mut dst = vec![0; 1024];
        let err = fs
            .read_from_block(999_999_999, 0, &mut dst)
            .await
            .unwrap_err();
        assert_eq!(err, block_read_error(999_999_999, 0, 1024),);
    }

    /// Test that reading at an offset >= the block size fails.
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_read_from_block_invalid_offset() {
        let fs = load_test_disk1().await;
        let mut dst = vec![0; 1024];
        let err = fs.read_from_block(1, 1024, &mut dst).await.unwrap_err();
        assert_eq!(err, block_read_error(1, 1024, 1024),);
    }

    /// Test that reading past the end of the block fails.
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_read_from_block_past_block_end() {
        let fs = load_test_disk1().await;
        let mut dst = vec![0; 25];
        let err = fs.read_from_block(1, 1000, &mut dst).await.unwrap_err();
        assert_eq!(err, block_read_error(1, 1000, 25),);
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_path_to_inode() {
        let fs = load_test_disk1().await;

        let follow = FollowSymlinks::All;

        let inode = fs
            .path_to_inode(Path::try_from("/").unwrap(), follow)
            .await
            .unwrap();
        assert_eq!(inode.index.get(), 2);

        // Successful lookup.
        let res = fs
            .path_to_inode(Path::try_from("/empty_file").unwrap(), follow)
            .await;
        assert!(res.is_ok());

        // Successful lookup with a "." component.
        let res = fs
            .path_to_inode(Path::try_from("/./empty_file").unwrap(), follow)
            .await;
        assert!(res.is_ok());

        // Successful lookup with a ".." component.
        let inode = fs
            .path_to_inode(Path::try_from("/empty_dir/..").unwrap(), follow)
            .await
            .unwrap();
        assert_eq!(inode.index.get(), 2);

        // Successful lookup with symlink.
        let res = fs
            .path_to_inode(Path::try_from("/sym_simple").unwrap(), follow)
            .await;
        assert!(res.is_ok());

        // Error: not an absolute path.
        let res = fs
            .path_to_inode(Path::try_from("empty_file").unwrap(), follow)
            .await;
        assert!(res.is_err());

        // Error: invalid child of a valid directory.
        let res = fs
            .path_to_inode(
                Path::try_from("/empty_dir/does_not_exist").unwrap(),
                follow,
            )
            .await;
        assert!(res.is_err());

        // Error: attempted to lookup child of a regular file.
        let res = fs
            .path_to_inode(
                Path::try_from("/empty_file/does_not_exist").unwrap(),
                follow,
            )
            .await;
        assert!(res.is_err());

        // TODO: add deeper paths to the test disk and test here.
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_inode_equivalence() {
        let fs = load_test_disk1().await;

        let mut inode = fs
            .path_to_inode(
                Path::try_from("/empty_file").unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        let data = inode.inode_data.clone();
        inode.update_inode_data(&fs);
        assert_eq!(inode.inode_data, data);
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_block_modification() {
        // Modify a block and check that the change is visible when reading the block again.
        let fs = load_test_disk1_rw_no_fsck().await;
        let block_index = 100;
        let offset_within_block = 0;
        let mut data = vec![5; 4];
        fs.write_to_block(block_index, offset_within_block, &data)
            .await
            .unwrap();
        fs.read_from_block(block_index, offset_within_block, &mut data)
            .await
            .unwrap();
        assert_eq!(data, [5; 4]);
    }
}
