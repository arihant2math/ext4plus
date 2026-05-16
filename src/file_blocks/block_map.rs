use super::utils::{
    add_to_file_offset, file_block_from_offset, offset_in_block_usize,
};
use crate::block_index::{FileBlockIndex, FsBlockIndex};
use crate::util::{read_u32le, u64_from_usize, usize_from_u32};
use crate::{Ext4, Ext4Error, Inode};

use crate::error::CorruptKind;
use alloc::vec;
use alloc::vec::Vec;
use core::marker::PhantomData;
use core::num::{NonZeroU32, NonZeroUsize};

const DIRECT_BLOCKS: usize = 12;

struct BlockMapLayout {
    blocks_per_block: NonZeroUsize,
    blocks_per_double_indirect: NonZeroUsize,
    single_indirect_limit: usize,
    double_indirect_limit: usize,
    triple_indirect_limit: usize,
}

fn get_block_map_layout(fs: &Ext4) -> Result<BlockMapLayout, Ext4Error> {
    let blocks_per_block =
        NonZeroUsize::new(fs.0.superblock.block_size().to_usize() / 4)
            .ok_or(CorruptKind::InvalidBlockSize)?;
    let blocks_per_double_indirect = blocks_per_block
        .checked_mul(blocks_per_block)
        .ok_or(Ext4Error::FileTooLarge)?;
    let blocks_per_triple_indirect = blocks_per_double_indirect
        .checked_mul(blocks_per_block)
        .ok_or(Ext4Error::FileTooLarge)?;
    let single_indirect_limit = DIRECT_BLOCKS
        .checked_add(blocks_per_block.get())
        .ok_or(Ext4Error::FileTooLarge)?;
    let double_indirect_limit = single_indirect_limit
        .checked_add(blocks_per_double_indirect.get())
        .ok_or(Ext4Error::FileTooLarge)?;
    let triple_indirect_limit = double_indirect_limit
        .checked_add(blocks_per_triple_indirect.get())
        .ok_or(Ext4Error::FileTooLarge)?;

    Ok(BlockMapLayout {
        blocks_per_block,
        blocks_per_double_indirect,
        single_indirect_limit,
        double_indirect_limit,
        triple_indirect_limit,
    })
}

fn block_index_to_u32(block_index: FsBlockIndex) -> Result<u32, Ext4Error> {
    u32::try_from(block_index).map_err(|_| Ext4Error::FileTooLarge)
}

trait BlockMapEntry {
    fn from_index(block_index: BlockIndex) -> Self;
}

#[derive(Copy, Clone, Debug)]
struct BlockIndex(u32);

impl BlockIndex {
    fn value(&self) -> u32 {
        self.0
    }
}

impl BlockMapEntry for BlockIndex {
    fn from_index(block_index: BlockIndex) -> Self {
        block_index
    }
}

#[derive(Copy, Clone)]
struct IndirectBlock<T: BlockMapEntry> {
    block_index: BlockIndex,
    phantom_data: PhantomData<T>,
}

impl<T: BlockMapEntry> BlockMapEntry for IndirectBlock<T> {
    fn from_index(block_index: BlockIndex) -> Self {
        Self::new(block_index)
    }
}

impl<T: BlockMapEntry> IndirectBlock<T> {
    fn new(block_index: BlockIndex) -> Self {
        Self {
            block_index,
            phantom_data: PhantomData,
        }
    }

    #[maybe_async::maybe_async]
    async fn entries(&self, fs: &Ext4) -> Result<Vec<BlockIndex>, Ext4Error> {
        if self.block_index.value() == 0 {
            return Ok(Vec::new());
        }

        let block_data = fs.read_block(u64::from(self.block_index.0)).await?;
        let mut entries = Vec::with_capacity(block_data.len() / 4);
        for offset in (0..block_data.len()).step_by(4) {
            entries.push(BlockIndex(read_u32le(&block_data, offset)));
        }
        Ok(entries)
    }

    #[maybe_async::maybe_async]
    async fn get(&self, index: usize, fs: &Ext4) -> Result<T, Ext4Error> {
        let block_data = fs.read_block(u64::from(self.block_index.0)).await?;
        let entry_index = index
            .checked_mul(4)
            .ok_or(CorruptKind::BlockMap(self.block_index.value()))?;
        if entry_index >= block_data.len() {
            return Err(CorruptKind::BlockMap(self.block_index.value()))?;
        }
        let entry_block_index = read_u32le(&block_data, entry_index);
        Ok(T::from_index(BlockIndex(entry_block_index)))
    }

    #[maybe_async::maybe_async]
    async fn set(
        &mut self,
        index: usize,
        block_index: BlockIndex,
        fs: &Ext4,
    ) -> Result<(), Ext4Error> {
        let mut block_data =
            fs.read_block(u64::from(self.block_index.0)).await?;
        let entry_index = index
            .checked_mul(4)
            .ok_or(CorruptKind::BlockMap(self.block_index.value()))?;
        if entry_index >= block_data.len() {
            return Err(CorruptKind::BlockMap(self.block_index.value()))?;
        }
        block_data[entry_index
            ..entry_index
                .checked_add(4)
                .ok_or(CorruptKind::BlockMap(self.block_index.value()))?]
            .copy_from_slice(&block_index.value().to_le_bytes());
        fs.write_to_block(u64::from(self.block_index.0), 0, &block_data)
            .await?;
        Ok(())
    }
}

#[maybe_async::maybe_async]
async fn initialize_indirect_block(
    fs: &Ext4,
    block_index: FsBlockIndex,
) -> Result<(), Ext4Error> {
    let zeroes = vec![0; fs.0.superblock.block_size().to_usize()];
    fs.write_to_block(block_index, 0, &zeroes).await
}

#[maybe_async::maybe_async]
async fn ensure_allocated<T: BlockMapEntry>(
    block: &mut IndirectBlock<T>,
    allocated: &mut u32,
    fs: &Ext4,
) -> Result<(), Ext4Error> {
    if block.block_index.value() == 0 {
        let new_block_index = fs.alloc_block(NonZeroU32::MIN).await?;
        initialize_indirect_block(fs, new_block_index).await?;
        *allocated = allocated.checked_add(1).ok_or(Ext4Error::FileTooLarge)?;
        *block = IndirectBlock::new(BlockIndex(block_index_to_u32(
            new_block_index,
        )?));
    }
    Ok(())
}

pub(crate) struct BlockMap {
    fs: Ext4,
    direct_blocks: [u32; DIRECT_BLOCKS],
    single_indirect_block: IndirectBlock<BlockIndex>,
    double_indirect_block: IndirectBlock<IndirectBlock<BlockIndex>>,
    triple_indirect_block:
        IndirectBlock<IndirectBlock<IndirectBlock<BlockIndex>>>,
}

impl BlockMap {
    pub(crate) fn initialize(fs: Ext4) -> Self {
        Self {
            fs,
            direct_blocks: [0; DIRECT_BLOCKS],
            single_indirect_block: IndirectBlock::<BlockIndex>::new(
                BlockIndex(0),
            ),
            double_indirect_block: IndirectBlock::new(BlockIndex(0)),
            triple_indirect_block: IndirectBlock::new(BlockIndex(0)),
        }
    }

    pub(crate) fn from_inode(inode: &Inode, fs: Ext4) -> Self {
        let data = inode.inline_data();
        let mut direct_blocks = [0; DIRECT_BLOCKS];
        for (i, direct_block) in direct_blocks.iter_mut().enumerate() {
            *direct_block = read_u32le(&data, i.checked_mul(4).unwrap());
        }
        let single_indirect_block = read_u32le(&data, DIRECT_BLOCKS * 4);
        let double_indirect_block = read_u32le(&data, (DIRECT_BLOCKS + 1) * 4);
        let triple_indirect_block = read_u32le(&data, (DIRECT_BLOCKS + 2) * 4);
        Self {
            fs,
            direct_blocks,
            single_indirect_block: IndirectBlock::new(BlockIndex(
                single_indirect_block,
            )),
            double_indirect_block: IndirectBlock::new(BlockIndex(
                double_indirect_block,
            )),
            triple_indirect_block: IndirectBlock::new(BlockIndex(
                triple_indirect_block,
            )),
        }
    }

    pub(crate) fn to_bytes(&self) -> [u8; 15 * 4] {
        let mut data = [0; 15 * 4];
        for i in 0usize..12 {
            let start = i.checked_mul(4).unwrap();
            let end = i.checked_add(1).unwrap().checked_mul(4).unwrap();
            data[start..end]
                .copy_from_slice(&self.direct_blocks[i].to_le_bytes());
        }
        data[DIRECT_BLOCKS * 4..(DIRECT_BLOCKS + 1) * 4].copy_from_slice(
            &self.single_indirect_block.block_index.value().to_le_bytes(),
        );
        data[(DIRECT_BLOCKS + 1) * 4..(DIRECT_BLOCKS + 2) * 4].copy_from_slice(
            &self.double_indirect_block.block_index.value().to_le_bytes(),
        );
        data[(DIRECT_BLOCKS + 2) * 4..(DIRECT_BLOCKS + 3) * 4].copy_from_slice(
            &self.triple_indirect_block.block_index.value().to_le_bytes(),
        );
        data
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn get_block(
        &self,
        file_block_index: FileBlockIndex,
    ) -> Result<FsBlockIndex, Ext4Error> {
        let layout = get_block_map_layout(&self.fs)?;
        let file_block_index = usize_from_u32(file_block_index);

        if file_block_index < DIRECT_BLOCKS {
            Ok(u64::from(self.direct_blocks[file_block_index]))
        } else if file_block_index < layout.single_indirect_limit {
            if self.single_indirect_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let single_indirect_index = file_block_index
                .checked_sub(DIRECT_BLOCKS)
                .ok_or(Ext4Error::FileTooLarge)?;
            let block_index = self
                .single_indirect_block
                .get(single_indirect_index, &self.fs)
                .await?;
            Ok(u64::from(block_index.value()))
        } else if file_block_index < layout.double_indirect_limit {
            if self.double_indirect_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let double_indirect_index = file_block_index
                .checked_sub(layout.single_indirect_limit)
                .ok_or(Ext4Error::FileTooLarge)?;
            let first_level_index =
                double_indirect_index / layout.blocks_per_block;
            let second_level_index =
                double_indirect_index % layout.blocks_per_block;
            let first_level_block = self
                .double_indirect_block
                .get(first_level_index, &self.fs)
                .await?;
            if first_level_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let block_index =
                first_level_block.get(second_level_index, &self.fs).await?;
            Ok(u64::from(block_index.value()))
        } else if file_block_index < layout.triple_indirect_limit {
            if self.triple_indirect_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let triple_indirect_index = file_block_index
                .checked_sub(layout.double_indirect_limit)
                .ok_or(Ext4Error::FileTooLarge)?;
            let first_level_index =
                triple_indirect_index / layout.blocks_per_double_indirect;
            let second_level_index = (triple_indirect_index
                / layout.blocks_per_block)
                % layout.blocks_per_block;
            let third_level_index =
                triple_indirect_index % layout.blocks_per_block;
            let first_level_block = self
                .triple_indirect_block
                .get(first_level_index, &self.fs)
                .await?;
            if first_level_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let second_level_block =
                first_level_block.get(second_level_index, &self.fs).await?;
            if second_level_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let block_index =
                second_level_block.get(third_level_index, &self.fs).await?;
            Ok(u64::from(block_index.value()))
        } else {
            Err(Ext4Error::FileTooLarge)
        }
    }

    /// Set the mapping for a file block index to a filesystem block index, allocating
    /// any necessary metadata blocks. Returns the number of allocated metadata blocks.
    #[maybe_async::maybe_async]
    pub(crate) async fn set_block(
        &mut self,
        file_block_index: FileBlockIndex,
        fs_block_index: FsBlockIndex,
    ) -> Result<u32, Ext4Error> {
        let mut allocated_metadata_blocks: u32 = 0;
        let layout = get_block_map_layout(&self.fs)?;
        let file_block_index = usize_from_u32(file_block_index);
        let fs_block_index = block_index_to_u32(fs_block_index)?;

        if file_block_index < DIRECT_BLOCKS {
            self.direct_blocks[file_block_index] = fs_block_index;
        } else if file_block_index < layout.single_indirect_limit {
            let single_indirect_index = file_block_index
                .checked_sub(DIRECT_BLOCKS)
                .ok_or(Ext4Error::FileTooLarge)?;
            ensure_allocated(
                &mut self.single_indirect_block,
                &mut allocated_metadata_blocks,
                &self.fs,
            )
            .await?;
            self.single_indirect_block
                .set(
                    single_indirect_index,
                    BlockIndex(fs_block_index),
                    &self.fs,
                )
                .await?;
        } else if file_block_index < layout.double_indirect_limit {
            let double_indirect_index = file_block_index
                .checked_sub(layout.single_indirect_limit)
                .ok_or(Ext4Error::FileTooLarge)?;
            let first_level_index =
                double_indirect_index / layout.blocks_per_block;
            let second_level_index =
                double_indirect_index % layout.blocks_per_block;
            ensure_allocated(
                &mut self.double_indirect_block,
                &mut allocated_metadata_blocks,
                &self.fs,
            )
            .await?;
            let mut first_level_block = self
                .double_indirect_block
                .get(first_level_index, &self.fs)
                .await?;
            let mut needs_set = false;
            if first_level_block.block_index.value() == 0 {
                ensure_allocated(
                    &mut first_level_block,
                    &mut allocated_metadata_blocks,
                    &self.fs,
                )
                .await?;
                needs_set = true;
            }
            if needs_set {
                self.double_indirect_block
                    .set(
                        first_level_index,
                        first_level_block.block_index,
                        &self.fs,
                    )
                    .await?;
            }
            first_level_block
                .set(second_level_index, BlockIndex(fs_block_index), &self.fs)
                .await?;
        } else if file_block_index < layout.triple_indirect_limit {
            let triple_indirect_index = file_block_index
                .checked_sub(layout.double_indirect_limit)
                .ok_or(Ext4Error::FileTooLarge)?;
            let first_level_index =
                triple_indirect_index / layout.blocks_per_double_indirect;
            let second_level_index = (triple_indirect_index
                / layout.blocks_per_block)
                % layout.blocks_per_block;
            let third_level_index =
                triple_indirect_index % layout.blocks_per_block;
            ensure_allocated(
                &mut self.triple_indirect_block,
                &mut allocated_metadata_blocks,
                &self.fs,
            )
            .await?;
            let mut first_level_block = self
                .triple_indirect_block
                .get(first_level_index, &self.fs)
                .await?;
            let mut first_needs_set = false;
            if first_level_block.block_index.value() == 0 {
                ensure_allocated(
                    &mut first_level_block,
                    &mut allocated_metadata_blocks,
                    &self.fs,
                )
                .await?;
                first_needs_set = true;
            }
            if first_needs_set {
                self.triple_indirect_block
                    .set(
                        first_level_index,
                        first_level_block.block_index,
                        &self.fs,
                    )
                    .await?;
            }
            let mut second_level_block =
                first_level_block.get(second_level_index, &self.fs).await?;
            let mut second_needs_set = false;
            if second_level_block.block_index.value() == 0 {
                ensure_allocated(
                    &mut second_level_block,
                    &mut allocated_metadata_blocks,
                    &self.fs,
                )
                .await?;
                second_needs_set = true;
            }
            if second_needs_set {
                first_level_block
                    .set(
                        second_level_index,
                        second_level_block.block_index,
                        &self.fs,
                    )
                    .await?;
            }
            second_level_block
                .set(third_level_index, BlockIndex(fs_block_index), &self.fs)
                .await?;
        } else {
            return Err(Ext4Error::FileTooLarge);
        }
        Ok(allocated_metadata_blocks)
    }

    /// Clear a range of file blocks from the mapping and return the corresponding
    /// allocated filesystem blocks that were removed.
    #[maybe_async::maybe_async]
    pub(crate) async fn remove_range(
        &mut self,
        start: FileBlockIndex,
        count: u32,
    ) -> Result<Vec<FsBlockIndex>, Ext4Error> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let end_usize =
            start.checked_add(count).ok_or(Ext4Error::FileTooLarge)?;
        let mut removed_blocks = Vec::with_capacity(usize_from_u32(count));

        for i in start..end_usize {
            let block = self.get_block(i).await?;
            if block != 0 {
                removed_blocks.push(block);
            }
            self.set_block(i, 0).await?;
        }
        Ok(removed_blocks)
    }

    #[maybe_async::maybe_async]
    async fn collect_data_and_metadata_blocks(
        &self,
    ) -> Result<(Vec<FsBlockIndex>, Vec<FsBlockIndex>), Ext4Error> {
        let mut data_blocks = self
            .direct_blocks
            .iter()
            .copied()
            .filter(|block| *block != 0)
            .map(FsBlockIndex::from)
            .collect::<Vec<_>>();
        let mut metadata_blocks = Vec::new();

        if self.single_indirect_block.block_index.value() != 0 {
            metadata_blocks.push(FsBlockIndex::from(
                self.single_indirect_block.block_index.value(),
            ));
            for block in self.single_indirect_block.entries(&self.fs).await? {
                if block.value() != 0 {
                    data_blocks.push(FsBlockIndex::from(block.value()));
                }
            }
        }

        if self.double_indirect_block.block_index.value() != 0 {
            metadata_blocks.push(FsBlockIndex::from(
                self.double_indirect_block.block_index.value(),
            ));
            for block in self.double_indirect_block.entries(&self.fs).await? {
                if block.value() == 0 {
                    continue;
                }

                metadata_blocks.push(FsBlockIndex::from(block.value()));
                let indirect_block = IndirectBlock::<BlockIndex>::new(block);
                for data_block in indirect_block.entries(&self.fs).await? {
                    if data_block.value() != 0 {
                        data_blocks
                            .push(FsBlockIndex::from(data_block.value()));
                    }
                }
            }
        }

        if self.triple_indirect_block.block_index.value() != 0 {
            metadata_blocks.push(FsBlockIndex::from(
                self.triple_indirect_block.block_index.value(),
            ));
            for double_indirect in
                self.triple_indirect_block.entries(&self.fs).await?
            {
                if double_indirect.value() == 0 {
                    continue;
                }

                metadata_blocks
                    .push(FsBlockIndex::from(double_indirect.value()));
                let double_indirect_block =
                    IndirectBlock::<IndirectBlock<BlockIndex>>::new(
                        double_indirect,
                    );
                for indirect in double_indirect_block.entries(&self.fs).await? {
                    if indirect.value() == 0 {
                        continue;
                    }

                    metadata_blocks.push(FsBlockIndex::from(indirect.value()));
                    let indirect_block =
                        IndirectBlock::<BlockIndex>::new(indirect);
                    for data_block in indirect_block.entries(&self.fs).await? {
                        if data_block.value() != 0 {
                            data_blocks
                                .push(FsBlockIndex::from(data_block.value()));
                        }
                    }
                }
            }
        }

        Ok((data_blocks, metadata_blocks))
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn free_all(&self) -> Result<(), Ext4Error> {
        let (data_blocks, metadata_blocks) =
            self.collect_data_and_metadata_blocks().await?;

        for block in data_blocks {
            self.fs.free_block(block).await?;
        }
        for block in metadata_blocks {
            self.fs.free_block(block).await?;
        }
        Ok(())
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn write_at(
        &mut self,
        inode: &mut Inode,
        buf: &[u8],
        offset: u64,
    ) -> Result<usize, Ext4Error> {
        let block_size = self.fs.0.superblock.block_size();
        let block_size_u64 = block_size.to_u64();
        let block_size_usize = block_size.to_usize();
        let start_block = file_block_from_offset(offset, block_size_u64)?;
        let offset_in_block = offset_in_block_usize(offset, block_size_u64)?;
        let remaining_in_block = block_size_usize
            .checked_sub(offset_in_block)
            .ok_or(CorruptKind::InvalidBlockSize)?;

        if remaining_in_block > 0 {
            let to_write = core::cmp::min(buf.len(), remaining_in_block);
            let new_size = add_to_file_offset(offset, to_write)?;
            let fs_block = match self.get_block(start_block).await? {
                0 => {
                    let new_fs_block = self.fs.alloc_block(inode.index).await?;
                    let metadata_blocks =
                        self.set_block(start_block, new_fs_block).await?;
                    let new_fs_blocks = inode
                        .fs_blocks(&self.fs)?
                        .checked_add(1)
                        .ok_or(Ext4Error::FileTooLarge)?
                        .checked_add(u64::from(metadata_blocks))
                        .ok_or(Ext4Error::FileTooLarge)?;
                    inode.set_fs_blocks(new_fs_blocks, &self.fs)?;
                    inode.set_inline_data(self.to_bytes());
                    inode.write(&self.fs).await?;
                    new_fs_block
                }
                fs_block => fs_block,
            };
            self.fs
                .write_to_block(
                    fs_block,
                    u32::try_from(offset_in_block)
                        .map_err(|_| CorruptKind::InvalidBlockSize)?,
                    &buf[..to_write],
                )
                .await?;
            if new_size > inode.size_in_bytes() {
                inode.set_size_in_bytes(new_size);
                inode.write(&self.fs).await?;
            }
            Ok(to_write)
        } else {
            let to_write = core::cmp::min(buf.len(), block_size_usize);
            let new_size = add_to_file_offset(offset, to_write)?;
            let next_block =
                start_block.checked_add(1).ok_or(Ext4Error::FileTooLarge)?;
            let fs_block = match self.get_block(next_block).await? {
                0 => {
                    let new_fs_block = self.fs.alloc_block(inode.index).await?;
                    let metadata_blocks =
                        self.set_block(next_block, new_fs_block).await?;
                    let new_fs_blocks = inode
                        .fs_blocks(&self.fs)?
                        .checked_add(1)
                        .ok_or(Ext4Error::FileTooLarge)?
                        .checked_add(u64::from(metadata_blocks))
                        .ok_or(Ext4Error::FileTooLarge)?;
                    inode.set_fs_blocks(new_fs_blocks, &self.fs)?;
                    inode.set_inline_data(self.to_bytes());
                    inode.write(&self.fs).await?;
                    new_fs_block
                }
                fs_block => fs_block,
            };
            let write_buf = &buf[..to_write];
            self.fs.write_to_block(fs_block, 0, write_buf).await?;
            if new_size > inode.size_in_bytes() {
                inode.set_size_in_bytes(new_size);
                inode.write(&self.fs).await?;
            }
            Ok(write_buf.len())
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn truncate(
        &mut self,
        inode: &mut Inode,
        new_size: u64,
    ) -> Result<(), Ext4Error> {
        let old_size = inode.size_in_bytes();
        if new_size == old_size {
            return Ok(());
        }

        if new_size > old_size {
            inode.set_size_in_bytes(new_size);
            inode.write(&self.fs).await?;
            return Ok(());
        }

        let block_size_u64 = self.fs.0.superblock.block_size().to_u64();
        let old_blocks = old_size.div_ceil(block_size_u64);
        let new_blocks = new_size.div_ceil(block_size_u64);

        if new_blocks < old_blocks {
            let drop_from = FileBlockIndex::try_from(new_blocks)
                .map_err(|_| Ext4Error::FileTooLarge)?;
            let drop_count = u32::try_from(
                old_blocks
                    .checked_sub(new_blocks)
                    .ok_or(Ext4Error::FileTooLarge)?,
            )
            .map_err(|_| Ext4Error::FileTooLarge)?;

            let freed = self.remove_range(drop_from, drop_count).await?;
            let fs_blocks = inode
                .fs_blocks(&self.fs)?
                .checked_sub(u64_from_usize(freed.len()))
                .ok_or(CorruptKind::InodeBlockCount(inode.index))?;
            inode.set_fs_blocks(fs_blocks, &self.fs)?;
            inode.set_inline_data(self.to_bytes());
            for blk in freed {
                if blk != 0 {
                    self.fs.free_block(blk).await?;
                }
            }
        }

        inode.set_size_in_bytes(new_size);
        inode.write(&self.fs).await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::inode::InodeIndex;
    use crate::test_util::load_compressed_filesystem_rw;
    use crate::{FileType, InodeCreationOptions, InodeFlags, InodeMode};

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_initialize_indirect_block_zeroes_contents() {
        let (fs, _) =
            load_compressed_filesystem_rw("test_disk_ext2.bin.zst").await;
        let block = fs.alloc_block(InodeIndex::new(2).unwrap()).await.unwrap();

        let garbage = vec![0xa5; fs.0.superblock.block_size().to_usize()];
        fs.write_to_block(block, 0, &garbage).await.unwrap();

        initialize_indirect_block(&fs, block).await.unwrap();

        let block_data = fs.read_block(block).await.unwrap();
        assert!(block_data.iter().all(|&byte| byte == 0));
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_free_all_frees_indirect_metadata() {
        let (fs, _) =
            load_compressed_filesystem_rw("test_disk_ext2.bin.zst").await;
        let mut inode = fs
            .create_inode(InodeCreationOptions {
                file_type: FileType::Regular,
                mode: InodeMode::S_IFREG
                    | InodeMode::S_IRUSR
                    | InodeMode::S_IWUSR,
                uid: 0,
                gid: 0,
                time: Default::default(),
                flags: InodeFlags::empty(),
            })
            .await
            .unwrap();
        let mut block_map = BlockMap::from_inode(&inode, fs.clone());
        let block_size = fs.0.superblock.block_size();
        let block_size_u64 = block_size.to_u64();
        let data = vec![0xa5; block_size.to_usize()];
        let free_blocks_before = fs.0.superblock.free_blocks_count();

        for i in 0..13u64 {
            block_map
                .write_at(
                    &mut inode,
                    &data,
                    i.checked_mul(block_size_u64).unwrap(),
                )
                .await
                .unwrap();
        }

        let free_blocks_after_write = fs.0.superblock.free_blocks_count();
        assert_eq!(free_blocks_before - free_blocks_after_write, 14);
        assert_ne!(block_map.single_indirect_block.block_index.value(), 0);

        block_map.free_all().await.unwrap();

        assert_eq!(fs.0.superblock.free_blocks_count(), free_blocks_before);
    }
}
