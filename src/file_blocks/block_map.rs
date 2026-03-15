use crate::block_index::{FileBlockIndex, FsBlockIndex};
use crate::util::{read_u32le, usize_from_u32};
use crate::{Ext4, Ext4Error, Inode};

use alloc::vec::Vec;
use core::marker::PhantomData;
use core::num::{NonZeroU32, NonZeroUsize};

const DIRECT_BLOCKS: usize = 12;

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
    async fn get(&self, index: usize, fs: &Ext4) -> Result<T, Ext4Error> {
        let block_data = fs.read_block(u64::from(self.block_index.0)).await?;
        let entry_index = index.checked_mul(4).unwrap();
        if entry_index >= block_data.len() {
            todo!(
                "Handle out-of-bounds access for indirect block index {}",
                index
            );
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
        let entry_index = index.checked_mul(4).unwrap();
        if entry_index >= block_data.len() {
            todo!(
                "Handle out-of-bounds access for indirect block index {}",
                index
            );
        }
        block_data[entry_index..entry_index.checked_add(4).unwrap()]
            .copy_from_slice(&block_index.value().to_le_bytes());
        fs.write_to_block(u64::from(self.block_index.0), 0, &block_data)
            .await?;
        Ok(())
    }
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
        let single_indirect_block =
            read_u32le(&data, DIRECT_BLOCKS.checked_mul(4).unwrap());
        let double_indirect_block = read_u32le(
            &data,
            (DIRECT_BLOCKS.checked_add(1).unwrap())
                .checked_mul(4)
                .unwrap(),
        );
        let triple_indirect_block = read_u32le(
            &data,
            (DIRECT_BLOCKS.checked_add(2).unwrap())
                .checked_mul(4)
                .unwrap(),
        );
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
        data[DIRECT_BLOCKS.checked_mul(4).unwrap()
            ..DIRECT_BLOCKS
                .checked_add(1)
                .unwrap()
                .checked_mul(4)
                .unwrap()]
            .copy_from_slice(
                &self.single_indirect_block.block_index.value().to_le_bytes(),
            );
        data[(DIRECT_BLOCKS.checked_add(1).unwrap())
            .checked_mul(4)
            .unwrap()
            ..DIRECT_BLOCKS
                .checked_add(2)
                .unwrap()
                .checked_mul(4)
                .unwrap()]
            .copy_from_slice(
                &self.double_indirect_block.block_index.value().to_le_bytes(),
            );
        data[(DIRECT_BLOCKS.checked_add(2).unwrap())
            .checked_mul(4)
            .unwrap()
            ..DIRECT_BLOCKS
                .checked_add(3)
                .unwrap()
                .checked_mul(4)
                .unwrap()]
            .copy_from_slice(
                &self.triple_indirect_block.block_index.value().to_le_bytes(),
            );
        data
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn get_block(
        &self,
        file_block_index: FileBlockIndex,
    ) -> Result<FsBlockIndex, Ext4Error> {
        let blocks_per_block =
            NonZeroUsize::new(self.fs.0.superblock.block_size().to_usize() / 4)
                .unwrap();
        if usize_from_u32(file_block_index) < DIRECT_BLOCKS {
            Ok(u64::from(
                self.direct_blocks[usize_from_u32(file_block_index)],
            ))
        } else if usize_from_u32(file_block_index)
            < DIRECT_BLOCKS.checked_add(blocks_per_block.get()).unwrap()
        {
            if self.single_indirect_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let single_indirect_index = usize_from_u32(file_block_index)
                .checked_sub(DIRECT_BLOCKS)
                .unwrap();
            let block_index = self
                .single_indirect_block
                .get(single_indirect_index, &self.fs)
                .await?;
            Ok(u64::from(block_index.value()))
        } else if usize_from_u32(file_block_index)
            < DIRECT_BLOCKS
                .checked_add(blocks_per_block.get())
                .unwrap()
                .checked_add(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap()
        {
            if self.double_indirect_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let double_indirect_index = usize_from_u32(file_block_index)
                .checked_sub(DIRECT_BLOCKS)
                .unwrap()
                .checked_sub(blocks_per_block.get())
                .unwrap();
            let first_level_index = double_indirect_index / blocks_per_block;
            let second_level_index = double_indirect_index % blocks_per_block;
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
        } else if usize_from_u32(file_block_index)
            < DIRECT_BLOCKS
                .checked_add(blocks_per_block.get())
                .unwrap()
                .checked_add(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap()
                .checked_add(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap()
        {
            if self.triple_indirect_block.block_index.value() == 0 {
                return Ok(0); // TODO: Should error
            }
            let triple_indirect_index = usize_from_u32(file_block_index)
                .checked_sub(DIRECT_BLOCKS)
                .unwrap()
                .checked_sub(blocks_per_block.get())
                .unwrap()
                .checked_sub(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap();
            let first_level_index = triple_indirect_index
                / (blocks_per_block.checked_mul(blocks_per_block).unwrap());
            let second_level_index =
                (triple_indirect_index / blocks_per_block) % blocks_per_block;
            let third_level_index = triple_indirect_index % blocks_per_block;
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
            // TODO: proper error
            Err(Ext4Error::FileTooLarge)
        }
    }

    #[maybe_async::maybe_async]
    pub(crate) async fn set_block(
        &mut self,
        file_block_index: FileBlockIndex,
        fs_block_index: FsBlockIndex,
    ) -> Result<(), Ext4Error> {
        let blocks_per_block =
            NonZeroUsize::new(self.fs.0.superblock.block_size().to_usize() / 4)
                .unwrap();

        if usize_from_u32(file_block_index) < DIRECT_BLOCKS {
            self.direct_blocks[usize_from_u32(file_block_index)] =
                u32::try_from(fs_block_index).unwrap();
        } else if usize_from_u32(file_block_index)
            < DIRECT_BLOCKS.checked_add(blocks_per_block.get()).unwrap()
        {
            let single_indirect_index = usize_from_u32(file_block_index)
                .checked_sub(DIRECT_BLOCKS)
                .unwrap();
            if self.single_indirect_block.block_index.value() == 0 {
                // TODO: make block allocation u32 but actually where the inode is
                let new_block_index =
                    self.fs.alloc_block(NonZeroU32::new(1).unwrap()).await?;
                self.single_indirect_block = IndirectBlock::new(BlockIndex(
                    u32::try_from(new_block_index).unwrap(),
                ));
            }
            self.single_indirect_block
                .set(
                    single_indirect_index,
                    BlockIndex(u32::try_from(fs_block_index).unwrap()),
                    &self.fs,
                )
                .await?;
        } else if usize_from_u32(file_block_index)
            < DIRECT_BLOCKS
                .checked_add(blocks_per_block.get())
                .unwrap()
                .checked_add(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap()
        {
            let double_indirect_index = usize_from_u32(file_block_index)
                .checked_sub(DIRECT_BLOCKS)
                .unwrap()
                .checked_sub(blocks_per_block.get())
                .unwrap();
            let first_level_index = double_indirect_index / blocks_per_block;
            let second_level_index = double_indirect_index % blocks_per_block;
            if self.double_indirect_block.block_index.value() == 0 {
                let new_block_index =
                    self.fs.alloc_block(NonZeroU32::new(1).unwrap()).await?;
                self.double_indirect_block = IndirectBlock::new(BlockIndex(
                    u32::try_from(new_block_index).unwrap(),
                ));
            }
            let mut first_level_block = self
                .double_indirect_block
                .get(first_level_index, &self.fs)
                .await?;
            if first_level_block.block_index.value() == 0 {
                let new_block_index =
                    self.fs.alloc_block(NonZeroU32::new(1).unwrap()).await?;
                first_level_block = IndirectBlock::new(BlockIndex(
                    u32::try_from(new_block_index).unwrap(),
                ));
                self.double_indirect_block
                    .set(
                        first_level_index,
                        first_level_block.block_index,
                        &self.fs,
                    )
                    .await?;
            }
            first_level_block
                .set(
                    second_level_index,
                    BlockIndex(u32::try_from(fs_block_index).unwrap()),
                    &self.fs,
                )
                .await?;
        } else if usize_from_u32(file_block_index)
            < DIRECT_BLOCKS
                .checked_add(blocks_per_block.get())
                .unwrap()
                .checked_add(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap()
                .checked_add(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap()
        {
            let triple_indirect_index = usize_from_u32(file_block_index)
                .checked_sub(DIRECT_BLOCKS)
                .unwrap()
                .checked_sub(blocks_per_block.get())
                .unwrap()
                .checked_sub(
                    blocks_per_block
                        .checked_mul(blocks_per_block)
                        .unwrap()
                        .get(),
                )
                .unwrap();
            let first_level_index = triple_indirect_index
                / (blocks_per_block.checked_mul(blocks_per_block).unwrap());
            let second_level_index =
                (triple_indirect_index / blocks_per_block) % blocks_per_block;
            let third_level_index = triple_indirect_index % blocks_per_block;
            if self.triple_indirect_block.block_index.value() == 0 {
                let new_block_index =
                    self.fs.alloc_block(NonZeroU32::new(1).unwrap()).await?;
                self.triple_indirect_block = IndirectBlock::new(BlockIndex(
                    u32::try_from(new_block_index).unwrap(),
                ));
            }
            let mut first_level_block = self
                .triple_indirect_block
                .get(first_level_index, &self.fs)
                .await?;
            if first_level_block.block_index.value() == 0 {
                let new_block_index =
                    self.fs.alloc_block(NonZeroU32::new(1).unwrap()).await?;
                first_level_block = IndirectBlock::new(BlockIndex(
                    u32::try_from(new_block_index).unwrap(),
                ));
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
            if second_level_block.block_index.value() == 0 {
                let new_block_index =
                    self.fs.alloc_block(NonZeroU32::new(1).unwrap()).await?;
                second_level_block = IndirectBlock::new(BlockIndex(
                    u32::try_from(new_block_index).unwrap(),
                ));
                first_level_block
                    .set(
                        second_level_index,
                        second_level_block.block_index,
                        &self.fs,
                    )
                    .await?;
            }
            second_level_block
                .set(
                    third_level_index,
                    BlockIndex(u32::try_from(fs_block_index).unwrap()),
                    &self.fs,
                )
                .await?;
        } else {
            // TODO: proper error
            return Err(Ext4Error::FileTooLarge);
        }
        Ok(())
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

        let end_usize = start.checked_add(count).unwrap();
        let mut removed_blocks = Vec::with_capacity(usize_from_u32(count));

        for i in start..end_usize {
            let block =
                self.get_block(FileBlockIndex::try_from(i).unwrap()).await?;
            if block != 0 {
                removed_blocks.push(block);
            }
            self.set_block(FileBlockIndex::try_from(i).unwrap(), 0);
        }
        Ok(removed_blocks)
    }
}
