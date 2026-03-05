use crate::block_index::{FileBlockIndex, FsBlockIndex};
use crate::util::{read_u32le, usize_from_u32};
use crate::{Ext4Error, Inode};

const DIRECT_BLOCKS: usize = 12;

pub(crate) struct BlockMap {
    direct_blocks: [u32; DIRECT_BLOCKS],
    single_indirect_block: u32,
    double_indirect_block: u32,
    triple_indirect_block: u32,
}

impl BlockMap {
    pub(crate) fn initialize() -> Self {
        Self {
            direct_blocks: [0; DIRECT_BLOCKS],
            single_indirect_block: 0,
            double_indirect_block: 0,
            triple_indirect_block: 0,
        }
    }

    pub(crate) fn from_inode(inode: &Inode) -> Self {
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
            direct_blocks,
            single_indirect_block,
            double_indirect_block,
            triple_indirect_block,
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
            .copy_from_slice(&self.single_indirect_block.to_le_bytes());
        data[(DIRECT_BLOCKS.checked_add(1).unwrap())
            .checked_mul(4)
            .unwrap()
            ..DIRECT_BLOCKS
                .checked_add(2)
                .unwrap()
                .checked_mul(4)
                .unwrap()]
            .copy_from_slice(&self.double_indirect_block.to_le_bytes());
        data[(DIRECT_BLOCKS.checked_add(2).unwrap())
            .checked_mul(4)
            .unwrap()
            ..DIRECT_BLOCKS
                .checked_add(3)
                .unwrap()
                .checked_mul(4)
                .unwrap()]
            .copy_from_slice(&self.triple_indirect_block.to_le_bytes());
        data
    }

    pub(crate) fn get_block(
        &self,
        file_block_index: FileBlockIndex,
    ) -> Result<FsBlockIndex, Ext4Error> {
        if usize_from_u32(file_block_index) < DIRECT_BLOCKS {
            Ok(u64::from(
                self.direct_blocks[usize_from_u32(file_block_index)],
            ))
        } else {
            todo!(
                "Handle indirect blocks for file block index {}",
                file_block_index
            );
        }
    }

    pub(crate) fn set_block(
        &mut self,
        file_block_index: FileBlockIndex,
        fs_block_index: FsBlockIndex,
    ) {
        if usize_from_u32(file_block_index) < DIRECT_BLOCKS {
            self.direct_blocks[usize_from_u32(file_block_index)] =
                u32::try_from(fs_block_index).unwrap();
        } else {
            todo!(
                "Handle indirect blocks for file block index {}",
                file_block_index
            );
        }
    }
}
