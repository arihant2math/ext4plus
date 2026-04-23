use crate::Ext4;
use crate::checksum::Checksum;
use crate::error::{CorruptKind, Ext4Error};
use crate::util::{read_u16le, read_u32le};

#[derive(Debug, Clone)]
#[allow(unused)]
pub(crate) struct Mmp {
    magic: u32,
    seq: u32,
    time: u64,
    nodename: [u8; 64],
    bdevname: [u8; 32],
    check_interval: u16,
    pad1: u16,
    pad2: [u32; 226],
    checksum: u32,
}

impl Mmp {
    pub(crate) fn from_bytes(
        ext4: &Ext4,
        data: &[u8],
    ) -> Result<Self, Ext4Error> {
        if data.len() < 1024 {
            return Err(CorruptKind::MmpMagic.into());
        }

        let magic = read_u32le(data, 0x0);
        if magic != 0x004D4D50 {
            return Err(CorruptKind::MmpMagic.into());
        }

        let expected_checksum = read_u32le(data, 0x3FC);

        if ext4.has_metadata_checksums() {
            let mut checksum = Checksum::new();
            // Checksum calculated against FS UUID and the MMP block up to the checksum field
            checksum.update(ext4.0.superblock.uuid().as_bytes());
            checksum.update(&data[..0x3FC]);

            if checksum.finalize() != expected_checksum {
                return Err(CorruptKind::MmpChecksum.into());
            }
        }

        let seq = read_u32le(data, 0x4);
        let time = u64::from_le_bytes(data[0x8..0x10].try_into().unwrap());

        let mut nodename = [0u8; 64];
        nodename.copy_from_slice(&data[0x10..0x50]);

        let mut bdevname = [0u8; 32];
        bdevname.copy_from_slice(&data[0x50..0x70]);

        let check_interval = read_u16le(data, 0x70);
        let pad1 = read_u16le(data, 0x72);

        let mut pad2 = [0u32; 226];
        for (i, item) in pad2.iter_mut().enumerate() {
            *item = read_u32le(
                data,
                0x74usize.checked_add(i.checked_mul(4).unwrap()).unwrap(),
            );
        }

        Ok(Self {
            magic,
            seq,
            time,
            nodename,
            bdevname,
            check_interval,
            pad1,
            pad2,
            checksum: expected_checksum,
        })
    }

    #[expect(unused)]
    pub(crate) fn check_interval(&self) -> u16 {
        self.check_interval
    }
}
