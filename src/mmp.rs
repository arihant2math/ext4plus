use crate::Ext4;
use crate::checksum::Checksum;
use crate::error::{CorruptKind, Ext4Error};
use crate::util::{read_u16le, read_u32le};

#[derive(Debug, Clone)]
#[cfg_attr(not(all(test, feature = "std")), expect(dead_code))]
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

    #[cfg_attr(not(all(test, feature = "std")), expect(dead_code))]
    pub(crate) fn check_interval(&self) -> u16 {
        self.check_interval
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::error::CorruptKind;
    use crate::test_util::load_test_disk1;

    fn make_mmp_block(ext4: &Ext4) -> [u8; 1024] {
        let mut data = [0u8; 1024];
        data[0x0..0x4].copy_from_slice(&0x004D4D50u32.to_le_bytes());
        data[0x4..0x8].copy_from_slice(&7u32.to_le_bytes());
        data[0x8..0x10].copy_from_slice(&123u64.to_le_bytes());
        data[0x10..0x18].copy_from_slice(b"node-123");
        data[0x50..0x58].copy_from_slice(b"disk-xyz");
        data[0x70..0x72].copy_from_slice(&11u16.to_le_bytes());
        data[0x72..0x74].copy_from_slice(&12u16.to_le_bytes());

        for (i, chunk) in data[0x74..0x3FC].chunks_exact_mut(4).enumerate() {
            chunk.copy_from_slice(&(i as u32).to_le_bytes());
        }

        let checksum = if ext4.has_metadata_checksums() {
            let mut checksum = Checksum::new();
            checksum.update(ext4.0.superblock.uuid().as_bytes());
            checksum.update(&data[..0x3FC]);
            checksum.finalize()
        } else {
            0xDEADBEEF
        };
        data[0x3FC..0x400].copy_from_slice(&checksum.to_le_bytes());

        data
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_mmp_from_bytes_success() {
        let ext4 = load_test_disk1().await;
        let data = make_mmp_block(&ext4);

        let mmp = Mmp::from_bytes(&ext4, &data).unwrap();

        assert_eq!(mmp.magic, 0x004D4D50);
        assert_eq!(mmp.seq, 7);
        assert_eq!(mmp.time, 123);
        assert_eq!(&mmp.nodename[..8], b"node-123");
        assert_eq!(&mmp.bdevname[..8], b"disk-xyz");
        assert_eq!(mmp.check_interval(), 11);
        assert_eq!(mmp.pad1, 12);
        assert_eq!(mmp.pad2[0], 0);
        assert_eq!(mmp.pad2[225], 225);
        assert_eq!(mmp.checksum, read_u32le(&data, 0x3FC));
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_mmp_from_bytes_errors() {
        let ext4 = load_test_disk1().await;

        assert_eq!(
            Mmp::from_bytes(&ext4, &[0; 16]).unwrap_err(),
            CorruptKind::MmpMagic
        );

        let mut bad_magic = make_mmp_block(&ext4);
        bad_magic[0x0..0x4].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(
            Mmp::from_bytes(&ext4, &bad_magic).unwrap_err(),
            CorruptKind::MmpMagic
        );

        let mut bad_checksum = make_mmp_block(&ext4);
        bad_checksum[0x3FC..0x400].copy_from_slice(&0u32.to_le_bytes());
        if ext4.has_metadata_checksums() {
            assert_eq!(
                Mmp::from_bytes(&ext4, &bad_checksum).unwrap_err(),
                CorruptKind::MmpChecksum
            );
        } else {
            assert_eq!(
                Mmp::from_bytes(&ext4, &bad_checksum).unwrap().checksum,
                0
            );
        }
    }
}
