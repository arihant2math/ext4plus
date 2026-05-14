// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
//! Extended attribute support.

use crate::Ext4;
use crate::error::{CorruptKind, Ext4Error};
use crate::features::{CompatibleFeatures, FilesystemFeature};
use crate::inode::Inode;
use crate::util::{read_u16le, read_u32le, write_u16le, write_u32le};
use alloc::vec;
use alloc::vec::Vec;

const EXT4_XATTR_MAGIC: u32 = 0xea02_0000;
const EXT4_XATTR_ENTRY_BASE_SIZE: usize = 16;
const EXT4_XATTR_IBODY_HEADER_SIZE: usize = 4;
const EXT4_XATTR_BLOCK_HEADER_SIZE: usize = 32;
const EXT4_XATTR_USER: u8 = 1;
const EXT4_XATTR_POSIX_ACL_ACCESS: u8 = 2;
const EXT4_XATTR_POSIX_ACL_DEFAULT: u8 = 3;
const EXT4_XATTR_TRUSTED: u8 = 4;
const EXT4_XATTR_SECURITY: u8 = 6;
const EXT4_XATTR_SYSTEM: u8 = 7;

#[derive(Clone, Debug, Eq, PartialEq)]
struct XattrEntry {
    name_index: u8,
    name: Vec<u8>,
    value: Vec<u8>,
}

impl XattrEntry {
    fn full_name(&self, inode: Inode) -> Result<Vec<u8>, Ext4Error> {
        let prefix = match self.name_index {
            EXT4_XATTR_USER => b"user.".as_slice(),
            EXT4_XATTR_POSIX_ACL_ACCESS => {
                if !self.name.is_empty() {
                    return Err(CorruptKind::Xattr(inode.index).into());
                }
                return Ok(b"system.posix_acl_access".to_vec());
            }
            EXT4_XATTR_POSIX_ACL_DEFAULT => {
                if !self.name.is_empty() {
                    return Err(CorruptKind::Xattr(inode.index).into());
                }
                return Ok(b"system.posix_acl_default".to_vec());
            }
            EXT4_XATTR_TRUSTED => b"trusted.".as_slice(),
            EXT4_XATTR_SECURITY => b"security.".as_slice(),
            EXT4_XATTR_SYSTEM => b"system.".as_slice(),
            _ => return Err(CorruptKind::Xattr(inode.index).into()),
        };

        let mut full_name = Vec::with_capacity(
            prefix.len().checked_add(self.name.len()).unwrap(),
        );
        full_name.extend_from_slice(prefix);
        full_name.extend_from_slice(&self.name);
        Ok(full_name)
    }
}

fn align_4(size: usize) -> usize {
    size.checked_add(3).unwrap() & !3
}

fn xattr_body_start(inode: &Inode) -> usize {
    if inode.inode_data.len() < 0x80 + 2 {
        128
    } else {
        128 + usize::from(read_u16le(&inode.inode_data, 0x80))
    }
}

fn parse_xattr_name(name: &[u8]) -> Result<(u8, Vec<u8>), Ext4Error> {
    if name.contains(&0) {
        return Err(Ext4Error::InvalidXattrName);
    }

    if let Some(name) = name.strip_prefix(b"user.") {
        if name.is_empty() || name.len() > usize::from(u8::MAX) {
            return Err(Ext4Error::InvalidXattrName);
        }
        return Ok((EXT4_XATTR_USER, name.to_vec()));
    }

    if name == b"system.posix_acl_access" {
        return Ok((EXT4_XATTR_POSIX_ACL_ACCESS, Vec::new()));
    }

    if name == b"system.posix_acl_default" {
        return Ok((EXT4_XATTR_POSIX_ACL_DEFAULT, Vec::new()));
    }

    if let Some(name) = name.strip_prefix(b"trusted.") {
        if name.is_empty() || name.len() > usize::from(u8::MAX) {
            return Err(Ext4Error::InvalidXattrName);
        }
        return Ok((EXT4_XATTR_TRUSTED, name.to_vec()));
    }

    if let Some(name) = name.strip_prefix(b"security.") {
        if name.is_empty() || name.len() > usize::from(u8::MAX) {
            return Err(Ext4Error::InvalidXattrName);
        }
        return Ok((EXT4_XATTR_SECURITY, name.to_vec()));
    }

    if let Some(name) = name.strip_prefix(b"system.") {
        if name.is_empty() || name.len() > usize::from(u8::MAX) {
            return Err(Ext4Error::InvalidXattrName);
        }
        return Ok((EXT4_XATTR_SYSTEM, name.to_vec()));
    }

    Err(Ext4Error::InvalidXattrName)
}

fn parse_xattr_entries(
    inode: &Inode,
    storage: &[u8],
    entries_start: usize,
    value_base: usize,
) -> Result<Vec<XattrEntry>, Ext4Error> {
    let mut entries = Vec::new();
    let mut entry_offset = entries_start;

    loop {
        let sentinel = storage
            .get(entry_offset..entry_offset.checked_add(4).unwrap())
            .ok_or_else(|| Ext4Error::from(CorruptKind::Xattr(inode.index)))?;
        if sentinel == [0, 0, 0, 0] {
            break;
        }

        let entry = storage
            .get(
                entry_offset
                    ..entry_offset
                        .checked_add(EXT4_XATTR_ENTRY_BASE_SIZE)
                        .unwrap(),
            )
            .ok_or_else(|| Ext4Error::from(CorruptKind::Xattr(inode.index)))?;
        let name_len = usize::from(entry[0]);
        if name_len == 0 {
            break;
        }

        let record_len =
            align_4(EXT4_XATTR_ENTRY_BASE_SIZE.checked_add(name_len).unwrap());
        let entry = storage
            .get(entry_offset..entry_offset.checked_add(record_len).unwrap())
            .ok_or_else(|| Ext4Error::from(CorruptKind::Xattr(inode.index)))?;

        let value_offs = usize::from(read_u16le(entry, 2));
        let value_inum = read_u32le(entry, 4);
        let value_size = usize::try_from(read_u32le(entry, 8))
            .map_err(|_| Ext4Error::from(CorruptKind::Xattr(inode.index)))?;

        if value_inum != 0 {
            return Err(CorruptKind::Xattr(inode.index).into());
        }

        let value_start = value_base
            .checked_add(value_offs)
            .ok_or_else(|| Ext4Error::from(CorruptKind::Xattr(inode.index)))?;
        let value_end = value_start
            .checked_add(value_size)
            .ok_or_else(|| Ext4Error::from(CorruptKind::Xattr(inode.index)))?;

        let name = entry
            [EXT4_XATTR_ENTRY_BASE_SIZE..EXT4_XATTR_ENTRY_BASE_SIZE + name_len]
            .to_vec();
        let value = storage
            .get(value_start..value_end)
            .ok_or_else(|| Ext4Error::from(CorruptKind::Xattr(inode.index)))?
            .to_vec();

        entries.push(XattrEntry {
            name_index: entry[1],
            name,
            value,
        });

        entry_offset = entry_offset
            .checked_add(record_len)
            .ok_or_else(|| Ext4Error::from(CorruptKind::Xattr(inode.index)))?;
    }

    Ok(entries)
}

fn serialize_xattrs_to_ibody(
    storage_len: usize,
    entries: &[XattrEntry],
) -> Result<Vec<u8>, Ext4Error> {
    let mut storage = vec![0; storage_len];
    if entries.is_empty() {
        return Ok(storage);
    }

    if storage_len < EXT4_XATTR_IBODY_HEADER_SIZE + 4 {
        return Err(Ext4Error::NoSpace);
    }

    write_u32le(&mut storage, 0, EXT4_XATTR_MAGIC);

    let mut entry_offset = EXT4_XATTR_IBODY_HEADER_SIZE;
    let mut value_cursor = storage_len;

    for entry in entries {
        let record_len = align_4(
            EXT4_XATTR_ENTRY_BASE_SIZE
                .checked_add(entry.name.len())
                .unwrap(),
        );
        let value_len = entry.value.len();
        let value_padded_len = align_4(value_len);

        if entry.name.len() > usize::from(u8::MAX) {
            return Err(Ext4Error::InvalidXattrName);
        }

        let next_entry_offset = entry_offset
            .checked_add(record_len)
            .and_then(|v| v.checked_add(4))
            .ok_or(Ext4Error::NoSpace)?;

        if value_padded_len > value_cursor {
            return Err(Ext4Error::NoSpace);
        }
        value_cursor = value_cursor.checked_sub(value_padded_len).unwrap();

        if next_entry_offset > value_cursor {
            return Err(Ext4Error::NoSpace);
        }

        storage[entry_offset] = u8::try_from(entry.name.len()).unwrap();
        storage[entry_offset.checked_add(1).unwrap()] = entry.name_index;

        let value_offs = if value_len == 0 {
            0
        } else {
            u16::try_from(
                value_cursor
                    .checked_sub(EXT4_XATTR_IBODY_HEADER_SIZE)
                    .ok_or(Ext4Error::NoSpace)?,
            )
            .map_err(|_| Ext4Error::NoSpace)?
        };
        write_u16le(
            &mut storage,
            entry_offset.checked_add(2).unwrap(),
            value_offs,
        );
        write_u32le(&mut storage, entry_offset.checked_add(4).unwrap(), 0);
        write_u32le(
            &mut storage,
            entry_offset.checked_add(8).unwrap(),
            u32::try_from(value_len).map_err(|_| Ext4Error::NoSpace)?,
        );
        write_u32le(&mut storage, entry_offset.checked_add(12).unwrap(), 0);
        let name_offset = entry_offset
            .checked_add(EXT4_XATTR_ENTRY_BASE_SIZE)
            .unwrap();
        storage
            [name_offset..name_offset.checked_add(entry.name.len()).unwrap()]
            .copy_from_slice(&entry.name);

        if value_len != 0 {
            storage[value_cursor..value_cursor.checked_add(value_len).unwrap()]
                .copy_from_slice(&entry.value);
        }

        entry_offset = entry_offset.checked_add(record_len).unwrap();
    }

    Ok(storage)
}

impl Inode {
    #[maybe_async::maybe_async]
    async fn read_xattrs(
        &self,
        ext4: &Ext4,
    ) -> Result<Vec<XattrEntry>, Ext4Error> {
        if !ext4
            .0
            .superblock
            .compatible_features()
            .contains(CompatibleFeatures::EXT_ATTR)
        {
            return Err(Ext4Error::UnsupportedOperation(
                FilesystemFeature::Compatible(CompatibleFeatures::EXT_ATTR),
            ));
        }
        let mut entries = Vec::new();

        let ibody_start = xattr_body_start(self);
        if ibody_start < self.inode_data.len() {
            let ibody = &self.inode_data[ibody_start..];
            if ibody.len() >= EXT4_XATTR_IBODY_HEADER_SIZE {
                let magic = read_u32le(ibody, 0);
                if magic == 0 {
                    // No inode-body xattrs.
                } else if magic == EXT4_XATTR_MAGIC {
                    entries.extend(parse_xattr_entries(
                        self,
                        ibody,
                        EXT4_XATTR_IBODY_HEADER_SIZE,
                        EXT4_XATTR_IBODY_HEADER_SIZE,
                    )?);
                } else {
                    return Err(CorruptKind::Xattr(self.index).into());
                }
            }
        }

        let file_acl = self.file_acl();
        if file_acl != 0 {
            let block = ext4.read_block(file_acl).await?;
            if block.len() < EXT4_XATTR_BLOCK_HEADER_SIZE
                || read_u32le(&block, 0) != EXT4_XATTR_MAGIC
            {
                return Err(CorruptKind::Xattr(self.index).into());
            }
            entries.extend(parse_xattr_entries(
                self,
                &block,
                EXT4_XATTR_BLOCK_HEADER_SIZE,
                0,
            )?);
        }

        entries.sort_by(|a, b| {
            (a.name_index, a.name.as_slice())
                .cmp(&(b.name_index, b.name.as_slice()))
        });

        for window in entries.windows(2) {
            if window[0].name_index == window[1].name_index
                && window[0].name == window[1].name
            {
                return Err(CorruptKind::Xattr(self.index).into());
            }
        }

        Ok(entries)
    }

    /// List the inode's extended attribute names.
    #[maybe_async::maybe_async]
    pub async fn list_xattrs(
        &self,
        ext4: &Ext4,
    ) -> Result<Vec<Vec<u8>>, Ext4Error> {
        if !ext4
            .0
            .superblock
            .compatible_features()
            .contains(CompatibleFeatures::EXT_ATTR)
        {
            return Err(Ext4Error::UnsupportedOperation(
                FilesystemFeature::Compatible(CompatibleFeatures::EXT_ATTR),
            ));
        }
        let entries = self.read_xattrs(ext4).await?;
        entries
            .into_iter()
            .map(|entry| entry.full_name(self.clone()))
            .collect()
    }

    /// Get an extended attribute value from the inode.
    #[maybe_async::maybe_async]
    pub async fn get_xattr<N>(
        &self,
        ext4: &Ext4,
        name: N,
    ) -> Result<Option<Vec<u8>>, Ext4Error>
    where
        N: AsRef<[u8]>,
    {
        if !ext4
            .0
            .superblock
            .compatible_features()
            .contains(CompatibleFeatures::EXT_ATTR)
        {
            return Err(Ext4Error::UnsupportedOperation(
                FilesystemFeature::Compatible(CompatibleFeatures::EXT_ATTR),
            ));
        }
        let (name_index, name) = parse_xattr_name(name.as_ref())?;
        let entries = self.read_xattrs(ext4).await?;
        Ok(entries
            .into_iter()
            .find(|entry| {
                entry.name_index == name_index && entry.name.as_slice() == name
            })
            .map(|entry| entry.value))
    }

    /// Set an extended attribute on the inode.
    ///
    /// The attribute is replaced if it already exists.
    ///
    /// Currently, writes are limited to attribute sets that fit entirely
    /// inside the inode body. Existing external xattr blocks can still be
    /// read, and will be removed if all attributes fit inline after the
    /// update.
    #[maybe_async::maybe_async]
    pub async fn set_xattr<N, V>(
        &mut self,
        ext4: &Ext4,
        name: N,
        value: V,
    ) -> Result<(), Ext4Error>
    where
        N: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        if !ext4
            .0
            .superblock
            .compatible_features()
            .contains(CompatibleFeatures::EXT_ATTR)
        {
            return Err(Ext4Error::UnsupportedOperation(
                FilesystemFeature::Compatible(CompatibleFeatures::EXT_ATTR),
            ));
        }
        let (name_index, name) = parse_xattr_name(name.as_ref())?;
        let value = value.as_ref().to_vec();
        let mut entries = self.read_xattrs(ext4).await?;

        if let Some(entry) = entries.iter_mut().find(|entry| {
            entry.name_index == name_index && entry.name.as_slice() == name
        }) {
            entry.value = value;
        } else {
            entries.push(XattrEntry {
                name_index,
                name,
                value,
            });
        }

        entries.sort_by(|a, b| {
            (a.name_index, a.name.as_slice())
                .cmp(&(b.name_index, b.name.as_slice()))
        });

        let ibody_start = xattr_body_start(self);
        let ibody_len = self.inode_data.len().saturating_sub(ibody_start);
        let serialized = serialize_xattrs_to_ibody(ibody_len, &entries)?;

        if ibody_start < self.inode_data.len() {
            self.inode_data[ibody_start..].copy_from_slice(&serialized);
        } else if !serialized.is_empty() {
            return Err(Ext4Error::NoSpace);
        }

        let file_acl = self.file_acl();
        if file_acl != 0 {
            ext4.free_block(file_acl).await?;
            self.set_file_acl(0);
            let fs_blocks = self.fs_blocks(ext4)?;
            self.set_fs_blocks(fs_blocks.checked_sub(1).unwrap(), ext4)?;
        }

        self.write(ext4).await
    }

    /// Remove an extended attribute from the inode.
    #[maybe_async::maybe_async]
    pub async fn remove_xattr<N>(
        &mut self,
        ext4: &Ext4,
        name: N,
    ) -> Result<(), Ext4Error>
    where
        N: AsRef<[u8]>,
    {
        if !ext4
            .0
            .superblock
            .compatible_features()
            .contains(CompatibleFeatures::EXT_ATTR)
        {
            return Err(Ext4Error::UnsupportedOperation(
                FilesystemFeature::Compatible(CompatibleFeatures::EXT_ATTR),
            ));
        }
        let (name_index, name) = parse_xattr_name(name.as_ref())?;
        let mut entries = self.read_xattrs(ext4).await?;
        let original_len = entries.len();
        entries.retain(|entry| {
            !(entry.name_index == name_index && entry.name.as_slice() == name)
        });

        if entries.len() == original_len {
            return Err(Ext4Error::NotFound);
        }

        let ibody_start = xattr_body_start(self);
        let ibody_len = self.inode_data.len().saturating_sub(ibody_start);
        let serialized = serialize_xattrs_to_ibody(ibody_len, &entries)?;

        if ibody_start < self.inode_data.len() {
            self.inode_data[ibody_start..].copy_from_slice(&serialized);
        }

        let file_acl = self.file_acl();
        if file_acl != 0 {
            ext4.free_block(file_acl).await?;
            self.set_file_acl(0);
            let fs_blocks = self.fs_blocks(ext4)?;
            self.set_fs_blocks(fs_blocks.checked_sub(1).unwrap(), ext4)?;
        }

        self.write(ext4).await
    }
}

#[cfg(test)]
mod tests {
    use super::parse_xattr_name;
    use crate::error::Ext4Error;

    #[test]
    fn test_parse_xattr_name() {
        assert_eq!(
            parse_xattr_name(b"user.demo").unwrap(),
            (super::EXT4_XATTR_USER, b"demo".to_vec())
        );
        assert_eq!(
            parse_xattr_name(b"system.posix_acl_access").unwrap(),
            (super::EXT4_XATTR_POSIX_ACL_ACCESS, Vec::new())
        );
        assert!(matches!(
            parse_xattr_name(b"user."),
            Err(Ext4Error::InvalidXattrName)
        ));
        assert!(matches!(
            parse_xattr_name(b"invalid"),
            Err(Ext4Error::InvalidXattrName)
        ));
    }
}
