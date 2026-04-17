// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
//! Interface used by [`crate::Ext4`] to read the filesystem data from a storage

use crate::error::BoxedError;
use alloc::boxed::Box;
use alloc::vec::Vec;
#[cfg(not(feature = "sync"))]
use async_trait::async_trait;

use crate::MemIoError;

/// Interface used by [`Ext4`] to read the filesystem data from a storage
/// file or device.
///
/// [`Ext4`]: crate::Ext4
#[cfg(not(feature = "multi-threaded"))]
#[maybe_async::maybe_async]
#[cfg_attr(not(feature = "sync"), async_trait(?Send))]
pub trait Ext4Read {
    /// Read bytes into `dst`, starting at `start_byte`.
    ///
    /// Exactly `dst.len()` bytes will be read; an error will be
    /// returned if there is not enough data to fill `dst`, or if the
    /// data cannot be read for any reason.
    async fn read(
        &self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), BoxedError>;
}

/// Interface used by [`Ext4`] to read the filesystem data from a storage
/// file or device.
///
/// [`Ext4`]: crate::Ext4
#[cfg(feature = "multi-threaded")]
#[maybe_async::maybe_async]
#[cfg_attr(not(feature = "sync"), async_trait)]
pub trait Ext4Read: Send + Sync {
    /// Read bytes into `dst`, starting at `start_byte`.
    ///
    /// Exactly `dst.len()` bytes will be read; an error will be
    /// returned if there is not enough data to fill `dst`, or if the
    /// data cannot be read for any reason.
    async fn read(
        &self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), BoxedError>;
}

#[cfg(feature = "multi-threaded")]
#[maybe_async::maybe_async]
#[cfg_attr(not(feature = "sync"), async_trait)]
impl<T> Ext4Read for alloc::sync::Arc<T>
where
    T: Ext4Read,
{
    async fn read(
        &self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), BoxedError> {
        (**self).read(start_byte, dst).await
    }
}

#[maybe_async::maybe_async]
#[cfg_attr(not(feature = "sync"), async_trait)]
impl Ext4Read for Vec<u8> {
    async fn read(
        &self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), BoxedError> {
        read_from_bytes(self, start_byte, dst).ok_or_else(|| {
            Box::new(MemIoError {
                start: start_byte,
                read_len: dst.len(),
                src_len: self.len(),
            })
            .into()
        })
    }
}

#[cfg(all(feature = "std", not(feature = "sync"), target_family = "unix"))]
#[async_trait]
impl Ext4Read for std::fs::File {
    async fn read(
        &self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), BoxedError> {
        use std::os::unix::fs::FileExt;

        let total = self.read_at(dst, start_byte).map_err(Box::new)?;
        if total != dst.len() {
            return Err(Box::new(MemIoError {
                start: start_byte,
                read_len: dst.len(),
                src_len: total,
            })
            .into());
        }
        Ok(())
    }
}

#[cfg(all(feature = "std", feature = "sync", target_family = "unix"))]
impl Ext4Read for std::fs::File {
    fn read(&self, start_byte: u64, dst: &mut [u8]) -> Result<(), BoxedError> {
        use std::os::unix::fs::FileExt;

        let total = self.read_at(dst, start_byte).map_err(Box::new)?;
        if total != dst.len() {
            return Err(Box::new(MemIoError {
                start: start_byte,
                read_len: dst.len(),
                src_len: total,
            })
            .into());
        }
        Ok(())
    }
}

fn read_from_bytes(src: &[u8], start_byte: u64, dst: &mut [u8]) -> Option<()> {
    let start = usize::try_from(start_byte).ok()?;
    let end = start.checked_add(dst.len())?;
    let src = src.get(start..end)?;
    dst.copy_from_slice(src);

    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_vec_read() {
        let src = vec![1, 2, 3];

        let mut dst = [0; 3];
        src.read(0, &mut dst).await.unwrap();
        assert_eq!(dst, [1, 2, 3]);

        let mut dst = [0; 2];
        src.read(1, &mut dst).await.unwrap();
        assert_eq!(dst, [2, 3]);

        let err = src.read(4, &mut dst).await.unwrap_err();
        assert_eq!(
            format!("{err}"),
            format!(
                "failed to read 2 bytes at offset 4 from a slice of length 3"
            )
        );
    }
}
