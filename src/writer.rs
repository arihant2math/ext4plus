use crate::error::BoxedError;
use async_trait::async_trait;

use alloc::boxed::Box;
use alloc::sync::Arc;

#[cfg(feature = "std")]
use std::sync::Mutex;

#[cfg(feature = "std")]
fn write_to_bytes(dst: &mut [u8], start_byte: u64, src: &[u8]) -> Option<()> {
    let start = usize::try_from(start_byte).ok()?;
    let end = start.checked_add(src.len())?;
    let dst = dst.get_mut(start..end)?;
    dst.copy_from_slice(src);

    Some(())
}

/// Interface used by [`Ext4`] to write the filesystem data to a storage
/// file or device.
///
/// [`Ext4`]: crate::Ext4
#[async_trait]
pub trait Ext4Write: Send + Sync {
    /// Write bytes from `src`, starting at `start_byte`.
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), BoxedError>;
}

#[cfg(feature = "std")]
#[async_trait]
impl Ext4Write for Mutex<Vec<u8>> {
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), BoxedError> {
        let mut guard = self.lock().unwrap();
        write_to_bytes(guard.as_mut(), start_byte, src).ok_or_else(|| {
            Box::new(crate::mem_io_error::MemIoError {
                start: start_byte,
                read_len: src.len(),
                src_len: guard.len(),
            })
            .into()
        })
    }
}

#[async_trait]
impl<T> Ext4Write for Arc<T>
where
    T: Ext4Write,
{
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), BoxedError> {
        (**self).write(start_byte, src).await
    }
}
