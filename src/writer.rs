//! Interface used by [`crate::Ext4`] to write the filesystem data to a storage
use crate::error::BoxedError;
#[cfg(not(feature = "sync"))]
use async_trait::async_trait;

#[cfg(not(feature = "sync"))]
use alloc::boxed::Box;

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
#[cfg(all(not(feature = "multi-threaded"), not(feature = "sync")))]
#[async_trait(?Send)]
pub trait Ext4Write {
    /// Write bytes from `src`, starting at `start_byte`.
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), BoxedError>;
}

/// Interface used by [`Ext4`] to write the filesystem data to a storage
/// file or device.
///
/// [`Ext4`]: crate::Ext4
#[cfg(all(not(feature = "multi-threaded"), feature = "sync"))]
pub trait Ext4Write {
    /// Write bytes from `src`, starting at `start_byte`.
    fn write(&self, start_byte: u64, src: &[u8]) -> Result<(), BoxedError>;
}

/// Interface used by [`Ext4`] to write the filesystem data to a storage
/// file or device.
///
/// [`Ext4`]: crate::Ext4
#[cfg(all(feature = "multi-threaded", not(feature = "sync")))]
#[async_trait]
pub trait Ext4Write: Send + Sync {
    /// Write bytes from `src`, starting at `start_byte`.
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), BoxedError>;
}

/// Interface used by [`Ext4`] to write the filesystem data to a storage
/// file or device.
///
/// [`Ext4`]: crate::Ext4
#[cfg(all(feature = "multi-threaded", feature = "sync"))]
pub trait Ext4Write: Send + Sync {
    /// Write bytes from `src`, starting at `start_byte`.
    fn write(&self, start_byte: u64, src: &[u8]) -> Result<(), BoxedError>;
}

#[cfg(all(
    feature = "std",
    not(feature = "multi-threaded"),
    not(feature = "sync")
))]
#[async_trait(?Send)]
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

#[cfg(all(feature = "std", feature = "multi-threaded", not(feature = "sync")))]
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

#[cfg(all(feature = "std", feature = "sync"))]
impl Ext4Write for Mutex<Vec<u8>> {
    fn write(&self, start_byte: u64, src: &[u8]) -> Result<(), BoxedError> {
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

#[cfg(all(not(feature = "multi-threaded"), not(feature = "sync")))]
#[async_trait(?Send)]
impl<T> Ext4Write for alloc::rc::Rc<T>
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

#[cfg(all(not(feature = "multi-threaded"), feature = "sync"))]
impl<T> Ext4Write for alloc::rc::Rc<T>
where
    T: Ext4Write,
{
    fn write(&self, start_byte: u64, src: &[u8]) -> Result<(), BoxedError> {
        (**self).write(start_byte, src)
    }
}

#[cfg(all(feature = "multi-threaded", not(feature = "sync")))]
#[async_trait]
impl<T> Ext4Write for alloc::sync::Arc<T>
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

#[cfg(all(feature = "multi-threaded", feature = "sync"))]
impl<T> Ext4Write for alloc::sync::Arc<T>
where
    T: Ext4Write,
{
    fn write(&self, start_byte: u64, src: &[u8]) -> Result<(), BoxedError> {
        (**self).write(start_byte, src)
    }
}

#[cfg(all(
    feature = "std",
    not(feature = "multi-threaded"),
    not(feature = "sync"),
    target_family = "unix"
))]
#[async_trait(?Send)]
impl Ext4Write for std::fs::File {
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), BoxedError> {
        use std::os::unix::fs::FileExt;

        let total = self.write_at(src, start_byte).map_err(Box::new)?;
        if total != src.len() {
            return Err(Box::new(crate::MemIoError {
                start: start_byte,
                read_len: src.len(),
                src_len: total,
            })
            .into());
        }
        Ok(())
    }
}

#[cfg(all(
    feature = "std",
    feature = "multi-threaded",
    not(feature = "sync"),
    target_family = "unix"
))]
#[async_trait]
impl Ext4Write for std::fs::File {
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), BoxedError> {
        use std::os::unix::fs::FileExt;

        let total = self.write_at(src, start_byte).map_err(Box::new)?;
        if total != src.len() {
            return Err(Box::new(crate::MemIoError {
                start: start_byte,
                read_len: src.len(),
                src_len: total,
            })
            .into());
        }
        Ok(())
    }
}

#[cfg(all(feature = "std", feature = "sync", target_family = "unix"))]
impl Ext4Write for std::fs::File {
    fn write(&self, start_byte: u64, src: &[u8]) -> Result<(), BoxedError> {
        use std::os::unix::fs::FileExt;

        let total = self.write_at(src, start_byte).map_err(Box::new)?;
        if total != src.len() {
            return Err(Box::new(crate::MemIoError {
                start: start_byte,
                read_len: src.len(),
                src_len: total,
            })
            .into());
        }
        Ok(())
    }
}
