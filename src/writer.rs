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

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_write_to_bytes() {
        let mut dst = [0, 1, 2, 3];
        assert_eq!(write_to_bytes(&mut dst, 1, &[9, 8]), Some(()));
        assert_eq!(dst, [0, 9, 8, 3]);

        assert_eq!(write_to_bytes(&mut dst, 4, &[1]), None);
        assert_eq!(write_to_bytes(&mut dst, u64::MAX, &[1]), None);
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_mutex_vec_write() {
        let storage = Mutex::new(vec![0, 1, 2, 3]);

        storage.write(1, &[9, 8]).await.unwrap();
        assert_eq!(*storage.lock().unwrap(), vec![0, 9, 8, 3]);

        let err = storage.write(4, &[1]).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "failed to read 1 bytes at offset 4 from a slice of length 4"
        );
    }

    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_arc_write_delegates_to_inner_writer() {
        let storage = Arc::new(Mutex::new(vec![0, 0, 0, 0]));

        storage.write(2, &[5, 6]).await.unwrap();

        assert_eq!(*storage.lock().unwrap(), vec![0, 0, 5, 6]);
    }

    #[cfg(target_family = "unix")]
    #[maybe_async::test(
        feature = "sync",
        async(not(feature = "sync"), tokio::test)
    )]
    async fn test_file_write() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().set_len(4).unwrap();

        Ext4Write::write(tmp.as_file(), 1, &[7, 8]).await.unwrap();

        assert_eq!(std::fs::read(tmp.path()).unwrap(), vec![0, 7, 8, 0]);
    }
}
