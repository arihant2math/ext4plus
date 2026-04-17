//! Sync primitives

#[cfg(not(feature = "sync"))]
mod async_inner {
    /// A mutual exclusion primitive useful for protecting shared data.
    pub struct Mutex<T> {
        inner: async_lock::Mutex<T>,
    }

    impl<T> Mutex<T> {
        /// Create a new mutex in an unlocked state ready for use.
        pub fn new(t: T) -> Self {
            Self {
                inner: async_lock::Mutex::new(t),
            }
        }

        /// Acquires a lock, blocking the current task until it is able to do so.
        pub async fn lock(&self) -> async_lock::MutexGuard<'_, T> {
            self.inner.lock().await
        }
    }

    /// A reader-writer lock.
    pub struct RwLock<T> {
        inner: async_lock::RwLock<T>,
    }

    impl<T> RwLock<T> {
        /// Create a new RwLock in an unlocked state ready for use.
        pub fn new(t: T) -> Self {
            Self {
                inner: async_lock::RwLock::new(t),
            }
        }

        /// Acquires a shared read lock, blocking the current task until it is able to do so.
        pub async fn read(&self) -> async_lock::RwLockReadGuard<'_, T> {
            self.inner.read().await
        }

        /// Acquires an exclusive write lock, blocking the current task until it is able to do so.
        pub async fn write(&self) -> async_lock::RwLockWriteGuard<'_, T> {
            self.inner.write().await
        }
    }
}

#[cfg(feature = "sync")]
mod sync_inner {
    /// A mutual exclusion primitive useful for protecting shared data.
    pub struct Mutex<T> {
        inner: spin::Mutex<T>,
    }

    impl<T> Mutex<T> {
        /// Create a new mutex in an unlocked state ready for use.
        pub fn new(t: T) -> Self {
            Self {
                inner: spin::Mutex::new(t),
            }
        }

        /// Acquires a lock, blocking the current thread until it is able to do so.
        pub fn lock(&self) -> spin::MutexGuard<'_, T> {
            self.inner.lock()
        }
    }

    /// A reader-writer lock.
    pub struct RwLock<T> {
        inner: spin::RwLock<T>,
    }

    impl<T> RwLock<T> {
        /// Create a new RwLock in an unlocked state ready for use.
        pub fn new(t: T) -> Self {
            Self {
                inner: spin::RwLock::new(t),
            }
        }

        /// Acquires a shared read lock, blocking the current thread until it is able to do so.
        pub fn read(&self) -> spin::RwLockReadGuard<'_, T> {
            self.inner.read()
        }

        /// Acquires an exclusive write lock, blocking the current thread until it is able to do so.
        pub fn write(&self) -> spin::RwLockWriteGuard<'_, T> {
            self.inner.write()
        }
    }
}

#[cfg(not(feature = "sync"))]
pub use self::async_inner::*;
#[cfg(feature = "sync")]
pub use self::sync_inner::*;

#[cfg(not(feature = "multi-threaded"))]
pub(crate) type PtrPrimitive<T> = alloc::rc::Rc<T>;

#[cfg(feature = "multi-threaded")]
pub(crate) type PtrPrimitive<T> = alloc::sync::Arc<T>;
