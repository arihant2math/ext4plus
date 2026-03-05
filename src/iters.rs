// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use alloc::vec::Vec;

/// Async version of the `Iterator` trait.
#[allow(async_fn_in_trait)]
pub trait AsyncIterator {
    /// The type of the items being yielded by this iterator.
    type Item;

    /// Equivalent to [`Iterator::next`]
    async fn next(&mut self) -> Option<<Self as AsyncIterator>::Item>;

    /// Equivalent to [`Iterator::skip`]
    fn skip(self, n: usize) -> AsyncSkip<Self>
    where
        Self: Sized,
    {
        AsyncSkip::new(self, n)
    }

    /// Equivalent to [`Iterator::nth`]
    async fn nth(self, n: usize) -> Option<<Self as AsyncIterator>::Item>
    where
        Self: Sized,
    {
        self.skip(n).next().await
    }

    /// Equivalent to [`Iterator::collect`], but only for [`Vec`]
    async fn collect(mut self) -> Vec<<Self as AsyncIterator>::Item>
    where
        Self: Sized,
    {
        let mut items = Vec::new();
        while let Some(item) = self.next().await {
            items.push(item);
        }
        items
    }

    /// Equivalent to [`Iterator::map`]
    async fn map<B, F>(self, f: F) -> AsyncMap<Self, F>
    where
        Self: Sized,
        F: FnMut(<Self as AsyncIterator>::Item) -> B,
    {
        AsyncMap { iter: self, f }
    }

    /// Equivalent to [`Iterator::filter`]
    async fn filter<F>(self, f: F) -> AsyncFilter<Self, F>
    where
        Self: Sized,
        F: FnMut(&<Self as AsyncIterator>::Item) -> bool,
    {
        AsyncFilter { iter: self, f }
    }

    /// Equivalent to [`Iterator::find`]
    async fn find<F>(
        mut self,
        mut f: F,
    ) -> Option<<Self as AsyncIterator>::Item>
    where
        Self: Sized,
        F: FnMut(&<Self as AsyncIterator>::Item) -> bool,
    {
        while let Some(item) = self.next().await {
            if f(&item) {
                return Some(item);
            }
        }
        None
    }

    /// Equivalent to [`Iterator::find_map`]
    async fn find_map<B, F>(mut self, mut f: F) -> Option<B>
    where
        Self: Sized,
        F: FnMut(<Self as AsyncIterator>::Item) -> Option<B>,
    {
        while let Some(item) = self.next().await {
            if let Some(mapped) = f(item) {
                return Some(mapped);
            }
        }
        None
    }

    /// Equivalent to [`Iterator::all`]
    async fn all<F>(mut self, mut f: F) -> bool
    where
        Self: Sized,
        F: FnMut(<Self as AsyncIterator>::Item) -> bool,
    {
        while let Some(item) = self.next().await {
            if !f(item) {
                return false;
            }
        }
        true
    }
}

/// Equivalent to [`Iterator::map`]
pub struct AsyncMap<I: AsyncIterator, F> {
    iter: I,
    f: F,
}

impl<I: AsyncIterator, F, B> AsyncIterator for AsyncMap<I, F>
where
    F: FnMut(I::Item) -> B,
{
    type Item = B;

    async fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next().await {
            Some(item) => Some((self.f)(item)),
            None => None,
        }
    }
}

/// Equivalent to [`Iterator::filter`]
pub struct AsyncFilter<I: AsyncIterator, F> {
    iter: I,
    f: F,
}

impl<I: AsyncIterator, F> AsyncIterator for AsyncFilter<I, F>
where
    F: FnMut(&I::Item) -> bool,
{
    type Item = I::Item;

    async fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.iter.next().await {
            if (self.f)(&item) {
                return Some(item);
            }
        }
        None
    }
}

/// Equivalent to [`Iterator::skip`]
pub struct AsyncSkip<I: AsyncIterator> {
    iter: I,
    n: usize,
}

impl<I: AsyncIterator> AsyncSkip<I> {
    fn new(iter: I, n: usize) -> Self {
        Self { iter, n }
    }
}

impl<I: AsyncIterator> AsyncIterator for AsyncSkip<I> {
    type Item = I::Item;

    async fn next(&mut self) -> Option<Self::Item> {
        while self.n > 0 {
            match self.iter.next().await {
                Some(_) => self.n = self.n.checked_sub(1).unwrap(),
                None => return None,
            }
        }
        self.iter.next().await
    }
}

/// This macro implements the `Iterator` trait for type `$target`. The
/// iterator's `Item` type is `Result<$item, Ext4Error>`.
///
/// The `target` type must provide two things:
/// 1. A boolean field named `is_done`. If this field is set to true,
///    iteration will end.
/// 2. A method named `next_impl`, which is where most of the actual
///    iteration is implemented.
///
/// The `next_impl` method returns `Result<Option<$item>, Ext4Error`. If
/// `next_impl` returns `Ok(Some(_))`, that value is yielded. If it
/// returns `Ok(None)`, `next_impl` will be called again. If it returns
/// `Err(_)`, the error will be yielded and `is_done` will be set to
/// true.
///
/// This macro makes iterators easier to write in two ways:
/// 1. Since `next_impl` returns a `Result`, normal error propagation
///    with `?` can be used. Without this macro, each error case would
///    have to set `is_done` before yielding the error.
/// 2. Automatically trying again when `next_impl` returns `Ok(None)`
///    makes it much easier to implement iterators that are logically
///    nested.
macro_rules! impl_result_iter {
    ($target:ident, $item:ident) => {
        impl crate::iters::AsyncIterator for $target {
            type Item = Result<$item, Ext4Error>;

            async fn next(&mut self) -> Option<Result<$item, Ext4Error>> {
                loop {
                    if self.is_done {
                        return None;
                    }

                    match self.next_impl().await {
                        Ok(Some(entry)) => return Some(Ok(entry)),
                        Ok(None) => {
                            // Continue.
                        }
                        Err(err) => {
                            self.is_done = true;
                            return Some(Err(err));
                        }
                    }
                }
            }
        }
    };
}

pub(crate) mod extents;
pub(crate) mod file_blocks;
pub(crate) mod read_dir;

#[cfg(test)]
mod tests {
    use crate::error::{CorruptKind, Ext4Error};
    use crate::iters::AsyncIterator;

    struct I {
        items: Vec<Result<Option<u8>, Ext4Error>>,
        is_done: bool,
    }

    impl I {
        async fn next_impl(&mut self) -> Result<Option<u8>, Ext4Error> {
            // Take and return the first element in `items`.
            self.items.remove(0)
        }
    }

    impl_result_iter!(I, u8);

    /// Test that if `Ok(None)` is returned, the iterator automatically
    /// skips to the next element.
    #[tokio::test]
    async fn test_iter_macro_none() {
        let mut iter = I {
            items: vec![Ok(Some(1)), Ok(None), Ok(Some(2))],
            is_done: false,
        };
        assert_eq!(iter.next().await.unwrap().unwrap(), 1);
        assert_eq!(iter.next().await.unwrap().unwrap(), 2);
    }

    /// Test that if `Err(_)` is returned, the iterator automatically
    /// stops after yielding that error.
    #[tokio::test]
    async fn test_iter_macro_error() {
        let mut iter = I {
            items: vec![
                Ok(Some(1)),
                Err(CorruptKind::SuperblockMagic.into()),
                Ok(Some(2)),
            ],
            is_done: false,
        };
        assert_eq!(iter.next().await.unwrap().unwrap(), 1);
        assert_eq!(
            iter.next().await.unwrap().unwrap_err(),
            CorruptKind::SuperblockMagic
        );
        assert!(iter.next().await.is_none());
    }

    /// Test that if `is_done` is set to true, the iterator stops.
    #[tokio::test]
    async fn test_iter_macro_is_done() {
        let mut iter = I {
            items: vec![Ok(Some(1)), Ok(Some(2))],
            is_done: false,
        };
        assert_eq!(iter.next().await.unwrap().unwrap(), 1);
        iter.is_done = true;
        assert!(iter.next().await.is_none());
    }
}
