// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use core::mem::size_of;

/// Convert a `u32` to a `usize`.
///
/// Rust allows `usize` to be as small as `u16`, but on platforms
/// supported by this crate, this conversion is infallible.
///
/// # Panics
///
/// Panics if `val` does not fit in this platform's `usize`.
#[inline]
#[must_use]
pub(crate) const fn usize_from_u32(val: u32) -> usize {
    assert!(size_of::<usize>() >= size_of::<u32>());

    // Cannot use `usize::try_from` in a `const fn`.
    #[expect(clippy::as_conversions)]
    {
        val as usize
    }
}

#[inline]
#[must_use]
pub(crate) const fn u64_from_usize(val: usize) -> u64 {
    assert!(size_of::<usize>() <= size_of::<u64>());

    // Cannot use `u64::try_from` in a `const fn`.
    #[expect(clippy::as_conversions)]
    {
        val as u64
    }
}

/// Create a `u64` from two `u32` values.
#[inline]
#[must_use]
pub(crate) fn u64_from_hilo(hi: u32, lo: u32) -> u64 {
    (u64::from(hi) << 32) | u64::from(lo)
}

/// Split a `u64` into two `u32` values, returning the high 32 bits and low 32 bits.
#[inline]
#[must_use]
#[expect(clippy::as_conversions)]
pub(crate) fn u64_to_hilo(val: u64) -> (u32, u32) {
    let hi = (val >> 32) as u32;
    let lo = (val & 0xffff_ffff) as u32;
    (hi, lo)
}

/// Create a `u32` from two `u16` values.
#[inline]
#[must_use]
pub(crate) fn u32_from_hilo(hi: u16, lo: u16) -> u32 {
    (u32::from(hi) << 16) | u32::from(lo)
}

/// Split a `u32` into two `u16` values, returning the high 16 bits and low 16 bits.
#[inline]
#[must_use]
pub(crate) fn u32_to_hilo(val: u32) -> (u16, u16) {
    #[expect(clippy::as_conversions)]
    let hi = (val >> 16) as u16;
    #[expect(clippy::as_conversions)]
    let lo = (val & 0xffff) as u16;
    (hi, lo)
}

/// Read a little-endian [`u16`] from `bytes` at `offset`.
///
/// # Panics
///
/// Panics if `bytes` is not large enough to read two bytes at `offset`.
#[inline]
#[must_use]
#[track_caller]
pub(crate) fn read_u16le(bytes: &[u8], offset: usize) -> u16 {
    // OK to unwrap: these panics are described in the docstring.
    let end = offset.checked_add(size_of::<u16>()).unwrap();
    let bytes = bytes.get(offset..end).unwrap();
    u16::from_le_bytes(bytes.try_into().unwrap())
}

/// Write a little-endian [`u16`] to `bytes` at `offset` .
///
/// # Panics
/// Panics if `bytes` is not large enough to write two bytes at `offset`.
/// Panics if `offset + 2` overflows.
#[inline]
#[track_caller]
pub(crate) fn write_u16le(bytes: &mut [u8], offset: usize, val: u16) {
    // OK to unwrap: these panics are described in the docstring.
    let end = offset.checked_add(size_of::<u16>()).unwrap();
    let bytes = bytes.get_mut(offset..end).unwrap();
    bytes.copy_from_slice(&val.to_le_bytes());
}

/// Read a little-endian [`u32`] from `bytes` at `offset`.
///
/// # Panics
///
/// Panics if `bytes` is not large enough to read four bytes at `offset`.
#[inline]
#[must_use]
#[track_caller]
pub(crate) fn read_u32le(bytes: &[u8], offset: usize) -> u32 {
    // OK to unwrap: these panics are described in the docstring.
    let end = offset.checked_add(size_of::<u32>()).unwrap();
    let bytes = bytes.get(offset..end).unwrap();
    u32::from_le_bytes(bytes.try_into().unwrap())
}

/// Write a little-endian [`u32`] to `bytes` at `offset` .
///
/// # Panics
/// Panics if `bytes` is not large enough to write four bytes at `offset`.
/// Panics if `offset + 4` overflows.
#[inline]
#[track_caller]
pub(crate) fn write_u32le(bytes: &mut [u8], offset: usize, val: u32) {
    // OK to unwrap: these panics are described in the docstring.
    let end = offset.checked_add(size_of::<u32>()).unwrap();
    let bytes = bytes.get_mut(offset..end).unwrap();
    bytes.copy_from_slice(&val.to_le_bytes());
}

/// Read a big-endian [`u32`] from `bytes` at `offset`.
///
/// # Panics
///
/// Panics if `bytes` is not large enough to read four bytes at `offset`.
#[inline]
#[must_use]
#[track_caller]
pub(crate) fn read_u32be(bytes: &[u8], offset: usize) -> u32 {
    // OK to unwrap: these panics are described in the docstring.
    let end = offset.checked_add(size_of::<u32>()).unwrap();
    let bytes = bytes.get(offset..end).unwrap();
    u32::from_be_bytes(bytes.try_into().unwrap())
}

/// Read a little-endian [`u64`] from `bytes` at `offset`.
///
/// # Panics
///
/// Panics if `bytes` is not large enough to read four bytes at `offset`.
#[inline]
#[must_use]
#[track_caller]
pub(crate) fn read_u64le(bytes: &[u8], offset: usize) -> u64 {
    // OK to unwrap: these panics are described in the docstring.
    let end = offset.checked_add(size_of::<u64>()).unwrap();
    let bytes = bytes.get(offset..end).unwrap();
    u64::from_le_bytes(bytes.try_into().unwrap())
}
