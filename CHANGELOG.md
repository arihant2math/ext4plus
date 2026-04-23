# Changelog

## [Unreleased]

- Expose `Superblock::num_block_groups`, `Superblock::inodes_per_block_group`, and `Superblock::write`.
- Add `Superblock::mount_time` and `Superblock::mkfs_time`.
- Ensure proper xattr feature exists in filesystem before allowing xattr operations.
- Add `FilesystemFeatures` struct to get a more detailed view of the features of the filesystem, exposes struct via `Superblock::features`.

## 0.1.0-beta.2

- Add `multi-threading` feature flag so that thread safety can be opted out for single-threaded environments.
- Add xattr support: `Ext4::get_xattr`, `Ext4::set_xattr`, `Ext4::remove_xattr`, and `Ext4::list_xattrs` as well as `Inode` methods.
    - Doesn't support very large amounts of xattrs

## 0.1.0-beta.1

- BREAKING: Require mutable dir for `Ext4::symlink` and mutable self for `Dir::link` and `Dir::unlink`.
- Support modification of htree directories
- Add `Inode::dtime_val` and `Inode::set_dtime_val` for handling of deleted inodes.
- Fix writing to directories not supporting internally allocating blocks
- Dependency bump
- More fsck fixes
    - Include fix for bg group bitmap checksum calculation
    - Also fixes superblock not being written back properly

## 0.1.0-alpha.7

- BREAKING: Hide `Inode::set_blocks`
- Add `ext4plus::sync` to export sync primitives.
- Add `Ext4::superblock` to get a reference to the superblock for more advanced use cases.
- Add `Inode::fs_blocks` and `Inode::set_fs_blocks` for cleaner API that doesn't have to consider 512-byte vs full block sizes.
- Disable writing when unsupported RO features are present to prevent corruption.
- Honor `min_extra_isize` when creating inodes.
- fsck fixes
    - Includes fixes that prevent data loss, generally allows better interop with other drivers due to checksum fixes

## 0.1.0-alpha.6

- BREAKING: `AsyncIterator::collect` functions like `Iterator::collect` now and requires a generic type parameter.
- BREAKING: Remove `Dir::open` and make `Dir::open_indoe` non-async.
- Added a sync API which is gated behind the `sync` feature flag.
- Error when writing to immutable files.

## 0.1.0-alpha.5

- BREAKING: Some imports have been moved to modules. Use `ext4plus::prelude` if you want to keep import compatibility.
- Set `i_crtime` when creating inodes.
- Fix infinite loop in block allocation logic
- Fix large block map write failing

## 0.1.0-alpha.4

- BREAKING: `Dir` now uses `inode()` and `inode_mut()` instead of `AsRef` and `AsMut` for consistent API with `File`.
- `Dir::open` (deprecated for now) has become `Dir::open_inode`
- Expose `Inode::blocks` and `Inode::set_blocks`.
- Expose `Inode::crtime` and `Inode::set_crtime` as well as `Metadata.crtime` (`Option<Duration>`, due to lack of availability on 32-bit filesystems).
- Remove all restrictions on block map writing, allowing writing to block maps even with indirect blocks.
- Support extra 2 bits for seconds in timestamp to avoid Y2038 problem as well as nanosecond precision.
- Fix unused_inodes_count not being updated when allocated inodes, leading to inodes possibly being overwritten by other drivers.
- Update `i_blocks` on writes.
- Set `extra_size` correctly when creating an inode
- Handle non 128/256-byte inode sizes better

## 0.1.0-alpha.3

- BREAKING: Removes `Ext4::link` and `Ext4::unlink`. These are superseded by `Dir::link` and `Dir::unlink`, which allow
  for any valid byte string to be a dirname, instead of just valid UTF-8.
- BREAKING: Can no longer open irregular files as `File`.
- BREAKING: Fix incorrect checksum updating when initializing directory, leading to the last entry being reported as corrupted.
- `File::truncate` now can be used beyond block boundaries, and will zero out new blocks as needed.
- Add `truncate` function for `File::truncate` without a file struct.
- Fix post-write position update in `File::write`
- Expose `Dir` object, which is like `File`. De-exposes `get_dir_entry_inode_by_name` and `init_directory`, these can be done by `Directory` instead.
- Fix inode size not being updated when writing to block map files
- Bump MSRV to 1.86

## 0.1.0-alpha.2

* Support writing to block maps when only using direct block pointers
* Support creating inodes with the `extents` feature disabled
* No hidden panics due to as conversions or arithmetic overflow, these are now findable by searching for `unwrap` or `expect`.

## 0.1.0-alpha.1

* Initial forked release
* Asyncify the API
* Add write support. The limitations are as follows:
  * No support for journaling.
  * Errors during writing can lead to filesystem corruption
  * No support for writing to htree directories.
  * No support for writing to files that use file block maps (i.e. filesystems with the `extents` feature disabled).

# Earlier changelog (ext4-view-rs)

## 0.9.3

* Added support for TEA hashes in directory blocks.
* Fixed 128-byte inodes incorrectly triggering a corruption error.

## 0.9.2

* Added a block cache to improve performance when running in an
  environment where the OS doesn't provide a block cache.

## 0.9.1

* Added support for the `journal_incompat_revoke` feature.

## 0.9.0

* Removed `Ext4Error::as_corrupt` and `Ext4Error::as_incompatible`.
* Renamed `Incompatible::Missing` to `Incompatible::MissingRequiredFeatures`.
* Renamed `Incompatible::Incompatible` to `Incompatible::UnsupportedFeatures`.
* Removed `Incompatible::Unknown`; these errors are now reported as
  `Incompatible::UnsupportedFeatures`.
* Removed `Incompatible::DirectoryEncrypted` and replaced it with
  `Ext4Error::Encrypted`.
* Removed `impl From<Corrupt> for Ext4Error` and
  `impl From<Incompatible>> for Ext4Error`.
* Made the `Incompatible` type opaque. It is no longer possible to
  `match` on specific types of incompatibility.
* Implemented several path conversions for non-Unix platforms that were
  previously only available on Unix. On non-Unix platforms, these
  conversions will fail on non-UTF-8 input.
  * `TryFrom<&OsStr> for ext4_view::Path`
  * `TryFrom<&std::path::PathBuf> for ext4_view::Path`
  * `TryFrom<OsString> for ext4_view::PathBuf`
  * `TryFrom<std::path::PathBuf> for ext4_view::PathBuf`
* Added support for reading filesystems that weren't cleanly unmounted.

## 0.8.0

* Added `Path::to_str` and `PathBuf::to_str`.
* Added `Ext4::label` to get the filesystem label.
* Added `Ext4::uuid` to get the filesystem UUID.
* Made the `Corrupt` type opaque. It is no longer possible to `match` on
  specific types of corruption.

## 0.7.0

* Added `File` type and `Ext4::open`. This can be used to read parts of
  files rather than reading the whole file at once with `Ext4::read`. If
  the `std` feature is enabled, `File` impls `Read` and `Seek`.
* Added `impl From<Ext4Error> for std::io::Error`.
* Added `impl From<Corrupt> for Ext4Error`.
* Added `impl From<Incompatible> for Ext4Error`.
* Made `BytesDisplay` public.
* Made the library more robust against arithmetic overflow.

## 0.6.1

* Fixed a panic when loading an invalid superblock.

## 0.6.0

* MSRV increased to `1.81`.
* The error types now unconditionally implement `core::error::Error`.
* The `IoError` trait has been removed. `Ext4Read::read` now returns
  `Box<dyn Error + Send + Sync + 'static>`, and that same type is now
  stored in `Ext4Error::Io`.
