// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::expected_holes_data;
use crate::test_util::load_test_disk1;
use ext4plus::prelude::{AsyncIterator, Ext4Error, Path, PathBuf};

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_ext4_debug() {
    let fs = load_test_disk1().await;
    let s = format!("{fs:?}");
    // Just check the start and end to avoid over-matching on the test data.
    assert!(s.starts_with("Ext4 { superblock: Superblock { "));
    assert!(s.ends_with(", .. }"));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_canonicalize() {
    let fs = load_test_disk1().await;

    let path = fs.canonicalize("/empty_file").await.unwrap();
    assert_eq!(path, "/empty_file");

    let path = fs.canonicalize("/").await.unwrap();
    assert_eq!(path, "/");
    let path = fs.canonicalize("/..").await.unwrap();
    assert_eq!(path, "/");
    let path = fs.canonicalize("/dir1").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/.").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/./").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/../dir1").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/../dir1/").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/dir2/sym_abs").await.unwrap();
    assert_eq!(path, "/small_file");
    let path = fs.canonicalize("/dir1/dir2/sym_rel").await.unwrap();
    assert_eq!(path, "/small_file");
    let path = fs.canonicalize("/dir1/dir2/sym_abs_dir").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/dir2/sym_abs_dir/").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/dir2/sym_rel_dir").await.unwrap();
    assert_eq!(path, "/dir1");
    let path = fs.canonicalize("/dir1/dir2/sym_rel_dir/").await.unwrap();
    assert_eq!(path, "/dir1");

    // Error: does not exist.
    let err = fs.canonicalize("/does_not_exist").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotFound));

    // Error: child of a non-directory.
    let err = fs.canonicalize("/small_file/invalid").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotADirectory));

    // Error: malformed path.
    let err = fs.canonicalize("\0").await.unwrap_err();
    assert!(matches!(err, Ext4Error::MalformedPath));

    // Error: path is not absolute.
    let err = fs.canonicalize("not_absolute").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotAbsolute));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_read() {
    let fs = load_test_disk1().await;

    // Empty file.
    let content = fs.read("/empty_file").await.unwrap();
    assert_eq!(content, []);

    // Small file.
    let content = fs.read("/small_file").await.unwrap();
    assert_eq!(content, b"hello, world!");

    // File with holes.
    let content = fs.read("/holes").await.unwrap();
    assert_eq!(content, expected_holes_data());

    // Errors.
    let err = fs.read("not_absolute").await;
    assert!(err.is_err());
    let err = fs.read("/does_not_exist").await;
    assert!(err.is_err());
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_read_to_string() {
    let fs = load_test_disk1().await;

    // Empty file.
    let text = fs.read_to_string("/empty_file").await.unwrap();
    assert_eq!(text, "");

    // Small file.
    let text = fs.read_to_string("/small_file").await.unwrap();
    assert_eq!(
        text,
        "hello, world!"
    );

    // Errors:
    let err = fs.read_to_string("/holes").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotUtf8));
    let err = fs.read_to_string("/empty_dir").await.unwrap_err();
    assert!(matches!(err, Ext4Error::IsADirectory));
    let err = fs.read_to_string("not_absolute").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotAbsolute));
    let err = fs.read_to_string("/does_not_exist").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotFound));
    let err = fs.read_to_string("\0").await.unwrap_err();
    assert!(matches!(err, Ext4Error::MalformedPath));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_read_link() {
    let fs = load_test_disk1().await;

    // Basic success test.
    let link = fs.read_link("/sym_simple").await.unwrap();
    assert_eq!(link, "small_file");

    // Symlinks prior to the final component are expanded as normal.
    let link = fs
        .read_link("/dir1/dir2/sym_abs_dir/../sym_simple")
        .await
        .unwrap();
    assert_eq!(link, "small_file");

    // Short symlink target is inline, longer symlink is stored in extents.
    let link = fs.read_link("/sym_59").await.unwrap();
    assert_eq!(link, "a".repeat(59));
    let link = fs.read_link("/sym_60").await.unwrap();
    assert_eq!(link, "a".repeat(60));

    // Error: path is not absolute.
    let err = fs.read_link("not_absolute").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotAbsolute));

    // Error: malformed path.
    let err = fs.read_link("\0").await.unwrap_err();
    assert!(matches!(err, Ext4Error::MalformedPath));

    // Error: does not exist.
    let err = fs.read_link("/does_not_exist").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotFound));

    // Error: not a symlink.
    let err = fs.read_link("/small_file").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotASymlink));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_read_dir() {
    let fs = load_test_disk1().await;

    // Get contents of directory `/big_dir`.
    let dir: Vec<_> = fs.read_dir("/big_dir").await.unwrap().collect().await;
    let dir = dir.into_iter().map(|s| s.unwrap()).collect::<Vec<_>>();

    // Get the sorted list of entry names.
    let mut entry_names: Vec<String> = dir
        .iter()
        .map(|e| e.file_name().as_str().unwrap().to_owned())
        .collect();
    entry_names.sort_unstable();

    // Get the sorted list of entry paths.
    let mut entry_paths: Vec<PathBuf> = dir.iter().map(|e| e.path()).collect();
    entry_paths.sort_unstable();

    // Check file types.
    for entry in &dir {
        let fname = entry.file_name();
        let ftype = entry.file_type().unwrap();
        if fname == "." || fname == ".." {
            assert!(ftype.is_dir());
        } else {
            assert!(ftype.is_regular_file());
        }
    }

    // Get expected entry names, 0-9999.
    let mut expected_names = vec![".".to_owned(), "..".to_owned()];
    expected_names.extend((0u32..10_000u32).map(|n| n.to_string()));
    expected_names.sort_unstable();

    // Get expected entry paths.
    let expected_paths = expected_names
        .iter()
        .map(|n| PathBuf::try_from(format!("/big_dir/{n}").as_bytes()).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(entry_names, expected_names);
    assert_eq!(entry_paths, expected_paths);

    // Errors:
    let err = fs.read_dir("not_absolute").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotAbsolute));
    let err = fs.read_dir("/empty_file").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotADirectory));
    let err = fs.read_dir("\0").await.unwrap_err();
    assert!(matches!(err, Ext4Error::MalformedPath));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_exists() {
    let fs = load_test_disk1().await;

    // Success: exists.
    let exists = fs.exists("/empty_file").await.unwrap();
    assert!(exists);

    // Success: does not exist.
    let exists = fs.exists("/does_not_exist").await.unwrap();
    assert!(!exists);

    // Error: malformed path.
    let err = fs.exists("\0").await.unwrap_err();
    assert!(matches!(err, Ext4Error::MalformedPath));

    // Error: path is not absolute.
    let err = fs.exists("not_absolute").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotAbsolute));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_metadata() {
    let fs = load_test_disk1().await;

    let metadata = fs.metadata("/small_file").await.unwrap();
    assert!(metadata.file_type().is_regular_file());
    assert!(!metadata.is_dir());
    assert!(!metadata.is_symlink());
    assert_eq!(metadata.mode(), 0o644);
    assert_eq!(
        metadata.len(),
        u64::try_from("hello, world!".len()).unwrap()
    );

    // Error: malformed path.
    let err = fs.metadata("\0").await.unwrap_err();
    assert!(matches!(err, Ext4Error::MalformedPath));

    // Error: path is not absolute.
    let err = fs.metadata("not_absolute").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotAbsolute));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_metadata_uid_gid() {
    let fs = load_test_disk1().await;

    let metadata = fs.metadata("/owner_file").await.unwrap();
    assert_eq!(metadata.uid(), 123);
    assert_eq!(metadata.gid(), 456);
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_direntry_debug() {
    let fs = load_test_disk1().await;
    let entry = fs
        .read_dir("/")
        .await
        .unwrap()
        .map(|e| e.unwrap())
        .await
        .find(|e| e.file_name() == "small_file")
        .await
        .unwrap();
    assert_eq!(format!("{:?}", entry.path()), r#""/small_file""#);
    assert_eq!(format!("{entry:?}"), r#"DirEntry("/small_file")"#);
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_direntry_metadata() {
    let fs = load_test_disk1().await;

    let entry = fs
        .read_dir("/")
        .await
        .unwrap()
        .find_map(|entry| {
            let entry = entry.unwrap();
            if entry.file_name() == "small_file" {
                Some(entry)
            } else {
                None
            }
        })
        .await
        .unwrap();
    let metadata = entry.metadata().await.unwrap();
    assert_eq!(
        metadata.len(),
        u64::try_from("hello, world!".len()).unwrap()
    );
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_symlink_metadata() {
    let fs = load_test_disk1().await;

    // Final component is a symlink.
    let metadata = fs.symlink_metadata("/sym_simple").await.unwrap();
    assert!(metadata.is_symlink());
    assert_eq!(metadata.mode(), 0o777);

    // Symlinks prior to the final component are followed as normal.
    let symlink_metadata = fs
        .symlink_metadata("/dir1/dir2/sym_abs_dir/../sym_simple")
        .await
        .unwrap();
    assert_eq!(symlink_metadata, metadata);

    // Final component not a symlink behaves same as `metadata`.
    let symlink_metadata = fs.symlink_metadata("/small_file").await.unwrap();
    let metadata = fs.metadata("/small_file").await.unwrap();
    assert_eq!(symlink_metadata, metadata);

    // Error: malformed path.
    let err = fs.symlink_metadata("\0").await.unwrap_err();
    assert!(matches!(err, Ext4Error::MalformedPath));

    // Error: path is not absolute.
    let err = fs.symlink_metadata("not_absolute").await.unwrap_err();
    assert!(matches!(err, Ext4Error::NotAbsolute));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_htree() {
    let fs = load_test_disk1().await;

    // Looking up paths in these directories exercises the
    // `get_dir_entry_via_htree` code. The external API doesn't provide
    // any way to check which code path is taken, but code coverage can
    // confirm it.

    let medium_dir = Path::new("/medium_dir");
    for i in 0..1_000 {
        let i = i.to_string();
        let content = fs.read_to_string(&medium_dir.join(&i)).await.unwrap();
        assert_eq!(content, i);
    }

    let big_dir = Path::new("/big_dir");
    for i in 0..10_000 {
        let i = i.to_string();
        let content = fs.read_to_string(&big_dir.join(&i)).await.unwrap();
        assert_eq!(content, i);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_encrypted_dir() {
    let fs = load_test_disk1().await;

    // This covers the check in `get_dir_entry_inode_by_name`.
    let err = fs.read("/encrypted_dir/file").await.unwrap_err();
    assert!(matches!(err, Ext4Error::Encrypted));

    // This covers the check in `ReadDir::new`.
    let err = fs.read_dir("/encrypted_dir").await.unwrap_err();
    assert!(matches!(err, Ext4Error::Encrypted));
}
