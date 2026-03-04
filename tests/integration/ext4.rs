// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::expected_holes_data;
use crate::test_util::load_test_disk1;
use ext4_rs::{AsyncIterator, Ext4Error, Path, PathBuf};

#[tokio::test]
async fn test_ext4_debug() {
    let fs = load_test_disk1().await;
    let s = format!("{fs:?}");
    // Just check the start and end to avoid over-matching on the test data.
    assert!(s.starts_with("Ext4 { superblock: Superblock { "));
    assert!(s.ends_with(", .. }"));
}

#[tokio::test]
async fn test_canonicalize() {
    let fs = load_test_disk1().await;

    assert_eq!(fs.canonicalize("/empty_file").await.unwrap(), "/empty_file");

    assert_eq!(fs.canonicalize("/").await.unwrap(), "/");
    assert_eq!(fs.canonicalize("/..").await.unwrap(), "/");
    assert_eq!(fs.canonicalize("/dir1").await.unwrap(), "/dir1");
    assert_eq!(fs.canonicalize("/dir1/").await.unwrap(), "/dir1");
    assert_eq!(fs.canonicalize("/dir1/.").await.unwrap(), "/dir1");
    assert_eq!(fs.canonicalize("/dir1/./").await.unwrap(), "/dir1");
    assert_eq!(fs.canonicalize("/dir1/../dir1").await.unwrap(), "/dir1");
    assert_eq!(fs.canonicalize("/dir1/../dir1/").await.unwrap(), "/dir1");
    assert_eq!(
        fs.canonicalize("/dir1/dir2/sym_abs").await.unwrap(),
        "/small_file"
    );
    assert_eq!(
        fs.canonicalize("/dir1/dir2/sym_rel").await.unwrap(),
        "/small_file"
    );
    assert_eq!(
        fs.canonicalize("/dir1/dir2/sym_abs_dir").await.unwrap(),
        "/dir1"
    );
    assert_eq!(
        fs.canonicalize("/dir1/dir2/sym_abs_dir/").await.unwrap(),
        "/dir1"
    );
    assert_eq!(
        fs.canonicalize("/dir1/dir2/sym_rel_dir").await.unwrap(),
        "/dir1"
    );
    assert_eq!(
        fs.canonicalize("/dir1/dir2/sym_rel_dir/").await.unwrap(),
        "/dir1"
    );

    // Error: does not exist.
    assert!(matches!(
        fs.canonicalize("/does_not_exist").await.unwrap_err(),
        Ext4Error::NotFound
    ));

    // Error: child of a non-directory.
    assert!(matches!(
        fs.canonicalize("/small_file/invalid").await.unwrap_err(),
        Ext4Error::NotADirectory
    ));

    // Error: malformed path.
    assert!(matches!(
        fs.canonicalize("\0").await.unwrap_err(),
        Ext4Error::MalformedPath
    ));

    // Error: path is not absolute.
    assert!(matches!(
        fs.canonicalize("not_absolute").await.unwrap_err(),
        Ext4Error::NotAbsolute
    ));
}

#[tokio::test]
async fn test_read() {
    let fs = load_test_disk1().await;

    // Empty file.
    assert_eq!(fs.read("/empty_file").await.unwrap(), []);

    // Small file.
    assert_eq!(fs.read("/small_file").await.unwrap(), b"hello, world!");

    // File with holes.
    assert_eq!(fs.read("/holes").await.unwrap(), expected_holes_data());

    // Errors.
    assert!(fs.read("not_absolute").await.is_err());
    assert!(fs.read("/does_not_exist").await.is_err());
}

#[tokio::test]
async fn test_read_to_string() {
    let fs = load_test_disk1().await;

    // Empty file.
    assert_eq!(fs.read_to_string("/empty_file").await.unwrap(), "");

    // Small file.
    assert_eq!(
        fs.read_to_string("/small_file").await.unwrap(),
        "hello, world!"
    );

    // Errors:
    assert!(matches!(
        fs.read_to_string("/holes").await.unwrap_err(),
        Ext4Error::NotUtf8
    ));
    assert!(matches!(
        fs.read_to_string("/empty_dir").await.unwrap_err(),
        Ext4Error::IsADirectory
    ));
    assert!(matches!(
        fs.read_to_string("not_absolute").await.unwrap_err(),
        Ext4Error::NotAbsolute
    ));
    assert!(matches!(
        fs.read_to_string("/does_not_exist").await.unwrap_err(),
        Ext4Error::NotFound
    ));
    assert!(matches!(
        fs.read_to_string("\0").await.unwrap_err(),
        Ext4Error::MalformedPath
    ));
}

#[tokio::test]
async fn test_read_link() {
    let fs = load_test_disk1().await;

    // Basic success test.
    assert_eq!(fs.read_link("/sym_simple").await.unwrap(), "small_file");

    // Symlinks prior to the final component are expanded as normal.
    assert_eq!(
        fs.read_link("/dir1/dir2/sym_abs_dir/../sym_simple")
            .await
            .unwrap(),
        "small_file"
    );

    // Short symlink target is inline, longer symlink is stored in extents.
    assert_eq!(fs.read_link("/sym_59").await.unwrap(), "a".repeat(59));
    assert_eq!(fs.read_link("/sym_60").await.unwrap(), "a".repeat(60));

    // Error: path is not absolute.
    assert!(matches!(
        fs.read_link("not_absolute").await.unwrap_err(),
        Ext4Error::NotAbsolute
    ));

    // Error: malformed path.
    assert!(matches!(
        fs.read_link("\0").await.unwrap_err(),
        Ext4Error::MalformedPath
    ));

    // Error: does not exist.
    assert!(matches!(
        fs.read_link("/does_not_exist").await.unwrap_err(),
        Ext4Error::NotFound
    ));

    // Error: not a symlink.
    assert!(matches!(
        fs.read_link("/small_file").await.unwrap_err(),
        Ext4Error::NotASymlink
    ));
}

#[tokio::test]
async fn test_read_dir() {
    let fs = load_test_disk1().await;

    // Get contents of directory `/big_dir`.
    let dir = fs.read_dir("/big_dir").await.unwrap().collect().await;
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
    assert!(matches!(
        fs.read_dir("not_absolute").await.unwrap_err(),
        Ext4Error::NotAbsolute
    ));
    assert!(matches!(
        fs.read_dir("/empty_file").await.unwrap_err(),
        Ext4Error::NotADirectory
    ));
    assert!(matches!(
        fs.read_dir("\0").await.unwrap_err(),
        Ext4Error::MalformedPath
    ));
}

#[tokio::test]
async fn test_exists() {
    let fs = load_test_disk1().await;

    // Success: exists.
    assert!(fs.exists("/empty_file").await.unwrap());

    // Success: does not exist.
    assert!(!fs.exists("/does_not_exist").await.unwrap());

    // Error: malformed path.
    assert!(matches!(
        fs.exists("\0").await.unwrap_err(),
        Ext4Error::MalformedPath
    ));

    // Error: path is not absolute.
    assert!(matches!(
        fs.exists("not_absolute").await.unwrap_err(),
        Ext4Error::NotAbsolute
    ));
}

#[tokio::test]
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
    assert!(matches!(
        fs.metadata("\0").await.unwrap_err(),
        Ext4Error::MalformedPath
    ));

    // Error: path is not absolute.
    assert!(matches!(
        fs.metadata("not_absolute").await.unwrap_err(),
        Ext4Error::NotAbsolute
    ));
}

#[tokio::test]
async fn test_metadata_uid_gid() {
    let fs = load_test_disk1().await;

    let metadata = fs.metadata("/owner_file").await.unwrap();
    assert_eq!(metadata.uid(), 123);
    assert_eq!(metadata.gid(), 456);
}

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
async fn test_symlink_metadata() {
    let fs = load_test_disk1().await;

    // Final component is a symlink.
    let metadata = fs.symlink_metadata("/sym_simple").await.unwrap();
    assert!(metadata.is_symlink());
    assert_eq!(metadata.mode(), 0o777);

    // Symlinks prior to the final component are followed as normal.
    assert_eq!(
        fs.symlink_metadata("/dir1/dir2/sym_abs_dir/../sym_simple")
            .await
            .unwrap(),
        metadata
    );

    // Final component not a symlink behaves same as `metadata`.
    assert_eq!(
        fs.symlink_metadata("/small_file").await.unwrap(),
        fs.metadata("/small_file").await.unwrap()
    );

    // Error: malformed path.
    assert!(matches!(
        fs.symlink_metadata("\0").await.unwrap_err(),
        Ext4Error::MalformedPath
    ));

    // Error: path is not absolute.
    assert!(matches!(
        fs.symlink_metadata("not_absolute").await.unwrap_err(),
        Ext4Error::NotAbsolute
    ));
}

#[tokio::test]
async fn test_htree() {
    let fs = load_test_disk1().await;

    // Looking up paths in these directories exercises the
    // `get_dir_entry_via_htree` code. The external API doesn't provide
    // any way to check which code path is taken, but code coverage can
    // confirm it.

    let medium_dir = Path::new("/medium_dir");
    for i in 0..1_000 {
        let i = i.to_string();
        assert_eq!(fs.read_to_string(&medium_dir.join(&i)).await.unwrap(), i);
    }

    let big_dir = Path::new("/big_dir");
    for i in 0..10_000 {
        let i = i.to_string();
        assert_eq!(fs.read_to_string(&big_dir.join(&i)).await.unwrap(), i);
    }
}

#[tokio::test]
async fn test_encrypted_dir() {
    let fs = load_test_disk1().await;

    // This covers the check in `get_dir_entry_inode_by_name`.
    assert!(matches!(
        fs.read("/encrypted_dir/file").await.unwrap_err(),
        Ext4Error::Encrypted
    ));

    // This covers the check in `ReadDir::new`.
    assert!(matches!(
        fs.read_dir("/encrypted_dir").await.unwrap_err(),
        Ext4Error::Encrypted
    ));
}
