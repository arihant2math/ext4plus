// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::test_util::{MemRw, load_test_disk1, load_test_disk1_rw};
use ext4plus::Ext4;
use ext4plus::prelude::{
    Dir, DirEntryName, Ext4Error, FileType, InodeCreationOptions, InodeFlags,
    InodeMode,
};

#[maybe_async::maybe_async]
async fn create_empty_file(fs: &Ext4) {
    let root_inode = fs.read_root_inode().await.unwrap();
    let mut root_dir = Dir::open_inode(fs, root_inode).unwrap();
    let mut inode = fs
        .create_inode(InodeCreationOptions {
            file_type: FileType::Regular,
            mode: InodeMode::S_IRUSR | InodeMode::S_IWUSR | InodeMode::S_IFREG,
            uid: 0,
            gid: 0,
            time: Default::default(),
            flags: InodeFlags::empty(),
        })
        .await
        .unwrap();
    root_dir
        .link(DirEntryName::try_from(b"xattr_file").unwrap(), &mut inode)
        .await
        .unwrap();
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_read_existing_xattrs() {
    let fs = load_test_disk1().await;

    let value = fs
        .get_xattr("/small_file", b"security.selinux")
        .await
        .unwrap();
    assert_eq!(
        value,
        Some(b"unconfined_u:object_r:unlabeled_t:s0\0".to_vec())
    );

    let names = fs.list_xattrs("/small_file").await.unwrap();
    assert_eq!(names, vec![b"security.selinux".to_vec()]);
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_xattrs_roundtrip() {
    let fs = load_test_disk1_rw().await;
    create_empty_file(&fs).await;

    let names = fs.list_xattrs("/xattr_file").await.unwrap();
    assert!(names.is_empty());
    let value = fs.get_xattr("/xattr_file", b"user.color").await.unwrap();
    assert_eq!(value, None);

    fs.set_xattr("/xattr_file", b"user.color", b"blue")
        .await
        .unwrap();
    fs.set_xattr("/xattr_file", b"trusted.flag", b"yes")
        .await
        .unwrap();

    let value = fs.get_xattr("/xattr_file", b"user.color").await.unwrap();
    assert_eq!(value, Some(b"blue".to_vec()));
    let value = fs.get_xattr("/xattr_file", b"trusted.flag").await.unwrap();
    assert_eq!(value, Some(b"yes".to_vec()));

    let mut names = fs.list_xattrs("/xattr_file").await.unwrap();
    names.sort();
    assert_eq!(
        names,
        vec![b"trusted.flag".to_vec(), b"user.color".to_vec()]
    );

    fs.set_xattr("/xattr_file", b"user.color", b"green")
        .await
        .unwrap();
    let value = fs.get_xattr("/xattr_file", b"user.color").await.unwrap();
    assert_eq!(value, Some(b"green".to_vec()));

    let reloaded = Ext4::load(Box::new(MemRw(fs.1.clone()))).await.unwrap();
    let mut names = reloaded.list_xattrs("/xattr_file").await.unwrap();
    names.sort();
    assert_eq!(
        names,
        vec![b"trusted.flag".to_vec(), b"user.color".to_vec()]
    );
    let value = reloaded
        .get_xattr("/xattr_file", b"user.color")
        .await
        .unwrap();
    assert_eq!(value, Some(b"green".to_vec()));

    fs.remove_xattr("/xattr_file", b"trusted.flag")
        .await
        .unwrap();
    let value = fs.get_xattr("/xattr_file", b"trusted.flag").await.unwrap();
    assert_eq!(value, None);

    let names = fs.list_xattrs("/xattr_file").await.unwrap();
    assert_eq!(names, vec![b"user.color".to_vec()]);

    fs.remove_xattr("/xattr_file", b"user.color").await.unwrap();
    let names = fs.list_xattrs("/xattr_file").await.unwrap();
    assert!(names.is_empty());
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_xattr_invalid_name_and_missing_attr() {
    let fs = load_test_disk1_rw().await;
    create_empty_file(&fs).await;

    let err = fs
        .set_xattr("/xattr_file", b"invalid", b"value")
        .await
        .unwrap_err();
    assert!(matches!(err, Ext4Error::InvalidXattrName));

    let err = fs
        .remove_xattr("/xattr_file", b"user.missing")
        .await
        .unwrap_err();
    assert!(matches!(err, Ext4Error::NotFound));
}
