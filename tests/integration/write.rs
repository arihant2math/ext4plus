// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use ext4plus::{
    Ext4Error, File, FileType, FollowSymlinks, Inode, InodeCreationOptions,
    InodeFlags, InodeMode, Path, write_at,
};
use tokio;

use super::test_util::{load_compressed_filesystem, load_test_disk1_rw};

#[tokio::test]
async fn test_write_requires_writer() {
    // Load filesystem without writer.
    let fs = load_compressed_filesystem("test_disk1.bin.zst").await;

    // Open a small file and attempt to write.
    let mut file = fs.open("/small_file").await.unwrap();
    let err = file.write_bytes(b"ABC").await.unwrap_err();
    assert!(matches!(err, Ext4Error::Readonly));
}

#[tokio::test]
async fn test_write_into_hole() {
    // Load filesystem with writer.
    let fs = load_test_disk1_rw().await;

    // Open the file with holes. The first two blocks are holes.
    let mut file = fs.open("/holes").await.unwrap();

    // Try to write at the start (in a hole). Should be Readonly.
    let write = file.write_bytes(b"XYZ").await.unwrap();
    assert_eq!(write, 3);
    let data = fs.read("/holes").await.unwrap();
    assert!(data.starts_with(b"XYZ"));
}

#[tokio::test]
async fn test_write_basic() {
    // Load filesystem with writer.
    let fs = load_test_disk1_rw().await;

    // Small file is "hello, world!" (13 bytes) and fits in a single block.
    let mut file = fs.open("/small_file").await.unwrap();

    // Seek near end and attempt to write more than remaining.
    file.seek_to(12).await.unwrap();
    let written = file.write_bytes(b". We're writing").await.unwrap();

    // Everything should be written up
    assert_eq!(written, 15);
    assert_eq!(file.position(), 27);
    file.seek_to(0).await.unwrap();
    // Verify file contents
    let mut buf = vec![0u8; 27];
    let n = file.read_bytes(&mut buf).await.unwrap();
    assert_eq!(n, 27);
    assert_eq!(&buf, b"hello, world. We're writing");
    // File contents should be "hello, worABCDEFGHIJ"
    let data = fs.read("/small_file").await.unwrap();
    assert_eq!(&data, b"hello, world. We're writing");
}

#[tokio::test]
async fn test_write_persists_data() {
    // Load filesystem with shared reader/writer to the same buffer.
    let fs = load_test_disk1_rw().await;

    // Open small_file and write within allocated space.
    let mut file = fs.open("/small_file").await.unwrap();
    // Overwrite first 5 bytes with "HELLO".
    file.seek_to(0).await.unwrap();
    let n = file.write_bytes(b"HELLO").await.unwrap();
    assert_eq!(n, 5);

    // Read back the file and verify the change persisted.
    let data = fs.read("/small_file").await.unwrap();
    assert!(data.starts_with(b"HELLO"));
}

#[tokio::test]
async fn test_inode_modification_time() {
    let fs = load_test_disk1_rw().await;

    let mut inode = fs
        .path_to_inode(
            Path::try_from("/empty_file").unwrap(),
            FollowSymlinks::All,
        )
        .await
        .unwrap();
    let new_atime = core::time::Duration::new(6000, 0);
    let now = core::time::Duration::new(5000, 0);
    inode.set_atime(new_atime);
    inode.set_mtime(now);
    inode.write(&fs).await.unwrap();
    // Reload inode to verify change persisted.
    let reloaded = fs
        .path_to_inode(
            Path::try_from("/empty_file").unwrap(),
            FollowSymlinks::All,
        )
        .await
        .unwrap();
    assert_eq!(reloaded.metadata().mtime, now);
    assert_eq!(reloaded.metadata().atime, new_atime);
}

#[tokio::test]
async fn test_inode_creation() {
    let fs = load_test_disk1_rw().await;

    // Create a new file in the root directory.
    let mut new_inode = fs
        .create_inode(InodeCreationOptions {
            file_type: FileType::Regular,
            mode: InodeMode::S_IRUSR | InodeMode::S_IWUSR | InodeMode::S_IFREG,
            uid: 0,
            gid: 0,
            time: Default::default(),
            flags: InodeFlags::INLINE_DATA,
        })
        .await
        .unwrap();
    assert_eq!(new_inode.metadata().file_type, FileType::Regular);
    assert_eq!(
        new_inode.metadata().mode,
        InodeMode::S_IRUSR | InodeMode::S_IWUSR | InodeMode::S_IFREG
    );
    assert_eq!(new_inode.metadata().uid, 0);
    assert_eq!(new_inode.metadata().gid, 0);
    let root_inode = fs
        .path_to_inode(Path::try_from("/").unwrap(), FollowSymlinks::All)
        .await
        .unwrap();
    // Link the new inode into the root directory.
    fs.link(&root_inode, "new_file".to_string(), &mut new_inode)
        .await
        .unwrap();
    // Ensure the new file is visible at the expected path.
    let new_file_inode = fs
        .path_to_inode("/new_file".try_into().unwrap(), FollowSymlinks::All)
        .await
        .unwrap();
    assert_eq!(new_file_inode.index, new_inode.index);
    assert_eq!(new_file_inode.links_count(), 1);
}

#[tokio::test]
async fn test_inode_deletion() {
    let fs = load_test_disk1_rw().await;

    let root_inode = fs
        .path_to_inode(Path::try_from("/").unwrap(), FollowSymlinks::All)
        .await
        .unwrap();
    let empty_inode = fs
        .path_to_inode("/empty_file".try_into().unwrap(), FollowSymlinks::All)
        .await
        .unwrap();
    let inode = fs
        .unlink(&root_inode, "empty_file".to_string(), empty_inode)
        .await
        .unwrap();
    assert!(inode.is_none());
    // Ensure the file is no longer visible.
    let err = fs
        .path_to_inode("/empty_file".try_into().unwrap(), FollowSymlinks::All)
        .await
        .unwrap_err();
    assert!(matches!(err, Ext4Error::NotFound));
}

#[tokio::test]
async fn test_new_file_grow() {
    let fs = load_test_disk1_rw().await;
    let new_inode = fs
        .create_inode(InodeCreationOptions {
            file_type: FileType::Regular,
            mode: InodeMode::S_IRUSR | InodeMode::S_IWUSR | InodeMode::S_IFREG,
            uid: 0,
            gid: 0,
            time: Default::default(),
            flags: InodeFlags::INLINE_DATA,
        })
        .await
        .unwrap();
    let index = new_inode.index;
    let mut file = File::open_inode(&fs, new_inode).unwrap();
    let data = b"Hello, world! This file will grow as we write to it.";
    let n = file.write_bytes(data).await.unwrap();
    assert_eq!(n, data.len());
    // Read back the inode and verify new length.
    let inode = Inode::read(&fs, index).await.unwrap();
    assert_eq!(inode.size_in_bytes(), data.len() as u64);
    let mut file = File::open_inode(&fs, inode).unwrap();
    let mut buf = vec![0u8; data.len()];
    let n = file.read_bytes(&mut buf).await.unwrap();
    assert_eq!(n, data.len());
    assert_eq!(&buf, data);
}

#[tokio::test]
async fn test_new_file_grow2() {
    let fs = load_test_disk1_rw().await;
    let mut new_inode = fs
        .create_inode(InodeCreationOptions {
            file_type: FileType::Regular,
            mode: InodeMode::S_IRUSR | InodeMode::S_IWUSR | InodeMode::S_IFREG,
            uid: 0,
            gid: 0,
            time: Default::default(),
            flags: InodeFlags::INLINE_DATA,
        })
        .await
        .unwrap();
    let index = new_inode.index;
    let data = b"Hello, world! This file will grow as we write to it.";
    let _ = write_at(&fs, &mut new_inode, data, 0).await.unwrap();
    let data = b"Hello, world! This file will grow as we write to it.";
    let n = write_at(&fs, &mut new_inode, data, 0).await.unwrap();
    assert_eq!(n, data.len());
    let replacement_data = b" and can also be appended to.";
    let n =
        write_at(&fs, &mut new_inode, replacement_data, data.len() as u64 - 1)
            .await
            .unwrap();
    assert_eq!(n, replacement_data.len());
    // Read back the inode and verify new length.
    let data = b"Hello, world! This file will grow as we write to it and can also be appended to.";
    let inode = Inode::read(&fs, index).await.unwrap();
    assert_eq!(inode.size_in_bytes(), data.len() as u64);
    let mut file = File::open_inode(&fs, inode).unwrap();
    let mut buf = vec![0u8; data.len()];
    let n = file.read_bytes(&mut buf).await.unwrap();
    assert_eq!(n, data.len());
    assert_eq!(&buf, data);
}

#[tokio::test]
async fn test_existing_file_grow() {
    let fs = load_test_disk1_rw().await;
    let mut inode = fs
        .path_to_inode("/small_file".try_into().unwrap(), FollowSymlinks::All)
        .await
        .unwrap();
    write_at(&fs, &mut inode, b" Adding more data to the small file.", 13)
        .await
        .unwrap();
    let data = b"hello, world! Adding more data to the small file.";
    // Read back the inode and verify new length.
    let inode = Inode::read(&fs, inode.index).await.unwrap();
    assert_eq!(inode.size_in_bytes(), data.len() as u64);
    let mut file = File::open_inode(&fs, inode).unwrap();
    let mut buf = vec![0u8; data.len()];
    let n = file.read_bytes(&mut buf).await.unwrap();
    assert_eq!(n, data.len());
    assert_eq!(&buf, data);
}

#[tokio::test]
async fn test_multi_block_write() {
    let fs = load_test_disk1_rw().await;
    let mut inode = fs
        .path_to_inode("/small_file".try_into().unwrap(), FollowSymlinks::All)
        .await
        .unwrap();
    let data = vec![b'A'; 10000];
    let mut total_written = 0;
    while total_written < data.len() {
        let n = write_at(
            &fs,
            &mut inode,
            &data[total_written..],
            total_written as u64,
        )
        .await
        .unwrap();
        assert!(n > 0);
        total_written += n;
    }
    assert_eq!(total_written, data.len());
    // Read back the inode and verify new length.
    let inode = Inode::read(&fs, inode.index).await.unwrap();
    assert_eq!(inode.size_in_bytes(), data.len() as u64);
    let mut file = File::open_inode(&fs, inode).unwrap();
    let mut buf = vec![0u8; data.len()];
    let mut total_read = 0;
    while total_read < data.len() {
        let n = file.read_bytes(&mut buf[total_read..]).await.unwrap();
        assert!(n > 0);
        total_read += n;
    }
    assert_eq!(total_read, data.len());
    assert_eq!(&buf, &data);
}

#[tokio::test]
async fn test_init_directory_creates_dot_and_dotdot() {
    let fs = load_test_disk1_rw().await;
    let root = fs.read_root_inode().await.unwrap();

    // Create a new directory inode and initialize it.
    let mut dir_inode = fs
        .create_inode(InodeCreationOptions {
            file_type: FileType::Directory,
            mode: InodeMode::S_IRUSR
                | InodeMode::S_IWUSR
                | InodeMode::S_IXUSR
                | InodeMode::S_IFDIR,
            uid: 0,
            gid: 0,
            time: Default::default(),
            flags: InodeFlags::empty(),
        })
        .await
        .unwrap();

    ext4plus::init_directory(&fs, &mut dir_inode, root.index)
        .await
        .unwrap();

    // Link it into the root so it becomes reachable via path resolution.
    fs.link(&root, "new_dir".to_string(), &mut dir_inode)
        .await
        .unwrap();

    // Open the directory and verify '.' and '..'.
    let opened = fs.open(Path::new("/new_dir")).await.unwrap();

    let dot = ext4plus::get_dir_entry_inode_by_name(
        &fs,
        opened.inode(),
        ext4plus::DirEntryName::try_from(".").unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(dot.index, opened.inode().index);

    let dotdot = ext4plus::get_dir_entry_inode_by_name(
        &fs,
        opened.inode(),
        ext4plus::DirEntryName::try_from("..").unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(dotdot.index, root.index);
}
