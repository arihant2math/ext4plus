// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::test_util::{
    Ext4Wrapper, load_compressed_filesystem, load_compressed_filesystem_rw,
    load_test_disk1_rw,
};
use ext4plus::path::PathBuf;
#[cfg(not(feature = "sync"))]
use ext4plus::prelude::AsyncIterator;
use ext4plus::prelude::{
    Dir, DirEntryName, Ext4Error, File, FileType, FollowSymlinks, Inode,
    InodeCreationOptions, InodeFlags, InodeMode, Path, truncate, write_at,
};

#[maybe_async::maybe_async]
pub async fn load_ext2_rw() -> Ext4Wrapper {
    let (fs, data) =
        load_compressed_filesystem_rw("test_disk_ext2.bin.zst").await;
    Ext4Wrapper(fs, data)
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_write_requires_writer() {
    // Load filesystem without writer.
    let fs = load_compressed_filesystem("test_disk1.bin.zst").await;

    // Open a small file and attempt to write.
    let mut file = fs.open("/small_file").await.unwrap();
    let err = file.write_bytes(b"ABC").await.unwrap_err();
    assert!(matches!(err, Ext4Error::Readonly));
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_write_into_hole() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];

    for fs in fses {
        // Open the file with holes. The first two blocks are holes.
        let mut file = fs.open("/holes").await.unwrap();

        // Try to write at the start (in a hole). Should be Readonly.
        let write = file.write_bytes(b"XYZ").await.unwrap();
        assert_eq!(write, 3);
        let data = fs.read("/holes").await.unwrap();
        assert!(data.starts_with(b"XYZ"));
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_write_basic() {
    // Load filesystem with writer.
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];

    for fs in fses {
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
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_write_persists_data() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];

    for fs in fses {
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
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_inode_modification_time() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut inode = fs
            .path_to_inode(
                Path::try_from("/small_file").unwrap(),
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
                Path::try_from("/small_file").unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        assert_eq!(reloaded.metadata().mtime, now);
        assert_eq!(reloaded.metadata().atime, new_atime);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_inode_creation() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        // Create a new file in the root directory.
        let mut new_inode = fs
            .create_inode(InodeCreationOptions {
                file_type: FileType::Regular,
                mode: InodeMode::S_IRUSR
                    | InodeMode::S_IWUSR
                    | InodeMode::S_IFREG,
                uid: 0,
                gid: 0,
                time: Default::default(),
                flags: InodeFlags::empty(),
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
        let root_inode = fs.read_root_inode().await.unwrap();
        let mut root_dir = Dir::open_inode(&fs.0, root_inode).unwrap();
        // Link the new inode into the root directory.
        root_dir
            .link(DirEntryName::try_from(b"new_file").unwrap(), &mut new_inode)
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
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_inode_deletion() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];

    for fs in fses {
        let root_inode = fs
            .path_to_inode(Path::try_from("/").unwrap(), FollowSymlinks::All)
            .await
            .unwrap();
        let root_dir = Dir::open_inode(&fs.0, root_inode).unwrap();
        let mut empty_inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        // Set dtime to make fsck happy
        empty_inode.set_dtime(core::time::Duration::new(5000, 0));
        empty_inode.write(&fs).await.unwrap();
        let inode = root_dir
            .unlink(DirEntryName::try_from(b"small_file").unwrap(), empty_inode)
            .await
            .unwrap();
        assert!(inode.is_none());
        // Ensure the file is no longer visible.
        let err = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Ext4Error::NotFound));
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_new_file_grow() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let new_inode = fs
            .create_inode(InodeCreationOptions {
                file_type: FileType::Regular,
                mode: InodeMode::S_IRUSR
                    | InodeMode::S_IWUSR
                    | InodeMode::S_IFREG,
                uid: 0,
                gid: 0,
                time: Default::default(),
                flags: InodeFlags::empty(),
            })
            .await
            .unwrap();
        let index = new_inode.index;
        let mut file = File::open_inode(&fs, new_inode).unwrap();
        // Add to dir
        let root_inode = fs.read_root_inode().await.unwrap();
        let mut root_dir = Dir::open_inode(&fs.0, root_inode).unwrap();
        root_dir
            .link(
                DirEntryName::try_from(b"new_file").unwrap(),
                &mut file.inode_mut(),
            )
            .await
            .unwrap();
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
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_new_file_grow2() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut new_inode = fs
            .create_inode(InodeCreationOptions {
                file_type: FileType::Regular,
                mode: InodeMode::S_IRUSR
                    | InodeMode::S_IWUSR
                    | InodeMode::S_IFREG,
                uid: 0,
                gid: 0,
                time: Default::default(),
                flags: InodeFlags::empty(),
            })
            .await
            .unwrap();
        let index = new_inode.index;
        // Add to dir
        let root_inode = fs.read_root_inode().await.unwrap();
        let mut root_dir = Dir::open_inode(&fs.0, root_inode).unwrap();
        root_dir
            .link(DirEntryName::try_from(b"new_file").unwrap(), &mut new_inode)
            .await
            .unwrap();
        let data = b"Hello, world! This file will grow as we write to it.";
        let _ = write_at(&fs, &mut new_inode, data, 0).await.unwrap();
        let data = b"Hello, world! This file will grow as we write to it.";
        let n = write_at(&fs, &mut new_inode, data, 0).await.unwrap();
        assert_eq!(n, data.len());
        let replacement_data = b" and can also be appended to.";
        let n = write_at(
            &fs,
            &mut new_inode,
            replacement_data,
            data.len() as u64 - 1,
        )
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
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_existing_file_grow() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
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
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_multi_block_write() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
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
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_massive_write() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        // Write 8 MiB
        for i in 0..8 {
            let data = vec![b'B'; 1024 * 1024];
            let mut total_written = 0;
            while total_written < data.len() {
                let n = write_at(
                    &fs,
                    &mut inode,
                    &data[total_written..],
                    total_written as u64 + (i * 1024 * 1024) as u64,
                )
                .await
                .unwrap();
                assert!(n > 0);
                total_written += n;
            }
            assert_eq!(total_written, data.len());
        }
        assert_eq!(inode.size_in_bytes(), 8 * 1024 * 1024);
        // Read back the inode and verify new length.
        let inode = Inode::read(&fs, inode.index).await.unwrap();
        assert_eq!(inode.size_in_bytes(), 8 * 1024 * 1024);
        // Read last 1 MiB and verify contents.
        let mut file = File::open_inode(&fs, inode).unwrap();
        file.seek_to(7 * 1024 * 1024).await.unwrap();
        let mut buf = vec![0u8; 1024 * 1024];
        let mut total_read = 0;
        while total_read < buf.len() {
            let n = file.read_bytes(&mut buf[total_read..]).await.unwrap();
            assert!(n > 0);
            total_read += n;
        }
        assert_eq!(total_read, buf.len());
        assert_eq!(&buf, &vec![b'B'; 1024 * 1024]);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_init_directory_creates_dot_and_dotdot() {
    let fses = [load_test_disk1_rw().await];
    for fs in fses {
        let mut root_dir =
            Dir::open_inode(&fs.0, fs.read_root_inode().await.unwrap())
                .unwrap();

        // Create a new directory inode and initialize it.
        let dir_inode = fs
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

        let mut dir_inode =
            Dir::init(fs.clone(), dir_inode, root_dir.inode().index)
                .await
                .unwrap();

        // Link it into the root so it becomes reachable via path resolution.
        root_dir
            .link(
                DirEntryName::try_from(b"new_dir").unwrap(),
                dir_inode.inode_mut(),
            )
            .await
            .unwrap();

        // Open the directory and verify '.' and '..'.
        let opened = fs
            .path_to_inode(Path::new("/new_dir"), FollowSymlinks::All)
            .await
            .unwrap();

        let dot = dir_inode
            .get_entry(DirEntryName::try_from(".").unwrap())
            .await
            .unwrap();
        assert_eq!(dot.index, opened.index);

        let dotdot = dir_inode
            .get_entry(DirEntryName::try_from("..").unwrap())
            .await
            .unwrap();
        assert_eq!(dotdot.index, root_dir.inode().index);
        for i in dir_inode.read_dir().unwrap().collect::<Vec<_>>().await {
            i.unwrap();
        }
        let len = dir_inode
            .read_dir()
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .len();
        assert_eq!(len, 2);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_truncate() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        let data = b"Hello, world! This file will be truncated.";
        write_at(&fs, &mut inode, data, 0).await.unwrap();
        // Truncate the file to a smaller size.
        truncate(&fs, &mut inode, 5).await.unwrap();
        let data = b"Hello";
        // Read back the inode and verify new length.
        let inode = Inode::read(&fs, inode.index).await.unwrap();
        assert_eq!(inode.size_in_bytes(), data.len() as u64);
        let mut file = File::open_inode(&fs, inode).unwrap();
        let mut buf = vec![0u8; data.len()];
        let n = file.read_bytes(&mut buf).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_truncate_grow() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        let data = b"hello, world!";
        // Truncate the file to a larger size.
        truncate(&fs, &mut inode, (data.len() + 5) as u64)
            .await
            .unwrap();
        let data = b"hello, world!\0\0\0\0\0";
        // Read back the inode and verify new length.
        let inode = Inode::read(&fs, inode.index).await.unwrap();
        assert_eq!(inode.size_in_bytes(), data.len() as u64);
        let mut file = File::open_inode(&fs, inode).unwrap();
        let mut buf = vec![0u8; data.len()];
        let n = file.read_bytes(&mut buf).await.unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf, data);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_truncate_to_zero() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let mut inode = fs
            .path_to_inode(
                "/small_file".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        // Truncate the file to zero.
        truncate(&fs, &mut inode, 0).await.unwrap();
        // Read back the inode and verify new length.
        let inode = Inode::read(&fs, inode.index).await.unwrap();
        assert_eq!(inode.size_in_bytes(), 0);
        let mut file = File::open_inode(&fs, inode).unwrap();
        let mut buf = vec![0u8; 10];
        let n = file.read_bytes(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_create_symlink() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let root_inode = fs.read_root_inode().await.unwrap();
        let mut root_dir = Dir::open_inode(&fs.0, root_inode).unwrap();
        fs.symlink(
            &mut root_dir,
            DirEntryName::try_from(b"link_to_small").unwrap(),
            PathBuf::try_from("/small_file").unwrap(),
            0,
            0,
            Default::default(),
        )
        .await
        .unwrap();
        // Verify the symlink is visible and points to the correct target.
        let link_inode = fs
            .path_to_inode(
                "/link_to_small".try_into().unwrap(),
                FollowSymlinks::All,
            )
            .await
            .unwrap();
        let mut file = File::open_inode(&fs, link_inode).unwrap();
        let mut buf = vec![0u8; 13];
        let n = file.read_bytes(&mut buf).await.unwrap();
        assert_eq!(n, 13);
        assert_eq!(&buf, b"hello, world!");
    }
}

#[maybe_async::test(
    feature = "sync",
    async(not(feature = "sync"), tokio::test)
)]
async fn test_many_dir_entries() {
    let fses = [load_test_disk1_rw().await, load_ext2_rw().await];
    for fs in fses {
        let root_inode = fs.read_root_inode().await.unwrap();
        let mut root_dir = Dir::open_inode(&fs.0, root_inode).unwrap();
        for i in 0..100 {
            let name = format!("file_{:03}", i);
            let mut new_inode = fs
                .create_inode(InodeCreationOptions {
                    file_type: FileType::Regular,
                    mode: InodeMode::S_IRUSR
                        | InodeMode::S_IWUSR
                        | InodeMode::S_IFREG,
                    uid: 0,
                    gid: 0,
                    time: Default::default(),
                    flags: InodeFlags::empty(),
                })
                .await
                .unwrap();
            root_dir
                .link(
                    DirEntryName::try_from(name.as_bytes()).unwrap(),
                    &mut new_inode,
                )
                .await
                .unwrap();
        }
        // Verify all entries are visible.
        for i in 0..100 {
            let name = format!("file_{:03}", i);
            let inode = fs
                .path_to_inode(Path::new(("/".to_string() + &name).as_bytes()), FollowSymlinks::All)
                .await
                .unwrap();
            assert_eq!(inode.metadata().file_type, FileType::Regular);
        }
    }
}
