// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// In addition to being used as a regular module in lib.rs, this module
// is used in `tests` via the `include!` macro.

use crate::{Ext4, Ext4Read, Ext4Write};
use async_trait::async_trait;
use core::fmt::{Display, Formatter};
use std::error::Error as StdError;
use std::fmt;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex};

// Simple error for MemWriter failures.
#[derive(Debug)]
pub(crate) struct MemWriterError;
impl Display for MemWriterError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "mem writer error")
    }
}
impl StdError for MemWriterError {}

// Reader+Writer backed by a shared Arc<Mutex<Vec<u8>>> to verify persistence.
pub(crate) struct MemRw(pub(crate) Arc<Mutex<Vec<u8>>>);

#[async_trait]
impl Ext4Read for MemRw {
    async fn read(
        &self,
        start_byte: u64,
        dst: &mut [u8],
    ) -> Result<(), Box<dyn StdError + Send + Sync + 'static>> {
        let guard = self.0.lock().unwrap();
        let start = start_byte as usize;
        let end = start.checked_add(dst.len()).ok_or_else(|| {
            Box::new(MemWriterError)
                as Box<dyn StdError + Send + Sync + 'static>
        })?;
        if end > guard.len() {
            return Err(Box::new(MemWriterError));
        }
        dst.copy_from_slice(&guard[start..end]);
        Ok(())
    }
}

#[async_trait]
impl Ext4Write for MemRw {
    async fn write(
        &self,
        start_byte: u64,
        src: &[u8],
    ) -> Result<(), Box<dyn StdError + Send + Sync + 'static>> {
        let mut guard = self.0.lock().unwrap();
        let start = start_byte as usize;
        let end = start.checked_add(src.len()).ok_or_else(|| {
            Box::new(MemWriterError)
                as Box<dyn StdError + Send + Sync + 'static>
        })?;
        if end > guard.len() {
            return Err(Box::new(MemWriterError));
        }
        guard[start..end].copy_from_slice(src);
        Ok(())
    }
}

/// Decompress a file with zstd, then load it into an `Ext4`.
pub(crate) async fn load_compressed_filesystem(name: &str) -> Ext4 {
    // This function executes quickly, so don't bother caching.
    let output = std::process::Command::new("zstd")
        .args([
            "--decompress",
            // Write to stdout and don't delete the input file.
            "--stdout",
            &format!("test_data/{name}"),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    Ext4::load(Box::new(output.stdout)).await.unwrap()
}

/// Decompress a file with zstd, then load it into an `Ext4`.
pub(crate) async fn load_compressed_filesystem_rw(
    name: &str,
) -> (Ext4, Arc<Mutex<Vec<u8>>>) {
    // This function executes quickly, so don't bother caching.
    let output = std::process::Command::new("zstd")
        .args([
            "--decompress",
            // Write to stdout and don't delete the input file.
            "--stdout",
            &format!("test_data/{name}"),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    let data: Vec<u8> = output.stdout;
    let shared = Arc::new(Mutex::new(data));
    let reader = Box::new(MemRw(shared.clone())) as Box<dyn Ext4Read>;
    let writer = Some(Box::new(MemRw(shared.clone())) as Box<dyn Ext4Write>);

    (
        Ext4::load_with_writer(reader, writer).await.unwrap(),
        shared,
    )
}

pub(crate) async fn load_test_disk1() -> Ext4 {
    load_compressed_filesystem("test_disk1.bin.zst").await
}

#[allow(unreachable_pub)]
pub struct Ext4Wrapper(Ext4, Arc<Mutex<Vec<u8>>>);

impl Deref for Ext4Wrapper {
    type Target = Ext4;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for Ext4Wrapper {
    fn drop(&mut self) {
        fsck_ext4_arc_image(&self.1);
    }
}

#[allow(unused)]
pub(crate) async fn load_test_disk1_rw() -> Ext4Wrapper {
    let (fs, data) = load_compressed_filesystem_rw("test_disk1.bin.zst").await;
    Ext4Wrapper(fs, data)
}

#[allow(unused)]
pub(crate) async fn load_test_disk1_rw_no_fsck() -> Ext4 {
    load_compressed_filesystem_rw("test_disk1.bin.zst").await.0
}

/// Validate that the provided filesystem image is a consistent ext4 filesystem by
/// invoking `fsck.ext4`.
///
/// Why this exists: `fsck.ext4` needs a path/device it can open; piping bytes on
/// stdin doesn't work. We therefore materialize the bytes into a temporary file.
///
/// Set `EXT4_RS_RUN_FSCK=1` to enable.
/// On macOS, ext4 tooling is uncommon. If `fsck.ext4` isn't available locally,
/// you can set `EXT4_RS_FSCK_DOCKER=1` to run the check via Docker using the
/// `e2fsprogs/e2fsprogs` image.
#[allow(dead_code)]
pub(crate) fn fsck_ext4_arc_image(image: &Arc<Mutex<Vec<u8>>>) {
    if std::env::var_os("EXT4_RS_RUN_FSCK").is_none() {
        return;
    }

    let bytes = { image.lock().unwrap().clone() };

    let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
    std::io::Write::write_all(&mut tmp, &bytes)
        .expect("write filesystem image");
    tmp.as_file().sync_all().expect("sync filesystem image");

    fsck_ext4_path(tmp.path());

    drop(tmp);
}

#[allow(dead_code)]
fn fsck_ext4_path(path: &Path) {
    let path_str = path.to_string_lossy();
    let args = ["-f", "-n", path_str.as_ref()];

    let local = std::process::Command::new("fsck.ext4").args(args).output();

    if let Ok(output) = local {
        if output.status.success() {
            return;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "fsck.ext4 reported errors (status={:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
            output.status.code(),
            stdout,
            stderr
        );
    }

    if std::env::var_os("EXT4_RS_FSCK_DOCKER").is_some() {
        let output = std::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--read-only",
                "-v",
                &format!("{}:/img:ro", path.display()),
                "e2fsprogs/e2fsprogs",
                "fsck.ext4",
                "-f",
                "-n",
                "/img",
            ])
            .output()
            .expect("run docker fsck.ext4");

        if output.status.success() {
            return;
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "docker fsck.ext4 reported errors (status={:?})\n--- stdout ---\n{}\n--- stderr ---\n{}",
            output.status.code(),
            stdout,
            stderr
        );
    }

    // If we got here, local fsck.ext4 wasn't available and docker fallback isn't enabled.
    // Keep this as a panic because the caller explicitly opted in via EXT4_RS_RUN_FSCK.
    panic!(
        "EXT4_RS_RUN_FSCK=1 but `fsck.ext4` couldn't be executed. Install e2fsprogs or set EXT4_RS_FSCK_DOCKER=1. Path was: {}",
        path.display()
    );
}
