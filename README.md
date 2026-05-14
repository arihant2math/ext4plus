# ext4plus

This repository provides a Rust crate that allows access and modification to an
[ext4] filesystem. It also works with [ext2] and [ext3] filesystems.
The crate is `no_std`, so it can be used in embedded or osdev contexts. However, it does require `alloc`.

[ext2]: https://en.wikipedia.org/wiki/Ext2
[ext3]: https://en.wikipedia.org/wiki/Ext3
[ext4]: https://en.wikipedia.org/wiki/Ext4

This project is a hard fork of [ext4-view-rs](https://github.com/nicholasbishop/ext4-view-rs/),
which is a read-only ext4 library.
The goal of this fork was to add write and async support, which required some significant changes to the API and internal design.

Additionally, due to the need for more low-level access to the filesystem, this crate exposes a greater API surface.

The two APIs that are exposed are the "raw" API, which is intended for OS drivers,
and the "std::fs" API, which replicates the standard library's `std::fs` API as closely as possible.

## Stability

This library is currently in pre-0.1.0 and in beta. The API is stable until 0.1.0.

A few experimental OSes are using this library as a filesystem driver.

Currently, there are known bugs, and it is recommended to use this library on ramdisks if writing.

## Sync vs Async

While this library is async-first, sync APIs are provided via the `sync` feature.
This has known limitations due to features needing to be additive, but it should be sufficient for most use cases.

## Limitations

- Lack of write support for journaling, although journaling can be read. It is recommended to disable journaling when using this library.
- Limited extended attribute (xattr) support. Small xattrs can be read and written when they fit in the inode body. Writing external xattr blocks is not supported yet.

Everything else should be fully supported, minus the features listed in the compatibility section below.

### Compatibility

incompatible:
------------

* filetype: **required**
* recover, extents, 64bit, bg_meta_csum, flex_bg, mmp: **yes**
* compression, journal_dev, meta_bg, ea_inode, dirdata, largedir, inline_data: **no**

compatible:
------------

* has_journal, ext_attr, dir_index: **yes**
* dir_prealloc, imagic_inodes, resize_inode: **no**

read-only:
------------

* sparse_super, large_file, huge_file, dir_nlink, extra_isize, metadata_csum: **yes**
* btree_dir, gdt_csum, quota, project_quotas, bigalloc: **no**

### Roadmap

Near-term goals (pre-0.1.0):

- stability/more testing

Goals that are also being worked on, but are not necessarily pre-0.1.0:

- fuller extended attribute support (external xattr block writes)
- inline data support
- journaling write support
- gdt checksum support

Future goals:

- quota support
- journal device support
- mmp support
- compression support

Non-goals:

- Support for undocumented features or ones that are not widely used, such as the "imagic inodes" feature.
- `no_alloc` support

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
