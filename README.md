# ext4-rs

**Note: This project is a fork of the original and has different design goals (including async and write support).**

This repository provides a Rust crate that allows read-only access to an
[ext4] filesystem. It also works with [ext2] and [ext3] filesystems.
The crate is `no_std`, so it can be used in embedded or osdev contexts. However, it does require `alloc`.

[ext2]: https://en.wikipedia.org/wiki/Ext2
[ext3]: https://en.wikipedia.org/wiki/Ext3
[ext4]: https://en.wikipedia.org/wiki/Ext4

## Design Goals

In order of importance:

1. Correct
   * All valid ext2/ext3/ext4 filesystems should be readable.
   * Invalid data should never cause crashes, panics, or non-terminating loops.
   * No `unsafe` code in the main package (it is allowed in dependencies).
   * Well tested.
2. Easy to use
   * The API should follow the conventions of [`std::fs`] where possible.
3. Good performance
   * Performance should not come at the expense of correctness or ease of use.
4. Writeable

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.

## Contributing

See the [code of conduct] and [contributing.md].

Bug reports and PRs are welcome!

[code of conduct]: docs/code-of-conduct.md
[contributing.md]: docs/contributing.md
