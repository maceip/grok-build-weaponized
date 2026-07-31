#![no_std]

//! Pure, allocation-free invariants shared by production code and symbolic
//! verification harnesses.
//!
//! Keep this crate dependency-free and compatible with the LLVM 16 Rust
//! toolchain pinned by `verification/klee/toolchain.env`. This lets KLEE
//! execute the exact functions used in production without compiling the full
//! workspace with an obsolete compiler.

mod streaming;

pub use streaming::{advance_stream_cursor, stream_window, StreamWindow};
