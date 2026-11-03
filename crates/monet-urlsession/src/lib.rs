//! Native iOS transport for monet-core: URLSession behind a Swift shim,
//! bridged with Swift↔C++ interop over cxx.
//!
//! Development and testing happen on the macOS host (URLSession exists
//! there); iOS cross-compiles with the same build script. See
//! `docs/plans/2026-11-monet-urlsession.md` for the design record.

#![allow(unsafe_code)] // FFI boundary: this crate bridges Rust and Swift.

mod ffi;

pub use ffi::bridge;
