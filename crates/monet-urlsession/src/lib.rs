//! Native iOS transport for monet-core: URLSession behind a Swift shim,
//! bridged with Swift↔C++ interop over cxx.
//!
//! Development and testing happen on the macOS host (URLSession exists
//! there); iOS cross-compiles with the same build script. See
//! `docs/plans/2026-11-monet-urlsession.md` for the design record.

mod ffi;

/// Data-plane entry point for the M1 spike: Swift calls this through the
/// generated cxx header.
fn monet_pong(id: u64) -> u64 {
    id
}

pub use ffi::ffi;
