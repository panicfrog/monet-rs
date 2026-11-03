//! Raw FFI declarations for the Cronet C API.
//!
//! Generated from the vendored headers in `include/` (Chromium
//! 143.0.7445.0, the last line whose release ships C headers byte-identical
//! through 145). Zero safe wrapping — see the `monet-cronet` crate for that.
//!
//! Linking is opt-in via the `link` feature (see `build.rs`); by default
//! nothing is linked, so host test builds can substitute a fake
//! implementation of these symbols.
#![allow(unsafe_code)] // FFI is this crate's entire purpose.
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::all)]

include!("bindings.rs");
