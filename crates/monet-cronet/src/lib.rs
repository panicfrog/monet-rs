//! Safe Cronet transport for monet-core (Android).
//!
//! [`CronetEngine`] owns the engine lifecycle (including public-key pin
//! injection from [`monet_core::pin::PinConfig`]); [`CronetTransport`]
//! implements [`monet_core::Transport`] on top of it, delivering the
//! response head through a boxed future and streaming the body chunk by
//! chunk.
//!
//! Host tests link a fake implementation of the Cronet C symbols
//! (`tests/fake`); on Android, enable the `link` feature and provide
//! `libcronet.so` through the `cronet-embedded` AAR. (There is no doctest
//! here on purpose: a doctest binary links this crate without the fake and
//! without a real libcronet.)

#![allow(unsafe_code)] // FFI boundary: this crate wraps the Cronet C API.

mod callback;
mod engine;
mod executor;
mod request;
mod transport;

pub use engine::{CronetEngine, EngineConfig, HttpCache};
pub use transport::CronetTransport;
