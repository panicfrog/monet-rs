//! Android platform HTTP transport: `android.net.http.HttpEngine` (the
//! system-provided Cronet, API 34+) behind monet-core's `Transport`.
//!
//! The engine is constructed over JNI from a globally-initialized JVM +
//! app Context (see [`init`]); requests run through a small Kotlin bridge
//! class that forwards Cronet's Java callbacks into this crate — the
//! bridge stays mechanical, the state machine lives here (the inverted
//! shape of monet-urlsession's Swift shim). Host builds compile to a stub
//! that rejects construction: the platform API only exists on Android,
//! and monet-rustls is the cross-version fallback.
//!
//! See docs/plans/2026-11-monet-httpengine.md for the design record.

#![cfg_attr(target_os = "android", allow(unsafe_code))] // JNI boundary.

#[cfg(any(target_os = "android", test))]
mod registry;

#[cfg(target_os = "android")]
mod bridge;
#[cfg(target_os = "android")]
mod jni;

use monet_core::cancel::CancelToken;
#[cfg_attr(target_os = "android", allow(unused_imports))]
use monet_core::error::{Error, Result};
use monet_core::request::Request;
use monet_core::response::Response;
use monet_core::transport::{BoxFuture, Transport};

#[cfg(target_os = "android")]
pub use jni::init;

#[cfg(not(target_os = "android"))]
pub use host_stub::init;

/// A [`Transport`] over the platform HttpEngine. Requires [`init`] to
/// have run with a JVM and app Context; on API < 34 (or ROMs without the
/// implementation) construction fails and callers should fall back to
/// monet-rustls.
pub struct HttpengineTransport {
    #[cfg(target_os = "android")]
    engine: jni::EngineHandle,
    #[cfg(not(target_os = "android"))]
    _platform: (),
}

impl HttpengineTransport {
    /// Builds the engine (Brotli on, pins from `pins` installed natively
    /// via `addPublicKeyPins`).
    #[cfg(target_os = "android")]
    pub fn new(pins: monet_core::pin::PinConfig) -> Result<Self> {
        let engine = jni::build_engine(&pins)?;
        Ok(HttpengineTransport { engine })
    }

    #[cfg(not(target_os = "android"))]
    pub fn new(_pins: monet_core::pin::PinConfig) -> Result<Self> {
        Err(Error::transport(
            "httpengine: the platform HttpEngine only exists on Android (API 34+)",
        ))
    }

    /// The engine's version string (e.g. "121.0.6167.71") — surfaced by
    /// the demo so a misconfigured build is visible at a glance.
    #[cfg(target_os = "android")]
    pub fn engine_version(&self) -> String {
        jni::engine_version(&self.engine)
    }
}

#[cfg(target_os = "android")]
impl Drop for HttpengineTransport {
    fn drop(&mut self) {
        // Every engine's Cronet context holds a broadcast receiver the
        // system keeps strongly referenced until shutdown() — dropping
        // without it leaks one per engine and the process dies at the
        // 1000-receiver cap. Safe by construction: embedders share one
        // transport behind an Arc, so this runs only after the last
        // in-flight request's pump has let go of its clone. (Not on
        // EngineHandle: execute() duplicates it per request, and those
        // must never shut the engine down mid-flight.)
        jni::shutdown_engine(&self.engine);
    }
}

impl Transport for HttpengineTransport {    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>> {
        #[cfg(target_os = "android")]
        {
            jni::execute(&self.engine, req, cancel)
        }
        #[cfg(not(target_os = "android"))]
        {
            let _ = (req, cancel);
            Box::pin(async { Err(Error::transport("httpengine: Android only")) })
        }
    }
}

#[cfg(not(target_os = "android"))]
mod host_stub {
    /// Host stub: the real signature takes the JVM and the app Context.
    pub fn init() -> monet_core::Result<()> {
        Err(monet_core::Error::transport(
            "httpengine: the platform HttpEngine only exists on Android (API 34+)",
        ))
    }
}
