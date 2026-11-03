//! Native iOS/macOS transport for monet-core: URLSession behind a Swift
//! shim, bridged with Swift↔C++ interop over cxx.
//!
//! The Swift shim owns the delegate state machine (per nitro-fetch's
//! `NitroBufferedTaskDelegate` pattern: one shared session, a delegate per
//! task, events pushed straight into the Rust data plane); this crate maps
//! that narrow callback face onto [`Transport`]. Development and testing
//! happen on the macOS host (URLSession exists there); iOS cross-compiles
//! with the same build script. See `docs/plans/2026-11-monet-urlsession.md`
//! for the design record.

#![allow(unsafe_code)] // FFI boundary: this crate bridges Rust and Swift.

mod ffi;
mod pin;
mod registry;

pub use ffi::bridge;

use futures_lite::StreamExt;
use futures_lite::future::race;
use monet_core::body::Body;
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::pin::PinConfig;
use monet_core::request::Request;
use monet_core::response::Response;
use monet_core::transport::{BoxFuture, Transport};

/// A [`Transport`] over URLSession via the Swift shim.
///
/// One shared URLSession (default configuration, system URLCache —
/// nitro-fetch parity) serves every request; per-request state lives in
/// the shim's task delegates and the Rust registry.
pub struct UrlsessionTransport {
    follow_redirects: bool,
    /// Per-request timeout; 0 keeps URLSession's default (60s).
    timeout_ms: u64,
}

impl UrlsessionTransport {
    /// Follows redirects with URLSession's default timeout.
    pub fn new() -> Self {
        UrlsessionTransport {
            follow_redirects: true,
            timeout_ms: 0,
        }
    }

    /// Sets whether 3xx responses are followed (`true` by default).
    pub fn with_follow_redirects(mut self, follow: bool) -> Self {
        self.follow_redirects = follow;
        self
    }

    /// Sets a per-request idle timeout in milliseconds, applied as the
    /// URLRequest's `timeoutInterval` (nitro-fetch's per-request-only
    /// approach). 0 keeps the system default.
    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// Registers the pinning configuration the Swift shim enforces on
    /// server-trust challenges (host matching lives in monet-core, so
    /// the semantics match the Rust transports exactly).
    ///
    /// The shim is process-global — one shared URLSession — so this
    /// registration is too: the last call wins. A config that failed to
    /// parse never gets here; the loader is expected to fail closed
    /// instead of constructing the transport.
    pub fn with_pins(self, pins: PinConfig) -> Self {
        pin::register(pins);
        self
    }
}

impl Default for UrlsessionTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for UrlsessionTransport {
    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>> {
        let follow_redirects = self.follow_redirects;
        let timeout_ms = self.timeout_ms;
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            // v1 uploads are fully buffered (nitro-fetch parity: the body
            // is collected before the task starts).
            let body = buffer_body(req.body).await?;

            let (id, head_rx) = registry::allocate(cancel.clone());
            for (name, value) in req.headers.iter_flattened() {
                bridge::transport_add_header(id, name.as_bytes(), value.as_bytes());
            }
            if !body.is_empty() {
                bridge::transport_set_body(id, &body);
            }
            bridge::transport_start(
                id,
                req.url.as_str().as_bytes(),
                req.method.as_str().as_bytes(),
                follow_redirects,
                timeout_ms,
            );

            enum Step<T> {
                Head(T),
                Cancelled,
            }
            match race(async { Step::Head(head_rx.await) }, async {
                cancel.cancelled().await;
                Step::Cancelled
            })
            .await
            {
                Step::Head(Ok(response)) => response,
                // The state was dropped without a terminal event (only the
                // registry's defensive paths do this) — report rather than
                // hang.
                Step::Head(Err(_)) => Err(Error::transport(
                    "urlsession: request state dropped before the response head",
                )),
                Step::Cancelled => {
                    bridge::transport_cancel(id);
                    registry::remove(id);
                    Err(Error::Cancelled)
                }
            }
        })
    }
}

/// Collects a request body into one buffer for the buffered-upload v1.
async fn buffer_body(mut body: Body) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf)
}
