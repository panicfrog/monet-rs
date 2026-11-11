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
mod roots;
mod upload;
mod ws;

pub use ffi::bridge;

use futures_lite::future::race;
use monet_core::body::Completeness;
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::pin::PinConfig;
use monet_core::request::Request;
use monet_core::response::Response;
use monet_core::transport::{BoxFuture, Transport, WebSocketTransport};
use monet_core::ws::WebSocketStream;

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

    /// Registers the app-supplied DER roots the shim anchors
    /// server-trust challenges with — additional to the system's, the
    /// network_security_config semantics (configureTrust).
    ///
    /// Process-global last-wins, like [`Self::with_pins`]: every
    /// construction registers (empty list included), so a clear reaches
    /// the shim.
    pub fn with_roots(self, roots: Vec<Vec<u8>>) -> Self {
        roots::register(roots);
        self
    }
}

impl Default for UrlsessionTransport {
    fn default() -> Self {
        Self::new()
    }
}

/// The rig's observation channel: appends to $HOME/monet-valve.log in
/// the app container (stderr reaches nowhere capturable on iOS, os_log
/// needs the os_log API, and NSLog perturbs the delegate races — a
/// plain file is the non-perturbing channel).
fn valve_log(line: &str) {
    use std::io::Write as _;
    static ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
        std::env::var("MONET_VALVE_LOG").is_ok()
            || std::env::args().any(|arg| arg == "MONET_VALVE_LOG=1")
    });
    if !*ENABLED {
        return;
    }
    if let Ok(home) = std::env::var("HOME") {
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(format!("{home}/monet-valve.log"))
        {
            let ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let _ = writeln!(file, "{ms} {line}");
        }
    }
}

impl Transport for UrlsessionTransport {
    fn execute(&self, mut req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>> {
        // Per-request overrides win; the instance values are the
        // defaults (nitro's per-request-only knobs ride the same
        // per-call control-plane parameters).
        let follow_redirects = req.follow_redirects.unwrap_or(self.follow_redirects);
        let timeout_ms = req.timeout_ms.unwrap_or(self.timeout_ms);
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            // v2/W2c: the probe decides the upload's shape without
            // parking — a complete body buffers (httpBody carries
            // Content-Length and redirects can replay it), a live one
            // streams through the upload pipe
            // (uploadTask(withStreamedRequest:) + needNewBodyStream —
            // chunked on the wire, not replayable), and a failure that
            // already arrived fails the request outright.
            enum Upload {
                Buffered(Vec<u8>),
                /// The drained prefix; the live stream follows through
                /// the feeder.
                Streamed(Vec<u8>),
            }
            let upload = match req.body.take_complete() {
                Completeness::Complete(bytes) => {
                    valve_log(&format!("PROBE complete bytes={}", bytes.len()));
                    Upload::Buffered(bytes)
                }
                Completeness::Live { drained } => {
                    valve_log(&format!("PROBE live drained={}", drained.len()));
                    Upload::Streamed(drained)
                }
                Completeness::Failed(error) => return Err(error),
            };

            let (id, head_rx) = registry::allocate(cancel.clone());
            for (name, value) in req.headers.iter_flattened() {
                bridge::transport_add_header(id, name.as_bytes(), value.as_bytes());
            }
            match &upload {
                Upload::Buffered(bytes) => {
                    if !bytes.is_empty() {
                        bridge::transport_set_body(id, bytes);
                    }
                }
                Upload::Streamed(_) => bridge::transport_set_streaming(id),
            }
            bridge::transport_start(
                id,
                req.url.as_str().as_bytes(),
                req.method.as_str().as_bytes(),
                follow_redirects,
                timeout_ms,
            );
            // The feeder follows the start: the Swift pipe must exist
            // before the first write can land.
            if let Upload::Streamed(drained) = upload {
                upload::spawn_feeder(id, req.body, cancel.clone(), drained);
            }

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

impl WebSocketTransport for UrlsessionTransport {
    fn connect(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<WebSocketStream>> {
        ws::connect(req, cancel)
    }
}
