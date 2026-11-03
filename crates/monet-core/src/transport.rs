//! Transport traits: the seam between the core and platform network stacks.

use std::future::Future;
use std::pin::Pin;

use crate::cancel::CancelToken;
use crate::error::Result;
use crate::request::Request;
use crate::response::Response;
use crate::ws::WebSocketStream;

/// A boxed, `Send` future — the crate's object-safe async currency.
/// Hand-rolled (no `async-trait`) to keep the core free of macro/runtime
/// coupling, mirroring the jsi-rs convention.
pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Executes HTTP requests against a platform network stack.
///
/// Implementations: `monet-cronet` (Cronet C API, Android), `monet-urlsession`
/// (Swift shim, iOS), the pure-Rust fallback (hyper + rustls), and
/// [`crate::MockTransport`] for tests.
///
/// The contract:
/// - Resolve once the response **head** is available; the [`Response`] body
///   then streams through its channel. Do not buffer the full body first.
/// - Return [`crate::Error::Cancelled`] if `cancel` fires before the head is
///   delivered (native transports should still map it to their cancel call
///   even later in the body stream).
/// - `Request` is consumed: transports may rewrite or drop its parts.
pub trait Transport: Send + Sync {
    /// Executes `req`, resolving with the response head.
    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>>;
}

/// Opens WebSocket connections.
///
/// The iOS transport implements this over `URLSessionWebSocketTask`; on
/// Android the pure-Rust (tokio-tungstenite + rustls) transport does.
/// Both feed the same channel-based [`WebSocketStream`], so frontends see
/// one shape.
pub trait WebSocketTransport: Send + Sync {
    /// Performs the upgrade request in `req` and resolves with an open socket.
    fn connect(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<WebSocketStream>>;
}
