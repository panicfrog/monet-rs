//! monet-core: the platform-agnostic heart of monet-rs.
//!
//! Everything here is pure Rust with zero RN/JSI/Dart dependencies and no
//! async runtime: futures are hand-boxed [`transport::BoxFuture`]s, streams
//! are `futures-core` abstractions, so any executor (jsi-turbo's worker
//! pool, tokio, flutter_rust_bridge) can drive the same core.
//!
//! The layout mirrors jsi-rs's core/wrapper split: platform transports
//! (`monet-cronet`, `monet-urlsession`, pure-Rust fallback) implement
//! [`transport::Transport`] and [`transport::WebSocketTransport`] on top of
//! these types; frontends (`monet-rn`, later `monet-dart`) wrap the same
//! core.
//!
//! ```
//! use monet_core::{CancelToken, HeaderMap, MockTransport, Request, Transport};
//! use monet_core::method::Method;
//!
//! # fn main() -> monet_core::Result<()> {
//! let transport = MockTransport::with_response(
//!     200,
//!     HeaderMap::new(),
//!     vec![bytes::Bytes::from("hello")],
//! );
//! let url = url::Url::parse("https://example.com").unwrap();
//! let req = Request::new(Method::Get, url).with_header("X-Trace", "42")?;
//! let resp = futures_lite::future::block_on(
//!     transport.execute(req, CancelToken::new()),
//! )?;
//! assert!(resp.is_success());
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

pub mod body;
pub mod cancel;
pub mod error;
pub mod extensions;
pub mod headers;
pub mod hook;
pub mod method;
pub mod mock;
pub mod pin;
pub mod request;
pub mod response;
pub mod transport;
pub mod ws;

pub use body::{Body, BodySender, channel as body_channel};
pub use cancel::CancelToken;
pub use error::{Error, PlatformErrorCode, Result};
pub use extensions::Extensions;
pub use headers::HeaderMap;
pub use hook::{HookRegistry, RequestHook, ResponseHook};
pub use method::Method;
pub use mock::{MockTransport, RecordedRequest};
pub use pin::{Date, PinConfig, PinEntry, PinError};
pub use request::Request;
pub use response::Response;
pub use transport::{BoxFuture, Transport, WebSocketTransport};
pub use ws::{CloseFrame, WebSocketDriver, WebSocketStream, WsMessage, channel as ws_channel};

/// Re-exported so consumers don't need the `url` crate directly.
pub use url::Url;
