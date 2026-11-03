//! Pure-Rust HTTP transport for monet-core — Android's default.
//!
//! hyper (HTTP/1.1 + HTTP/2) over rustls, running on a private
//! single-threaded tokio runtime exactly like monet-ws: callers poll plain
//! futures and the channel-based `Body`, never tokio types. DNS goes
//! through the system resolver (hyper-util's GaiResolver wraps getaddrinfo
//! in `spawn_blocking`). TLS trust and pinning come from the shared
//! `monet-tls` crate.
//!
//! HTTP/3 (quinn + h3) is a planned experimental feature; h3 remains 0.0.x
//! so HTTP/2 is the mature path here.

mod transport;

pub use transport::{MonetRustlsConfig, MonetRustlsTransport};
