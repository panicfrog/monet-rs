//! The shared conformance suite: one battery of checks, run against every
//! `Transport` and `WebSocketTransport` implementation.
//!
//! This is the architecture's core bet made executable — monet-rs keeps
//! one Rust core and multiple platform transports (URLSession via the
//! Swift shim, hyper+rustls, Cronet), and the only way that bet pays off
//! is if the transports behave identically behind
//! [`monet_core::Transport`]. Each transport crate dev-depends on this
//! crate and runs the same suite in its own integration tests; a check
//! that passes on one implementation and fails on another is a semantic
//! divergence, not a test problem.
//!
//! ```no_run
//! // In a transport crate's tests/conformance.rs:
//! # use monet_conformance::http;
//! # fn t() -> std::sync::Arc<dyn monet_core::Transport> { unimplemented!() }
//! # fn main() {
//! let server = http::HttpServer::spawn();
//! http::run_suite(&*t(), &server);
//! # }
//! ```
//!
//! Deliberate divergences are expressed by which checks a crate runs, and
//! recorded in its test file — the suite itself takes no sides.

pub mod battery;
pub mod http;
pub mod tls;
pub mod ws;

/// Every wait inside the suite is bounded: a hung implementation fails
/// with the step's name instead of blocking the run forever.
pub const STEP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Drives `fut` to completion, panicking with `what` on timeout.
pub(crate) fn wait_for<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    futures_lite::future::block_on(async {
        let outcome = futures_lite::future::or(async { Ok(fut.await) }, async {
            futures_timer::Delay::new(STEP_TIMEOUT).await;
            Err(())
        })
        .await;
        match outcome {
            Ok(value) => value,
            Err(()) => panic!("timed out after {STEP_TIMEOUT:?} waiting for {what}"),
        }
    })
}
