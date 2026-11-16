//! Pins registered after a connection is pooled must still be enforced.
//!
//! URLSession checks server trust only when it opens a TLS connection;
//! the shared session pools connections per host. Its own test binary:
//! pins and roots are process-global, so no other test may race them.

use std::future::Future;
use std::time::Duration;

use futures_lite::StreamExt;
use monet_conformance::tls::{LocalCa, spawn_https_keep_alive};
use monet_core::CancelToken;
use monet_core::error::Error;
use monet_core::method::Method;
use monet_core::pin::PinConfig;
use monet_core::request::Request;
use monet_core::transport::Transport as _;
use monet_urlsession::UrlsessionTransport;

fn wait_for<T>(what: &str, fut: impl Future<Output = T>) -> T {
    futures_lite::future::block_on(futures_lite::future::or(fut, async {
        futures_timer::Delay::new(Duration::from_secs(10)).await;
        panic!("timed out waiting for {what}")
    }))
}

#[test]
fn pins_registered_later_apply_to_a_pooled_connection() {
    let ca = LocalCa::generate();
    let server = spawn_https_keep_alive(&ca);
    let url = server.url_on("https", "localhost", "/plain");

    // Unpinned: the request goes through and leaves a pooled connection.
    let transport = UrlsessionTransport::new().with_roots(vec![ca.der()]);
    let response = wait_for(
        "unpinned response",
        transport.execute(Request::new(Method::Get, url.clone()), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200);
    wait_for("unpinned body", response.body.collect::<Vec<_>>());

    // A pin that matches nothing in the chain — the request must fail,
    // even though a connection to the host is already open.
    let wrong = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    let pins = PinConfig::from_json(&format!(
        r#"{{"localhost": {{"publicKeyHashes": ["{wrong}"]}}}}"#
    ))
    .unwrap();
    let pinned = UrlsessionTransport::new()
        .with_roots(vec![ca.der()])
        .with_pins(pins);
    let outcome = wait_for(
        "pinned outcome",
        pinned.execute(Request::new(Method::Get, url), CancelToken::new()),
    );
    match outcome {
        Err(Error::Transport { message, .. }) => {
            assert!(message.contains("pinning"), "got: {message}")
        }
        Err(other) => panic!("expected a pin rejection, got: {other:?}"),
        Ok(response) => panic!(
            "the pooled connection skipped the new pin (status {})",
            response.status
        ),
    }
}
