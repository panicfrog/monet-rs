//! The shared conformance battery against monet-urlsession — the same
//! checks every transport runs (see monet-conformance). The granular
//! macOS tests in http.rs / ws.rs stay; this file pins the transports to
//! one another.

use monet_conformance::{http, ws};
use monet_urlsession::UrlsessionTransport;

#[test]
fn http_conformance() {
    let transport = UrlsessionTransport::new();
    http::run(&transport);
}

#[test]
fn http_timeout_conformance() {
    let transport = UrlsessionTransport::new().with_timeout_ms(100);
    let server = http::HttpServer::spawn();
    http::timeout_maps_to_transport_error(&transport, &server);
}

#[test]
fn websocket_conformance() {
    let transport = UrlsessionTransport::new();
    ws::run_suite(&transport);
    ws::client_close_reaches_server(&transport);
    ws::ping_surfaced_and_auto_ponged(&transport);
}
