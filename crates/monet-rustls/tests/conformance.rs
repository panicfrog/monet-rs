//! The shared conformance battery against monet-rustls — the same checks
//! every transport runs (see monet-conformance).

use monet_conformance::http;
use monet_rustls::{MonetRustlsConfig, MonetRustlsTransport};

#[test]
fn http_conformance() {
    let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();
    http::run(&transport);
}

#[test]
fn http_timeout_conformance() {
    let transport = MonetRustlsTransport::new(MonetRustlsConfig {
        timeout_ms: Some(100),
        ..MonetRustlsConfig::default()
    })
    .unwrap();
    let server = http::HttpServer::spawn();
    http::timeout_maps_to_transport_error(&transport, &server);
    http::per_request_timeout_longer_wins(&transport, &server);
}

/// The hop limit's error names the limit (it once printed the literal
/// `{MAX_REDIRECTS}` placeholder).
#[test]
fn too_many_redirects_names_the_limit() {
    let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();
    let server = http::HttpServer::spawn();
    let message = http::redirect_loop_fails(&transport, &server).to_string();
    assert!(
        message.contains("too many redirects (> 20)"),
        "got: {message}"
    );
}
