//! The Apple battery: the same shared conformance checks the Android demo
//! runs, over the **native** iOS transport ([`UrlsessionTransport`]) —
//! URLSession for HTTP, NWConnection for WebSocket, the system trust
//! store for TLS. The rustls-specific checks (custom roots, pin
//! mismatch) are not run here: a runtime-minted CA is anchored nowhere,
//! so the system would refuse it before pinning ever comes into play.
//!
//! The C ABI below is what the demo app (`platform/ios-demo`) calls;
//! everything heavy lives in [`crate::run_battery_apple`], shared with
//! the host test.

// Exported symbols are the whole point of this module.
#![allow(unsafe_code)]

use std::ffi::{c_char, CString};

use monet_conformance::{battery, http, ws};
use monet_urlsession::UrlsessionTransport;

/// Records one HTTP check; a fresh transport per check so a failure
/// can't leak state into the next one.
fn http_check(
    reports: &mut Vec<battery::CheckReport>,
    name: &'static str,
    server: &http::HttpServer,
    f: fn(&dyn monet_core::transport::Transport, &http::HttpServer),
) {
    reports.push(battery::check(name, || {
        let transport = UrlsessionTransport::new();
        f(&transport, server);
    }));
}

/// Records one WebSocket check over the same native transport.
fn ws_check(
    reports: &mut Vec<battery::CheckReport>,
    name: &'static str,
    f: fn(&dyn monet_core::transport::WebSocketTransport),
) {
    reports.push(battery::check(name, || {
        let transport = UrlsessionTransport::new();
        f(&transport);
    }));
}

/// Runs the native-transport battery.
pub fn run_battery_apple() -> Vec<battery::CheckReport> {
    let mut reports = Vec::new();
    let server = http::HttpServer::spawn();

    http_check(&mut reports, "http:get_plain_body", &server, http::get_plain_body);
    http_check(
        &mut reports,
        "http:get_chunked_content",
        &server,
        http::get_chunked_content,
    );
    http_check(
        &mut reports,
        "http:post_reaches_server",
        &server,
        http::post_reaches_server,
    );
    http_check(
        &mut reports,
        "http:redirect_followed",
        &server,
        http::redirect_followed_by_default,
    );
    http_check(
        &mut reports,
        "http:pre_cancelled_fails_fast",
        &server,
        http::pre_cancelled_fails_fast,
    );
    http_check(
        &mut reports,
        "http:cancel_after_head_aborts_body",
        &server,
        http::cancel_after_head_aborts_body,
    );
    reports.push(battery::check("http:connection_refused", || {
        let transport = UrlsessionTransport::new();
        http::connection_refused_maps_to_transport_error(&transport);
    }));
    reports.push(battery::check("http:timeout_maps_to_transport_error", || {
        let transport = UrlsessionTransport::new().with_timeout_ms(100);
        http::timeout_maps_to_transport_error(&transport, &server);
    }));

    ws_check(&mut reports, "ws:round_trips", ws::text_and_binary_round_trip);
    ws_check(
        &mut reports,
        "ws:close_code_fidelity",
        ws::server_close_code_and_reason_round_trip_exactly,
    );
    ws_check(
        &mut reports,
        "ws:client_close_reaches_server",
        ws::client_close_reaches_server,
    );
    ws_check(
        &mut reports,
        "ws:ping_surfaced_and_ponged",
        ws::ping_surfaced_and_auto_ponged,
    );
    ws_check(
        &mut reports,
        "ws:pre_cancelled_fails_fast",
        ws::pre_cancelled_fails_fast,
    );

    reports
}

/// Runs the battery and returns a freshly allocated report string
/// (`name: OK/FAILED` lines plus a summary). Free it with
/// [`monet_demo_free_string`].
#[unsafe(no_mangle)]
pub extern "C" fn monet_demo_run_battery() -> *mut c_char {
    let text = std::panic::catch_unwind(|| {
        crate::format_battery(&run_battery_apple())
    })
    .unwrap_or_else(|_| "battery panicked".to_string());
    CString::into_raw(CString::new(text).expect("battery report has no NUL bytes"))
}

/// Frees a string returned by [`monet_demo_run_battery`].
#[unsafe(no_mangle)]
pub extern "C" fn monet_demo_free_string(text: *mut c_char) {
    if !text.is_null() {
        drop(unsafe { CString::from_raw(text) });
    }
}
