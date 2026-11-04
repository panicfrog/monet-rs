//! The battery driver: one entry point that runs every transport through
//! the shared conformance checks and returns a per-check report.
//!
//! The same function backs two very different harnesses — a host
//! integration test (`tests/battery.rs`) and the Android demo app's JNI
//! surface — which is the point: whatever passes here passes on-device,
//! because it is literally the same code over the same loopback servers.

use monet_conformance::battery::{self, CheckReport};
use monet_core::pin::PinConfig;
use monet_rustls::{MonetRustlsConfig, MonetRustlsTransport};
use monet_tls::TlsTrust;
use monet_ws::{MonetWsTransport, WsConfig};

/// Runs the full battery: HTTP conformance (plain loopback), TLS trust
/// and pinning over a locally minted CA, and the WebSocket suite.
///
/// Check names are stable — the app displays them verbatim and the host
/// test asserts the full set passes. Every check gets a freshly
/// constructed transport so failures can't leak state into the next one.
pub fn run_battery() -> Vec<CheckReport> {
    let mut reports = Vec::new();
    let server = monet_conformance::http::HttpServer::spawn();

    // ---- HTTP over plain loopback ----
    let http =
        |f: fn(&dyn monet_core::transport::Transport, &monet_conformance::http::HttpServer)| {
            let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();
            f(&transport, &server);
        };
    reports.push(battery::check("http:get_plain_body", || {
        http(monet_conformance::http::get_plain_body)
    }));
    reports.push(battery::check("http:get_chunked_content", || {
        http(monet_conformance::http::get_chunked_content)
    }));
    reports.push(battery::check("http:post_reaches_server", || {
        http(monet_conformance::http::post_reaches_server)
    }));
    reports.push(battery::check("http:redirect_followed", || {
        http(monet_conformance::http::redirect_followed_by_default)
    }));
    reports.push(battery::check("http:pre_cancelled_fails_fast", || {
        http(monet_conformance::http::pre_cancelled_fails_fast)
    }));
    reports.push(battery::check("http:cancel_after_head_aborts_body", || {
        http(monet_conformance::http::cancel_after_head_aborts_body)
    }));
    reports.push(battery::check(
        "http:held_body_does_not_block_new_requests",
        || http(monet_conformance::http::held_body_does_not_block_new_requests),
    ));
    reports.push(battery::check(
        "http:slow_upload_does_not_block_new_requests",
        || http(monet_conformance::http::slow_upload_does_not_block_new_requests),
    ));

    // ---- TLS: trust roots and pin policy over a fresh CA ----
    let ca = monet_conformance::tls::LocalCa::generate();
    let trusted = MonetRustlsTransport::new(MonetRustlsConfig {
        trust: TlsTrust::CustomRoots(vec![ca.der()]),
        ..MonetRustlsConfig::default()
    })
    .unwrap();
    reports.push(battery::check("tls:https_trusted_ca", || {
        monet_conformance::tls::https_get_with_trusted_ca(&trusted, &ca);
    }));
    let wrong_ca = MonetRustlsTransport::new(MonetRustlsConfig {
        trust: TlsTrust::CustomRoots(vec![monet_conformance::tls::LocalCa::generate().der()]),
        ..MonetRustlsConfig::default()
    })
    .unwrap();
    reports.push(battery::check("tls:https_untrusted_ca_rejects", || {
        monet_conformance::tls::https_untrusted_ca_rejects(&wrong_ca, &ca);
    }));
    let pinned_wrong = MonetRustlsTransport::new(MonetRustlsConfig {
        trust: TlsTrust::CustomRoots(vec![ca.der()]),
        pins: mismatched_pins("localhost"),
        ..MonetRustlsConfig::default()
    })
    .unwrap();
    reports.push(battery::check("tls:pin_mismatch_rejects", || {
        monet_conformance::tls::https_pin_mismatch_rejects(&pinned_wrong, &ca);
    }));

    // ---- WebSocket (plain + TLS) ----
    macro_rules! ws_check {
        ($name:expr, $f:path) => {
            reports.push(battery::check($name, || {
                let ws = MonetWsTransport::new(WsConfig::default()).unwrap();
                $f(&ws);
            }));
        };
    }
    ws_check!(
        "ws:round_trips",
        monet_conformance::ws::text_and_binary_round_trip
    );
    ws_check!(
        "ws:close_code_fidelity",
        monet_conformance::ws::server_close_code_and_reason_round_trip_exactly
    );
    ws_check!(
        "ws:client_close_reaches_server",
        monet_conformance::ws::client_close_reaches_server
    );
    ws_check!(
        "ws:ping_surfaced_and_ponged",
        monet_conformance::ws::ping_surfaced_and_auto_ponged
    );
    ws_check!(
        "ws:pre_cancelled_fails_fast",
        monet_conformance::ws::pre_cancelled_fails_fast
    );

    let wss = MonetWsTransport::new(WsConfig {
        trust: TlsTrust::CustomRoots(vec![ca.der()]),
        ..WsConfig::default()
    })
    .unwrap();
    reports.push(battery::check("ws:tls_echo", || {
        monet_conformance::tls::wss_echo_with_trusted_ca(&wss, &ca);
    }));

    reports
}

/// Formats the report for display (logcat, a TextView, a CI summary).
pub fn format_battery(reports: &[CheckReport]) -> String {
    battery::format(reports)
}

/// The platform-verifier battery (Android only): the transports trusting
/// the platform store. The negative case (an unanchored CA is rejected)
/// runs by default and exercises the full rustls-platform-verifier path.
///
/// The positive case — a CA anchored in the system store being trusted —
/// is root-caused but deliberately skipped: rustls-platform-verifier 0.7
/// verifies in two steps (X509TrustManagerExtensions, then a second PKIX
/// pass with revocation enabled), and a self-signed test CA has no
/// OCSP/CRL infrastructure, so step two reports Revoked for any fixture
/// CA. Everything short of that is verified: the injected anchor is
/// enumerated by AndroidCAStore, the chain passes
/// X509TrustManagerExtensions and PKIX with revocation disabled
/// (on-device probe evidence, 2026-11-04). Passing the full positive
/// path needs a real CA — same manual-item class as the iOS pin
/// challenge check.
#[cfg(target_os = "android")]
pub fn run_platform_battery() -> Vec<CheckReport> {
    use monet_conformance::tls::LocalCa;

    let mut reports = Vec::new();

    reports.push(battery::skip(
        "platform:system_ca_trusted",
        "needs a real CA with OCSP/CRL — rustls-platform-verifier's second PKIX pass (revocation enabled) rejects fixture CAs by design; see the battery docs in lib.rs",
    ));

    // A CA minted at runtime is anchored nowhere: the platform verifier
    // must reject it (trust is not advisory). This check runs the full
    // library path — both verification steps.
    let runtime_ca = LocalCa::generate();
    match MonetRustlsTransport::new(MonetRustlsConfig {
        trust: TlsTrust::PlatformVerifier,
        ..MonetRustlsConfig::default()
    }) {
        Ok(platform) => {
            reports.push(battery::check("platform:runtime_ca_rejects", || {
                monet_conformance::tls::https_untrusted_ca_rejects(&platform, &runtime_ca);
            }));
        }
        Err(err) => {
            reports.push(CheckReport {
                name: "platform:runtime_ca_rejects",
                failure: Some(format!("transport construction failed: {err}")),
            });
        }
    }

    reports
}

/// A pin entry for `host` whose hash matches nothing in the minted
/// chain: the base64 of 32 zero bytes.
fn mismatched_pins(host: &str) -> PinConfig {
    let json = format!(
        r#"{{"{host}":{{"publicKeyHashes":["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="]}}}}"#
    );
    PinConfig::from_json(&json).expect("pin config parses")
}

#[cfg(target_os = "android")]
mod android;

#[cfg(any(target_os = "ios", target_os = "macos"))]
mod apple;

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole battery, on the host — the Android app runs the
    /// identical function.
    #[test]
    fn full_battery_passes() {
        let reports = run_battery();
        let failed: Vec<&CheckReport> = reports.iter().filter(|r| !r.passed()).collect();
        assert!(
            failed.is_empty(),
            "battery failures:\n{}",
            format_battery(&reports)
        );
    }
}

/// The platform HttpEngine battery (Android only): the full HTTP
/// conformance suite against the system-provided Cronet (API 34+),
/// prefixed with the engine version. Requires initHttpEngine to have
/// run — the app calls it with its Context.
#[cfg(target_os = "android")]
pub fn run_httpengine_battery() -> (String, Vec<CheckReport>) {
    use monet_core::pin::PinConfig;
    let mut reports = Vec::new();
    let (version, transport) = match monet_httpengine::HttpengineTransport::new(PinConfig::default())
    {
        Ok(transport) => (transport.engine_version(), Some(transport)),
        Err(err) => (format!("engine construction failed: {err}"), None),
    };
    let Some(transport) = transport else {
        reports.push(CheckReport {
            name: "httpengine:construction",
            failure: Some(version.clone()),
        });
        return (version, reports);
    };
    reports.push(battery::check("httpengine:conformance_http", || {
        monet_conformance::http::run(&transport);
    }));
    (version, reports)
}

/// Diagnostic: a header-bearing POST without upload data isolates the
/// addHeader path from the upload path (the conformance GETs carry no
/// headers, so a broken addHeader only surfaces on POST-with-upload).
#[cfg(target_os = "android")]
pub fn httpengine_header_probe() -> String {
    use bytes::Bytes;
    use futures_lite::StreamExt as _;
    use monet_core::method::Method;
    use monet_core::request::Request;
    use monet_core::transport::Transport as _;
    let server = monet_conformance::http::HttpServer::spawn();
    let url = server.url("/echo");
    let transport = match monet_httpengine::HttpengineTransport::new(PinConfig::default()) {
        Ok(transport) => transport,
        Err(err) => return format!("construction failed: {err}"),
    };
    let request = Request::new(Method::Post, url)
        .with_header("content-type", "application/json")
        .unwrap();
    let response = match futures_lite::future::block_on(
        transport.execute(request, monet_core::CancelToken::new()),
    ) {
        Ok(response) => response,
        Err(err) => return format!("probe request failed: {err}"),
    };
    let chunks = futures_lite::future::block_on(response.body.collect::<Vec<_>>());
    let body: Vec<u8> = chunks
        .into_iter()
        .map(|chunk| chunk.unwrap())
        .collect::<Vec<Bytes>>()
        .concat();
    format!(
        "status={} body={:?} headers={:?}",
        response.status,
        body,
        response.headers.iter().collect::<Vec<_>>()
    )
}
