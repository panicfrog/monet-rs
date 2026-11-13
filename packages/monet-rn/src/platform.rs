//! Platform transport selection: the stack behind `Monet` per build.
//!
//! The selection is the whole point of the monet-core `Transport` trait:
//! the same fetch/WebSocket channels run over whichever stack the
//! platform offers — a configuration choice, never an architecture fork.
//!
//! **Android** — the stack is a deployment decision the app makes at
//! build time through this crate's cargo features (see Cargo.toml):
//!
//! | App minSdk | Features | Runtime behavior |
//! |---|---|---|
//! | >= 34 | `httpengine` (default) | platform HttpEngine only — the system Cronet, framework API (CTS-mandatory on every Android 14+ ROM, GMS or not), zero app-shipped binary |
//! | < 34 | `httpengine` + `fallback-rustls` | HttpEngine on 34+ devices (SDK gate inside the transport), hyper+h2+rustls on older ones |
//!
//! The `Build.VERSION.SDK_INT` gate lives inside
//! `HttpengineTransport::new`; the try-then-fallback here is the
//! defensive second layer (engine construction also fails if the
//! embedder never ran the JNI bootstrap — the npm package's Android
//! library provides it). WebSocket is `monet-ws` (tokio-tungstenite +
//! rustls) whenever the `websocket` feature is on — HttpEngine has no
//! WebSocket surface, so it is independent of the HTTP choice.
//!
//! **iOS/macOS**: `monet-urlsession` — URLSession HTTP + NWConnection
//! WebSocket + system trust (pins installed in the Swift shim,
//! process-global, last-wins).

use std::sync::Arc;

use monet_core::pin::PinConfig;
use monet_core::Transport;

/// The trust surface the platform transports are built with: pins for
/// pin enforcement, and the app-supplied custom roots anchored
/// alongside the platform store (configureTrust) — one configuration
/// event, one rebuild.
#[derive(Clone)]
pub struct TrustConfig {
    pub pins: PinConfig,
    pub roots: Vec<Vec<u8>>,
}

/// The rustls trust for the configured roots: the platform store plus
/// the app's own anchors (network_security_config semantics), or the
/// bare platform store when none are configured. Android only — the
/// Apple stacks take pins and roots through the Swift shim, not
/// monet-tls.
#[cfg(target_os = "android")]
fn rustls_trust(roots: &[Vec<u8>]) -> monet_tls::TlsTrust {
    if roots.is_empty() {
        monet_tls::TlsTrust::PlatformVerifier
    } else {
        monet_tls::TlsTrust::PlatformAndCustomRoots(roots.to_vec())
    }
}

/// The HTTP transport for this platform + configuration.
pub fn http_transport(trust: &TrustConfig) -> Arc<dyn Transport> {
    select_http(trust).unwrap_or_else(|reason| Arc::new(BrokenTransport(reason)))
}

#[allow(unused_variables)]
fn select_http(trust: &TrustConfig) -> Result<Arc<dyn Transport>, String> {
    #[cfg(all(target_os = "android", feature = "httpengine"))]
    {
        // Pins only: HttpEngine.Builder has no custom-roots API (the
        // boundary on record in the plans) — fetches through it trust
        // the system + network_security_config anchors, and
        // configureTrust reaches monet-ws and the rustls fallback.
        match monet_httpengine::HttpengineTransport::new(trust.pins.clone()) {
            Ok(transport) => return Ok(Arc::new(transport)),
            Err(reason) => {
                // On an httpengine-only build this is fatal (the app
                // declared minSdk >= 34 — a failure here means the
                // bootstrap didn't run); with the fallback compiled in,
                // it's the older-device path.
                #[cfg(not(feature = "fallback-rustls"))]
                return Err(reason.to_string());
                #[cfg(feature = "fallback-rustls")]
                log::info!(
                    "monet-rn: HttpEngine unavailable ({reason}) — falling back to monet-rustls"
                );
            }
        }
    }
    #[cfg(all(target_os = "android", feature = "fallback-rustls"))]
    {
        let config = monet_rustls::MonetRustlsConfig {
            // Platform trust store, not the embedded Mozilla bundle —
            // the transports' default; the system CA store (and any
            // network_security_config anchors the app adds) is what a
            // platform stack must honor. The custom roots extend it
            // (configureTrust).
            trust: rustls_trust(&trust.roots),
            pins: trust.pins.clone(),
            ..monet_rustls::MonetRustlsConfig::default()
        };
        return monet_rustls::MonetRustlsTransport::new(config)
            .map(Arc::new)
            .map_err(|e| e.to_string());
    }
    // Only reachable when neither stack feature compiled in — with
    // either one the block above is the tail, and an ungated block here
    // would be dead code behind its `return` (the compiler says so).
    #[cfg(all(
        target_os = "android",
        not(feature = "httpengine"),
        not(feature = "fallback-rustls")
    ))]
    {
        Err("monet-rn: no Android HTTP stack compiled in (enable `httpengine` or `fallback-rustls`)".into())
    }
    #[cfg(any(target_os = "ios", target_os = "macos"))]
    {
        // Construction is infallible (`new() -> Self`); pins and roots
        // live in the shim's process-global registry, last registration
        // wins. EVERY construction registers — an empty config included:
        // that is how a clearPins/clearTrust reaches the shim (a bare
        // new() would leave the previous registration live, enforcing
        // configuration the app cleared).
        let transport = monet_urlsession::UrlsessionTransport::new()
            .with_pins(trust.pins.clone())
            .with_roots(trust.roots.clone());
        Ok(Arc::new(transport) as Arc<dyn Transport>)
    }
    #[cfg(not(any(target_os = "android", target_os = "ios", target_os = "macos")))]
    {
        Err("monet-rn: no platform stack linked (dev build)".into())
    }
}

/// The WebSocket transport for this platform.
#[allow(unused_variables)]
pub fn ws_transport(trust: &TrustConfig) -> anyhow::Result<Arc<dyn monet_core::WebSocketTransport>> {
    #[cfg(all(target_os = "android", feature = "websocket"))]
    {
        let config = monet_ws::WsConfig {
            // Platform trust store (see select_http's fallback note):
            // the same trust surface as the HttpEngine fetch path — the
            // transports' own default is the embedded Mozilla bundle,
            // which silently diverges from the platform stacks. The
            // custom roots extend it (configureTrust).
            trust: rustls_trust(&trust.roots),
            pins: trust.pins.clone(),
            ..monet_ws::WsConfig::default()
        };
        let transport = monet_ws::MonetWsTransport::new(config)
            .map_err(|e| anyhow::anyhow!("monet-ws construction: {e}"))?;
        Ok(Arc::new(transport))
    }
    #[cfg(all(target_os = "android", not(feature = "websocket")))]
    {
        let _ = trust;
        Err(anyhow::anyhow!(
            "monet-rn: WebSocket support not compiled in (enable the `websocket` feature)"
        ))
    }
    #[cfg(any(target_os = "ios", target_os = "macos"))]
    {
        // One UrlsessionTransport type serves both stacks; pins and
        // roots live in the shim's process-global registry (last
        // registration wins), so every construction registers —
        // including the empty config a cleared state carries (same
        // reasoning as select_http).
        let transport = monet_urlsession::UrlsessionTransport::new()
            .with_pins(trust.pins.clone())
            .with_roots(trust.roots.clone());
        Ok(Arc::new(transport) as Arc<dyn monet_core::WebSocketTransport>)
    }
    #[cfg(not(any(target_os = "android", target_os = "ios", target_os = "macos")))]
    {
        let _ = trust;
        Err(anyhow::anyhow!(
            "monet-rn: no platform stack linked (dev build)"
        ))
    }
}

/// A transport that fails every request with a fixed reason — the last
/// resort when no real stack can be constructed, so errors surface as
/// transport failures instead of panics.
struct BrokenTransport(String);

impl Transport for BrokenTransport {
    fn execute(
        &self,
        _req: monet_core::Request,
        _cancel: monet_core::CancelToken,
    ) -> monet_core::transport::BoxFuture<monet_core::Result<monet_core::Response>> {
        let reason = self.0.clone();
        Box::pin(async move {
            Err(monet_core::Error::transport(format!(
                "monet-rn: transport unavailable: {reason}"
            )))
        })
    }
}

/// The app's cache directory (write_cache_file's landing zone).
/// Android: stashed by the bootstrap from Context.getCacheDir(). Apple:
/// $HOME/Library/Caches (the simulator and device both guarantee it).
pub(crate) fn set_cache_dir(dir: String) {
    // Once-only by construction (the bootstrap runs once); a second set
    // is a no-op, not an error worth surfacing.
    let _ = CACHE_DIR.set(dir);
}

/// The app's cache directory; empty when unknown (Apple pre-HOME, or an
/// embedder that never stashed it).
pub(crate) fn cache_dir() -> std::path::PathBuf {
    if let Some(dir) = CACHE_DIR.get() {
        return std::path::PathBuf::from(dir);
    }
    #[cfg(any(target_os = "ios", target_os = "macos"))]
    if let Ok(home) = std::env::var("HOME") {
        return std::path::Path::new(&home).join("Library/Caches");
    }
    std::path::PathBuf::new()
}

static CACHE_DIR: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// The stack description the demo prints (a misconfigured build is
/// visible at a glance instead of buried in link errors).
pub fn transport_stack() -> &'static str {
    #[cfg(all(
        target_os = "android",
        feature = "httpengine",
        feature = "fallback-rustls"
    ))]
    {
        "android: HttpEngine (API 34+) → monet-rustls fallback + monet-ws"
    }
    #[cfg(all(
        target_os = "android",
        feature = "httpengine",
        not(feature = "fallback-rustls")
    ))]
    {
        "android: HttpEngine only (API 34+) + monet-ws"
    }
    #[cfg(all(
        target_os = "android",
        not(feature = "httpengine"),
        feature = "fallback-rustls"
    ))]
    {
        "android: monet-rustls + monet-ws"
    }
    #[cfg(all(
        target_os = "android",
        not(feature = "httpengine"),
        not(feature = "fallback-rustls")
    ))]
    {
        "android: NO HTTP STACK (enable httpengine or fallback-rustls)"
    }
    #[cfg(any(target_os = "ios", target_os = "macos"))]
    {
        "apple: URLSession HTTP + NWConnection WebSocket + system trust"
    }
    #[cfg(not(any(target_os = "android", target_os = "ios", target_os = "macos")))]
    {
        "dev build: no platform stack linked"
    }
}
