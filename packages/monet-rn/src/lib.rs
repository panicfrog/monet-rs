//! Monet — monet-core's Client behind a Rust TurboModule.
//!
//! This impl block IS the spec: the method table, auto-registration and
//! the TypeScript bindings (`generated/rust-turbo/Monet.ts`) are all
//! generated from it. The fetch/WebSocket surface follows the
//! callback-signal + zero-copy-pull protocol (see docs/plans/2026-11-monet-rn.md).
//!
//! JS usage:
//!   import { getMonet } from 'monet-rn/generated/rust-turbo/Monet';

use jsi::RuntimeHandle;
use jsi_turbo::RustTurboModule;

#[derive(Default)]
pub struct Monet;

#[RustTurboModule("Monet")]
impl Monet {
    /// Smoke check: the module is alive end to end.
    pub fn ping(&self, _rt: &mut RuntimeHandle) -> anyhow::Result<String> {
        Ok("monet-rn 0.1.0".into())
    }

    /// The native stack this build linked (per-platform cfg).
    #[turbo(getter)]
    pub fn get_transport_stack(&self) -> anyhow::Result<String> {
        Ok(transport_stack().into())
    }
}

/// The stack behind `Monet` on this platform — the demo prints it so a
/// misconfigured build (wrong target, missing cfg gate) is visible at a
/// glance instead of buried in link errors.
#[cfg(any(target_os = "ios", target_os = "macos"))]
fn transport_stack() -> &'static str {
    "urlsession: URLSession HTTP + NWConnection WebSocket + system trust"
}

#[cfg(target_os = "android")]
fn transport_stack() -> &'static str {
    "rustls: hyper+h2+rustls HTTP + tokio-tungstenite WS + platform verifier"
}

#[cfg(not(any(target_os = "ios", target_os = "macos", target_os = "android")))]
fn transport_stack() -> &'static str {
    "dev build: no platform stack linked"
}
