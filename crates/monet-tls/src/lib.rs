//! Shared rustls trust + SPKI pinning for monet-rs transports.
//!
//! One TLS layer for every rustls-based transport (monet-ws today,
//! monet-rustls next): trust-source selection (platform store vs embedded
//! Mozilla bundle) plus monet-core's pin policy wrapped around whichever is
//! active. Keeping it here means the pin semantics live in exactly one
//! place — the lesson from nitro-fetch, which copies them three times.
//!
//! # Trust sources
//!
//! - Default: the embedded Mozilla CA bundle (`webpki-roots` feature).
//! - Optional: the platform trust store (`platform-verifier` feature) via
//!   rustls-platform-verifier — Android system CAs plus
//!   `network_security_config`, iOS/macOS keychain. **On Android the
//!   embedder must initialize it before any connection**: call
//!   `rustls_platform_verifier::android::init_hosted` (or the JNI variant)
//!   from the app's bootstrap — monet-rn will do this. Until then, keep the
//!   default `webpki-roots`.

#![allow(unsafe_code)] // rustls danger APIs for the custom verifier.

pub mod spki;
pub mod verifier;

pub use verifier::{TlsTrust, client_config};

/// Re-exported so consumers share one rustls crate instance.
pub use rustls;
