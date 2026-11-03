//! Pinning query service for the Swift shim.
//!
//! Host-matching semantics (walk-up, includeSubdomains, expiry) live in
//! monet-core's [`PinConfig`] — one implementation shared with the Rust
//! transports. Swift asks "is this host pinned, which hashes?" when a
//! server-trust challenge arrives, then does the mechanical parts itself
//! (SecTrustEvaluate + SPKI SHA-256 comparison, nitro-fetch's
//! `CertificatePinning.swift` pattern).
//!
//! Fail-closed lives one level up: a broken config fails
//! [`PinConfig::from_json`] before it can ever reach [`register`], so the
//! bootstrap that loads it refuses to run unpinned.

use std::sync::{LazyLock, Mutex};

use monet_core::pin::PinConfig;

/// SPKI SHA-256 hash length, the unit the Swift buffer counts in.
const HASH_LEN: usize = 32;

static PINS: LazyLock<Mutex<Option<PinConfig>>> = LazyLock::new(|| Mutex::new(None));

/// Registers the pin configuration the shim answers challenges with. The
/// shim is process-global (one shared URLSession), so this is too; the
/// last registration wins. Called by
/// [`UrlsessionTransport::with_pins`](crate::UrlsessionTransport::with_pins).
pub(crate) fn register(config: PinConfig) {
    *PINS.lock().unwrap() = Some(config);
}

/// Copies the pinned SPKI hashes for `host` into `out` (32 bytes each)
/// and returns how many were written. Zero means the host is not pinned
/// — default challenge handling applies.
pub(crate) fn hashes_for_host(host: &[u8], out: &mut [u8]) -> usize {
    let Ok(host) = std::str::from_utf8(host) else {
        return 0;
    };
    let pins = PINS.lock().unwrap();
    let Some(config) = pins.as_ref() else {
        return 0;
    };
    let Some(entry) = config.pins_for_host(host) else {
        return 0;
    };
    let mut written = 0;
    for hash in &entry.hashes {
        if out.len() < written + HASH_LEN {
            break;
        }
        out[written..written + HASH_LEN].copy_from_slice(hash);
        written += HASH_LEN;
    }
    written / HASH_LEN
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> PinConfig {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::STANDARD;
        let a = engine.encode([0x00; 32]);
        let b = engine.encode([0xBB; 32]);
        let json = format!(
            r#"{{"api.example.com": {{"publicKeyHashes": ["{a}", "{b}"], "includeSubdomains": true}}}}"#
        );
        PinConfig::from_json(&json).unwrap()
    }

    #[test]
    fn pinned_host_hashes_cross_the_buffer() {
        register(config());
        let mut out = [0u8; 64];
        let count = hashes_for_host(b"api.example.com", &mut out);
        assert_eq!(count, 2);
        assert_eq!(&out[..32], &[0x00; 32]);
        assert_eq!(&out[32..], &[0xBB; 32]);
    }

    #[test]
    fn subdomains_covered_by_walk_up() {
        register(config());
        let mut out = [0u8; 64];
        assert_eq!(hashes_for_host(b"v2.api.example.com", &mut out), 2);
    }

    #[test]
    fn unpinned_host_and_small_buffer_report_zero() {
        register(config());
        let mut out = [0u8; 64];
        assert_eq!(hashes_for_host(b"other.com", &mut out), 0);
        let mut tiny = [0u8; 31];
        assert_eq!(hashes_for_host(b"api.example.com", &mut tiny), 0);
    }
}
