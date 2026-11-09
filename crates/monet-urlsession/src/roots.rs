//! Additional trust roots for the Swift shim (configureTrust).
//!
//! Same process-global, last-wins shape as the pinning registry in
//! [`crate::pin`]: the shim's session is process-global, so the
//! registration is too. Swift asks "how many roots, which DER at index
//! i?" when a server-trust challenge arrives, then does the mechanical
//! parts itself (`SecTrustSetAnchorCertificates` + standard evaluation —
//! anchors added, never replacing the system's, the
//! network_security_config semantics).
//!
//! An empty registry is the "no custom roots" state; every construction
//! of the transport registers (empty list included), so a `clearTrust`
//! reaches the shim instead of leaving a stale registration live — the
//! same reasoning as the pinning registry's `None` handling.

use std::sync::{LazyLock, Mutex};

static ROOTS: LazyLock<Mutex<Vec<Vec<u8>>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// Registers the DER roots the shim anchors challenges with. The last
/// registration wins. Called by
/// [`UrlsessionTransport::with_roots`](crate::UrlsessionTransport::with_roots).
pub(crate) fn register(roots: Vec<Vec<u8>>) {
    *ROOTS.lock().unwrap() = roots;
}

/// How many roots are registered (Swift's loop bound).
pub(crate) fn count() -> usize {
    ROOTS.lock().unwrap().len()
}

/// Copies root `index`'s DER into `out` and returns the bytes written.
/// An `out` shorter than the root writes nothing and returns the
/// needed length — Swift sizes its buffer with a first empty call. An
/// out-of-range index (the registry was replaced between the count and
/// this call) returns 0; the shim skips the slot and the handshake
/// fails closed downstream.
pub(crate) fn root_at(index: usize, out: &mut [u8]) -> usize {
    let roots = ROOTS.lock().unwrap();
    let Some(der) = roots.get(index) else {
        return 0;
    };
    if out.len() < der.len() {
        return der.len();
    }
    out[..der.len()].copy_from_slice(der);
    der.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_round_trip_by_index() {
        register(vec![vec![1, 2, 3], vec![4, 5]]);
        assert_eq!(count(), 2);

        let needed = root_at(0, &mut []);
        assert_eq!(needed, 3);
        let mut out = [0u8; 3];
        assert_eq!(root_at(0, &mut out), 3);
        assert_eq!(out, [1, 2, 3]);

        let mut out = [0u8; 2];
        assert_eq!(root_at(1, &mut out), 2, "the buffer fits exactly");
        assert_eq!(out, [4, 5]);
    }

    #[test]
    fn an_empty_registration_clears() {
        register(vec![vec![9]]);
        register(vec![]);
        assert_eq!(count(), 0);
        assert_eq!(root_at(0, &mut []), 0, "no out-of-range phantom");
    }
}
