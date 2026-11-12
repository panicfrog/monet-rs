//! The prefetch cache (v2/W4, nitro's FetchCache shape): one complete
//! response per key, stored by `prefetch()` at its terminal, read by any
//! fetch carrying the same key. In-memory only — the cold-start
//! persisted-registration form needs a storage surface and is recorded
//! as a v2.2 candidate.
//!
//! The freshness window is evaluated at READ time by the reading
//! request (nitro's semantics — `prefetchCacheTtlMs` on the fetch, 5s
//! default), not at store time. Pending entries dedupe racing
//! prefetches and let fetches join the in-flight request.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use monet_core::error::Error;
use monet_core::headers::HeaderMap;

/// One cached response: everything the mock arm needs to deliver it.
#[derive(Clone)]
pub(crate) struct CachedResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

enum State {
    /// A prefetch is in flight; the waiters join its outcome.
    Pending(Vec<futures_channel::oneshot::Sender<Option<CachedResponse>>>),
    /// Stored; fresh until the reading request's TTL says otherwise.
    Done { response: CachedResponse, stored_at: Instant },
}

static CACHE: LazyLock<Mutex<HashMap<String, State>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Claims `key` for a prefetch. `false` when a fresh entry or a pending
/// prefetch already covers it (nitro's beginPending: racing prefetches
/// for the same key start one request).
pub(crate) fn begin(key: &str) -> bool {
    let mut cache = CACHE.lock().unwrap();
    match cache.get(key) {
        Some(State::Pending(_)) => false,
        Some(State::Done { .. }) => false,
        None => {
            cache.insert(key.to_string(), State::Pending(Vec::new()));
            true
        }
    }
}

/// Stores the prefetched response, waking every joined fetch.
pub(crate) fn complete(key: &str, response: CachedResponse) {
    let mut cache = CACHE.lock().unwrap();
    if let Some(State::Pending(waiters)) = cache.remove(key) {
        for waiter in waiters {
            let _ = waiter.send(Some(response.clone()));
        }
    }
    cache.insert(key.to_string(), State::Done { response, stored_at: Instant::now() });
}

/// Ends a failed prefetch: joined fetches fall through to the network.
pub(crate) fn fail(key: &str) {
    let mut cache = CACHE.lock().unwrap();
    if let Some(State::Pending(waiters)) = cache.remove(key) {
        for waiter in waiters {
            let _ = waiter.send(None);
        }
    }
}

/// The fresh-entry read (the fetch side). `None` when absent, stale
/// (past `ttl_ms`), or pending — a pending entry is joined through
/// [`join`] instead.
pub(crate) fn take_if_fresh(key: &str, ttl_ms: u64) -> Option<CachedResponse> {
    let cache = CACHE.lock().unwrap();
    match cache.get(key) {
        Some(State::Done { response, stored_at }) => {
            let ttl = if ttl_ms == 0 { 5_000 } else { ttl_ms };
            ((stored_at.elapsed().as_millis() as u64) < ttl)
                .then(|| response.clone())
        }
        _ => None,
    }
}

/// Joins a pending prefetch (the fetch side): resolves with the stored
/// response when it lands, `None` when the prefetch fails (the caller
/// falls through to the network). `Ok(None)` when nothing is pending
/// (check [`take_if_fresh`] first).
pub(crate) async fn join(key: &str) -> Result<Option<CachedResponse>, Error> {
    let (tx, rx) = futures_channel::oneshot::channel();
    {
        let mut cache = CACHE.lock().unwrap();
        match cache.get_mut(key) {
            Some(State::Pending(waiters)) => waiters.push(tx),
            _ => return Ok(None),
        }
    }
    rx.await.map_err(|_| Error::transport("monet-rn: prefetch dropped mid-join"))
}

/// Drops every entry (fresh and pending alike).
pub(crate) fn clear() {
    let mut cache = CACHE.lock().unwrap();
    cache.clear();
}
