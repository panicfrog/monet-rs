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

use crate::error::Error;
use crate::headers::HeaderMap;

/// A claim on a key's pending prefetch, handed out by [`begin`]: only the
/// prefetch holding the current ticket may complete or fail the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticket(u64);

/// One cached response: everything the mock arm needs to deliver it.
#[derive(Clone)]
pub struct CachedResponse {
    /// The status code.
    pub status: u16,
    /// The response headers.
    pub headers: HeaderMap,
    /// The complete body.
    pub body: Vec<u8>,
}

/// An in-flight prefetch: its ticket and the fetches joined to it.
struct Pending {
    ticket: Ticket,
    waiters: Vec<futures_channel::oneshot::Sender<Option<CachedResponse>>>,
}

/// A stored response; fresh until the reading request's TTL says
/// otherwise.
struct Stored {
    response: CachedResponse,
    stored_at: Instant,
}

/// Pending prefetches and stored responses live apart (nitro's shape):
/// a stored entry keeps serving while its refresh is in flight.
#[derive(Default)]
struct Cache {
    next_ticket: u64,
    pending: HashMap<String, Pending>,
    stored: HashMap<String, Stored>,
}

static CACHE: LazyLock<Mutex<Cache>> = LazyLock::new(Default::default);

/// Claims `key` for a prefetch. `None` while another prefetch for the key
/// is in flight (nitro's beginPending: racing prefetches start one
/// request). A stored entry never blocks — freshness is the reader's
/// call, so a new prefetch refreshes it.
pub fn begin(key: &str) -> Option<Ticket> {
    let mut cache = CACHE.lock().unwrap();
    if cache.pending.contains_key(key) {
        return None;
    }
    cache.next_ticket += 1;
    let ticket = Ticket(cache.next_ticket);
    cache.pending.insert(
        key.to_string(),
        Pending {
            ticket,
            waiters: Vec::new(),
        },
    );
    Some(ticket)
}

/// Takes `key`'s pending entry if `ticket` still owns it — a prefetch
/// cleared mid-flight (or superseded) must not touch the key.
fn take_pending(cache: &mut Cache, key: &str, ticket: Ticket) -> Option<Pending> {
    match cache.pending.get(key) {
        Some(pending) if pending.ticket == ticket => cache.pending.remove(key),
        _ => None,
    }
}

/// Stores the prefetched response, waking every joined fetch. A 5xx is
/// not stored — it ends the prefetch like [`fail`] (joined fetches fall
/// through to the network and their own retry policy) and returns
/// `false`. (nitro caches every status; a transient server error served
/// from the cache would skip the reader's request and retries.)
pub fn complete(key: &str, ticket: Ticket, response: CachedResponse) -> bool {
    if response.status >= 500 {
        fail(key, ticket);
        return false;
    }
    let waiters = {
        let mut cache = CACHE.lock().unwrap();
        let Some(pending) = take_pending(&mut cache, key, ticket) else {
            return false;
        };
        cache.stored.insert(
            key.to_string(),
            Stored {
                response: response.clone(),
                stored_at: Instant::now(),
            },
        );
        pending.waiters
    };
    for waiter in waiters {
        let _ = waiter.send(Some(response.clone()));
    }
    true
}

/// Ends a failed prefetch: joined fetches fall through to the network.
pub fn fail(key: &str, ticket: Ticket) {
    let waiters = {
        let mut cache = CACHE.lock().unwrap();
        take_pending(&mut cache, key, ticket).map(|pending| pending.waiters)
    };
    for waiter in waiters.into_iter().flatten() {
        let _ = waiter.send(None);
    }
}

/// The fresh-entry read (the fetch side). `None` when absent or stale
/// (past `ttl_ms`, 5s when 0) — a stale entry is evicted. A pending
/// prefetch is joined through [`join`] instead.
pub fn take_if_fresh(key: &str, ttl_ms: u64) -> Option<CachedResponse> {
    let mut cache = CACHE.lock().unwrap();
    let stored = cache.stored.get(key)?;
    let ttl = if ttl_ms == 0 { 5_000 } else { ttl_ms };
    if (stored.stored_at.elapsed().as_millis() as u64) < ttl {
        return Some(stored.response.clone());
    }
    cache.stored.remove(key);
    None
}

/// Joins a pending prefetch (the fetch side): resolves with the stored
/// response when it lands, `None` when the prefetch fails (the caller
/// falls through to the network). `Ok(None)` when nothing is pending
/// (check [`take_if_fresh`] first).
pub async fn join(key: &str) -> Result<Option<CachedResponse>, Error> {
    let (tx, rx) = futures_channel::oneshot::channel();
    {
        let mut cache = CACHE.lock().unwrap();
        match cache.pending.get_mut(key) {
            Some(pending) => pending.waiters.push(tx),
            None => return Ok(None),
        }
    }
    rx.await
        .map_err(|_| Error::transport("monet-rn: prefetch dropped mid-join"))
}

/// Drops every entry, stored and pending alike: joined fetches fall
/// through to the network, and the in-flight prefetches' late outcomes
/// are discarded (their tickets no longer match).
pub fn clear() {
    let mut cache = CACHE.lock().unwrap();
    cache.pending.clear();
    cache.stored.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16) -> CachedResponse {
        CachedResponse {
            status,
            headers: HeaderMap::new(),
            body: b"cached".to_vec(),
        }
    }

    fn is_cached(key: &str) -> bool {
        CACHE.lock().unwrap().stored.contains_key(key)
    }

    /// Freshness belongs to the reader (its TTL), so a stored entry never
    /// blocks the next prefetch — it refreshes the entry. Before, an
    /// entry blocked the key for good once it went stale.
    #[test]
    fn a_stored_entry_does_not_block_the_next_prefetch() {
        let key = "t-refresh";
        let ticket = begin(key).unwrap();
        complete(key, ticket, response(200));
        let again = begin(key).expect("the key is blocked by its stored entry");
        complete(key, again, response(201));
        assert_eq!(take_if_fresh(key, 5_000).unwrap().status, 201);
    }

    /// Racing prefetches still start one request.
    #[test]
    fn a_pending_prefetch_dedupes() {
        let key = "t-dedupe";
        let ticket = begin(key).unwrap();
        assert!(begin(key).is_none());
        fail(key, ticket);
        assert!(begin(key).is_some());
    }

    /// The fresh entry keeps serving while its refresh is in flight.
    #[test]
    fn a_fresh_entry_serves_during_its_refresh() {
        let key = "t-serve";
        let ticket = begin(key).unwrap();
        complete(key, ticket, response(200));
        let _refresh = begin(key);
        assert_eq!(
            take_if_fresh(key, 5_000).map(|r| r.status),
            Some(200),
            "the refresh hid the fresh entry"
        );
    }

    /// A read that finds the entry stale evicts it — entries otherwise
    /// lived until clear().
    #[test]
    fn a_stale_read_evicts_the_entry() {
        let key = "t-evict";
        let ticket = begin(key).unwrap();
        complete(key, ticket, response(200));
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(take_if_fresh(key, 1).is_none());
        assert!(!is_cached(key), "the stale entry stayed");
    }

    /// clear() during a pending prefetch: its late completion must not
    /// put the entry back.
    #[test]
    fn a_prefetch_cleared_mid_flight_stores_nothing() {
        let key = "t-cleared";
        let ticket = begin(key).unwrap();
        clear_key_for_test(key);
        complete(key, ticket, response(200));
        assert!(
            take_if_fresh(key, 5_000).is_none(),
            "the cleared prefetch landed"
        );
    }

    /// clear() drops every key; tests run in parallel on the shared
    /// cache, so this one drops just the key under test the same way.
    fn clear_key_for_test(key: &str) {
        let mut cache = CACHE.lock().unwrap();
        cache.pending.remove(key);
        cache.stored.remove(key);
    }

    /// A 5xx is a transient failure, not an answer to cache: a fetch
    /// served it from the cache would skip its own request and retry
    /// policy. Joined fetches fall through to the network instead.
    #[test]
    fn a_server_error_is_not_stored() {
        let key = "t-5xx";
        let ticket = begin(key).unwrap();
        let mut joined = Box::pin(join(key));
        assert!(
            futures_lite::future::block_on(futures_lite::future::poll_once(&mut joined)).is_none()
        );
        complete(key, ticket, response(503));
        assert!(take_if_fresh(key, 5_000).is_none(), "the 503 was cached");
        assert!(
            futures_lite::future::block_on(joined).unwrap().is_none(),
            "a joined fetch was handed the 503"
        );
    }

    /// A 4xx is the server's answer — stored like a 2xx.
    #[test]
    fn a_client_error_is_stored() {
        let key = "t-4xx";
        let ticket = begin(key).unwrap();
        complete(key, ticket, response(404));
        assert_eq!(take_if_fresh(key, 5_000).map(|r| r.status), Some(404));
    }
}
