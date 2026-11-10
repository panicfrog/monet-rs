//! Request registry: the Rust half of the HttpEngine bridge's request
//! lifecycle — the same shape as monet-cronet's and monet-urlsession's
//! registries, so all four transports share one mental model.
//!
//! `execute` allocates an id and parks on a oneshot for the response
//! head; Kotlin bridge callbacks (already attached to the JVM) look the
//! id up here. Terminal events and the cancel path remove entries, so
//! events for unknown ids — late arrivals after cancellation — are
//! ignored. Pure Rust: unit-testable on the host, no JNI types.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_channel::oneshot;

use monet_core::body::{BodySender, channel as body_channel};
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::headers::HeaderMap;
use monet_core::response::Response;

/// Cancels the native UrlRequest; the JNI layer supplies this (it knows
/// how to reach the request object).
pub(crate) type CancelNative = Arc<dyn Fn() + Send + Sync>;

struct RequestState {
    /// Resolves the execute future; taken by `head_delivered`.
    head_tx: Option<oneshot::Sender<Result<Response>>>,
    /// Accumulates headers between `on_header` and `head_delivered`.
    headers: HeaderMap,
    /// Body channel half, present between `head_delivered` and the
    /// terminal event.
    body_tx: Option<BodySender>,
    /// The request's cancel token, observed by the body watcher.
    cancel: CancelToken,
    /// Dropping this shuts the body watcher down; it lives exactly as
    /// long as the state.
    watcher_done: Option<oneshot::Sender<()>>,
    /// Cancels the native UrlRequest.
    cancel_native: Option<CancelNative>,
}

#[derive(Default)]
struct Registry {
    next_id: AtomicU64,
    states: Mutex<HashMap<u64, RequestState>>,
}

static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::default);

/// Cancels the native request for `id` if one is armed. The closure is
/// cloned out of the lock first — it re-enters the JVM, which may in
/// turn fire callbacks into the registry.
pub(crate) fn cancel_native(id: u64) {
    let cancel = {
        let states = REGISTRY.states.lock().unwrap();
        states
            .get(&id)
            .and_then(|state| state.cancel_native.clone())
    };
    if let Some(cancel) = cancel {
        cancel();
    }
}

/// Allocates a request id and the channel its response head arrives on.
pub(crate) fn allocate(cancel: CancelToken) -> (u64, oneshot::Receiver<Result<Response>>) {
    let (head_tx, head_rx) = oneshot::channel();
    let id = REGISTRY.next_id.fetch_add(1, Ordering::Relaxed) + 1;
    REGISTRY.states.lock().unwrap().insert(
        id,
        RequestState {
            head_tx: Some(head_tx),
            headers: HeaderMap::new(),
            body_tx: None,
            cancel,
            watcher_done: None,
            cancel_native: None,
        },
    );
    (id, head_rx)
}

/// Registers how to cancel the native request.
pub(crate) fn arm_native_cancel(id: u64, cancel: CancelNative) {
    let mut states = REGISTRY.states.lock().unwrap();
    if let Some(state) = states.get_mut(&id) {
        state.cancel_native = Some(cancel);
    }
}

/// Drops the Rust state without a terminal event (the pre-head cancel
/// path; the caller triggers the native cancel itself).
pub(crate) fn remove(id: u64) {
    REGISTRY.states.lock().unwrap().remove(&id);
}

pub(crate) fn on_header(id: u64, name: &[u8], value: &[u8]) {
    let mut states = REGISTRY.states.lock().unwrap();
    let Some(state) = states.get_mut(&id) else {
        return;
    };
    let name = String::from_utf8_lossy(name);
    let value = String::from_utf8_lossy(value);
    // A response header that fails our validation rules is dropped, not
    // fatal — the same tolerance every transport applies.
    let _ = state.headers.append(&name, value);
}

/// Completes the head: assembles the accumulated headers, resolves the
/// execute future, and arms the body channel. The caller (the JNI bridge)
/// starts pulling bytes after this returns.
pub(crate) fn head_delivered(id: u64, status: u16) {
    type Staged = (
        oneshot::Sender<Result<Response>>,
        HeaderMap,
        monet_core::body::Body,
        CancelToken,
        oneshot::Receiver<()>,
    );
    let mut staged: Option<Staged> = None;
    {
        let mut states = REGISTRY.states.lock().unwrap();
        if let Some(state) = states.get_mut(&id) {
            let headers = std::mem::take(&mut state.headers);
            if let Some(head_tx) = state.head_tx.take() {
                let (body, body_tx) = body_channel();
                state.body_tx = Some(body_tx);
                let (done_tx, done_rx) = oneshot::channel();
                state.watcher_done = Some(done_tx);
                staged = Some((head_tx, headers, body, state.cancel.clone(), done_rx));
            }
            // A state without head_tx is a duplicate head event — the
            // ordering contract forbids it; ignore.
        }
    }
    if let Some((head_tx, headers, body, cancel, done_rx)) = staged {
        if head_tx
            .send(Ok(Response::new(status, headers, body)))
            .is_ok()
        {
            spawn_body_watcher(id, cancel, done_rx);
        } else {
            // The execute future is gone (its cancel path won the race):
            // stop the native task and drop the orphaned state.
            cancel_native(id);
            remove(id);
        }
    }
}

pub(crate) fn on_chunk(id: u64, data: &[u8]) {
    let delivered = {
        let states = REGISTRY.states.lock().unwrap();
        match states.get(&id) {
            None => true, // late chunk after cancellation — nothing to do
            Some(state) => match &state.body_tx {
                Some(sender) => sender.send_chunk(Bytes::copy_from_slice(data)).is_ok(),
                None => true,
            },
        }
    };
    if !delivered {
        // The consumer dropped the response body: stop the native task.
        cancel_native(id);
    }
}

pub(crate) fn on_done(id: u64) {
    if let Some(state) = REGISTRY.states.lock().unwrap().remove(&id) {
        if let Some(sender) = state.body_tx {
            sender.complete();
        }
        if let Some(head_tx) = state.head_tx {
            let _ = head_tx.send(Err(Error::transport(
                "httpengine: request completed without a response head",
            )));
        }
    }
}

pub(crate) fn on_error(id: u64, message: &str) {
    let error = Error::transport(format!("httpengine: {message}"));
    if let Some(state) = REGISTRY.states.lock().unwrap().remove(&id) {
        if let Some(head_tx) = state.head_tx {
            let _ = head_tx.send(Err(error));
        } else if let Some(sender) = state.body_tx {
            sender.abort(error);
        }
    }
}

/// The native request was cancelled (`UrlRequest.cancel()`): surfaces as
/// `Error::Cancelled` on whichever side is still listening.
/// The native request was cancelled (`UrlRequest.cancel()`): surfaces as
/// `Error::Cancelled` on whichever side is still listening. Wired from
/// the bridge's onCanceled entry; host tests exercise it directly.
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
pub(crate) fn on_cancelled(id: u64) {
    let error = Error::Cancelled;
    if let Some(state) = REGISTRY.states.lock().unwrap().remove(&id) {
        if let Some(head_tx) = state.head_tx {
            let _ = head_tx.send(Err(error));
        } else if let Some(sender) = state.body_tx {
            sender.abort(error);
        }
    }
}

/// Watches the cancel token during the body phase (same parked-thread
/// approach as monet-urlsession: no runtime here, and nothing else polls
/// the token once the head has resolved).
fn spawn_body_watcher(id: u64, cancel: CancelToken, done: oneshot::Receiver<()>) {
    let spawned = std::thread::Builder::new()
        .name("monet-httpengine-cancel".into())
        .spawn(move || {
            futures_lite::future::block_on(futures_lite::future::race(
                async {
                    let _ = done.await;
                },
                async {
                    cancel.cancelled().await;
                },
            ));
            if cancel.is_cancelled() {
                cancel_native(id);
            }
        });
    if spawned.is_err() {
        // Thread-starved: degrade to event-driven cleanup.
        cancel_native(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::StreamExt;

    fn response_body(response: Response) -> Vec<u8> {
        let chunks = futures_lite::future::block_on(response.body.collect::<Vec<_>>());
        chunks
            .into_iter()
            .map(|chunk| chunk.unwrap())
            .collect::<Vec<Bytes>>()
            .concat()
            .to_vec()
    }

    #[test]
    fn head_then_body_then_done() {
        let (id, rx) = allocate(CancelToken::new());
        arm_native_cancel(id, Arc::new(|| {}));
        on_header(id, b"content-type", b"text/plain");
        head_delivered(id, 200);
        let response = futures_lite::future::block_on(rx).unwrap().unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.headers.get("content-type").unwrap(), "text/plain");

        on_chunk(id, b"abc");
        on_chunk(id, b"def");
        on_done(id);
        assert_eq!(response_body(response), b"abcdef".to_vec());
    }

    #[test]
    fn unknown_ids_are_ignored() {
        on_header(9999, b"a", b"b");
        on_chunk(9999, b"x");
        on_done(9999);
        on_error(9999, "stale");
        head_delivered(9999, 200);
    }

    #[test]
    fn error_before_head_rejects_the_future() {
        let (id, rx) = allocate(CancelToken::new());
        on_error(id, "connection refused");
        let err = futures_lite::future::block_on(rx).unwrap().unwrap_err();
        assert!(
            matches!(err, Error::Transport { message: ref m, .. } if m.contains("refused")),
            "got: {err:?}"
        );
    }

    #[test]
    fn error_after_head_aborts_the_body() {
        let (id, rx) = allocate(CancelToken::new());
        head_delivered(id, 200);
        let response = futures_lite::future::block_on(rx).unwrap().unwrap();
        on_chunk(id, b"partial");
        on_error(id, "broken pipe");
        let mut body = response.body;
        let first = futures_lite::future::block_on(body.next())
            .unwrap()
            .unwrap();
        assert_eq!(first, Bytes::from_static(b"partial"));
        let second = futures_lite::future::block_on(body.next()).unwrap();
        assert!(matches!(second, Err(Error::Transport { .. })));
    }

    #[test]
    fn drop_native_cancel_fires_when_body_consumer_vanishes() {
        let (id, rx) = allocate(CancelToken::new());
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = fired.clone();
        arm_native_cancel(id, Arc::new(move || probe.store(true, Ordering::SeqCst)));
        head_delivered(id, 200);
        let response = futures_lite::future::block_on(rx).unwrap().unwrap();
        drop(response.body);
        on_chunk(id, b"unwanted");
        assert!(fired.load(Ordering::SeqCst));
    }
}
