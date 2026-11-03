//! Request registry: the Rust half of the bridge's request lifecycle.
//!
//! `execute` allocates an id and parks on a oneshot channel for the
//! response head; Swift data-plane events look the id up here. Terminal
//! events (`on_done`, `on_error`) and the cancel path remove entries, so
//! events for unknown ids — late arrivals after cancellation — are
//! ignored.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_channel::oneshot;

use monet_core::body::{BodySender, channel as body_channel};
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, PlatformErrorCode, Result};
use monet_core::headers::HeaderMap;
use monet_core::response::Response;

use crate::bridge;

/// `NSURLErrorCancelled` — the code a `transport_cancel`ed task reports.
const URL_ERROR_CANCELLED: i32 = -999;

struct RequestState {
    /// Resolves the execute future; taken by `on_response_head`.
    head_tx: Option<oneshot::Sender<Result<Response>>>,
    /// Accumulates headers between `on_header` and `on_response_head`.
    headers: HeaderMap,
    /// Body channel half, present between `on_response_head` and the
    /// terminal event.
    body_tx: Option<BodySender>,
    /// The request's cancel token, watched during the body phase.
    cancel: CancelToken,
    /// Dropping this shuts the body-phase cancel watcher down; it lives
    /// exactly as long as the state.
    watcher_done: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct Registry {
    next_id: AtomicU64,
    states: Mutex<HashMap<u64, RequestState>>,
}

static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::default);

/// Allocates a request id and the channel its response head arrives on.
/// Ids start at 1; 0 never occurs.
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
        },
    );
    (id, head_rx)
}

/// Drops a request's state without a terminal event (the cancel path:
/// Swift discards a never-started request silently).
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
    // fatal — same tolerance as monet-cronet.
    let _ = state.headers.append(&name, value);
}

pub(crate) fn on_response_head(id: u64, status: u16) {
    // Phase 1 (under the lock): take what the head needs and stage the
    // watcher wiring. Phase 2 (outside): send, then wire — never call
    // into Swift while holding the registry lock.
    enum Staged {
        Duplicate,
        Deliver {
            head_tx: oneshot::Sender<Result<Response>>,
            headers: HeaderMap,
            body: monet_core::body::Body,
            cancel: CancelToken,
            done_rx: oneshot::Receiver<()>,
        },
    }
    let staged = {
        let mut states = REGISTRY.states.lock().unwrap();
        let Some(state) = states.get_mut(&id) else {
            return;
        };
        let headers = std::mem::take(&mut state.headers);
        match state.head_tx.take() {
            Some(head_tx) => {
                let (body, body_tx) = body_channel();
                state.body_tx = Some(body_tx);
                let (done_tx, done_rx) = oneshot::channel();
                state.watcher_done = Some(done_tx);
                Staged::Deliver {
                    head_tx,
                    headers,
                    body,
                    cancel: state.cancel.clone(),
                    done_rx,
                }
            }
            // Head already delivered; a duplicate event violates the
            // ordering contract — ignore it.
            None => Staged::Duplicate,
        }
    };
    match staged {
        Staged::Deliver {
            head_tx,
            headers,
            body,
            cancel,
            done_rx,
        } => {
            if head_tx
                .send(Ok(Response::new(status, headers, body)))
                .is_ok()
            {
                spawn_body_watcher(id, cancel, done_rx);
            } else {
                // The execute future is gone (its cancel path won the
                // race): stop the native task and drop the orphaned state.
                bridge::transport_cancel(id);
                remove(id);
            }
        }
        Staged::Duplicate => {}
    }
}

pub(crate) fn on_chunk(id: u64, data: &[u8]) {
    let delivered = {
        let states = REGISTRY.states.lock().unwrap();
        match states.get(&id) {
            // Unknown id: a late chunk after cancellation — nothing to do.
            None => true,
            Some(state) => match &state.body_tx {
                Some(sender) => sender.send_chunk(Bytes::copy_from_slice(data)).is_ok(),
                // Chunks before the head violate the ordering contract;
                // ignore rather than crash a delegate thread.
                None => true,
            },
        }
    };
    if !delivered {
        // The consumer dropped the response body: stop the native task.
        // The cancellation error arrives later through on_error.
        bridge::transport_cancel(id);
    }
}

pub(crate) fn on_done(id: u64) {
    if let Some(state) = REGISTRY.states.lock().unwrap().remove(&id) {
        if let Some(sender) = state.body_tx {
            sender.complete();
        }
        if let Some(head_tx) = state.head_tx {
            // Completion without a head: report instead of hanging the
            // execute future.
            let _ = head_tx.send(Err(Error::transport(
                "urlsession: request completed without a response head",
            )));
        }
    }
}

pub(crate) fn on_error(id: u64, code: i32, message: &[u8]) {
    let error = map_error(code, message);
    if let Some(state) = REGISTRY.states.lock().unwrap().remove(&id) {
        if let Some(head_tx) = state.head_tx {
            let _ = head_tx.send(Err(error));
        } else if let Some(sender) = state.body_tx {
            sender.abort(error);
        }
    }
}

/// Maps an NSError code onto the core error type.
fn map_error(code: i32, message: &[u8]) -> Error {
    let message = String::from_utf8_lossy(message);
    if code == URL_ERROR_CANCELLED {
        return Error::Cancelled;
    }
    Error::transport_with_code(
        format!("urlsession: {message} (code {code})"),
        PlatformErrorCode::new("urlsession", i64::from(code)),
    )
}

/// Watches the cancel token during the body phase. Once the head is
/// delivered, nothing else polls the token — no runtime here, unlike
/// monet-rustls's pump task — so a parked thread (woken by the token's
/// waker) maps a late cancellation to `transport_cancel`. Removing the
/// request state drops the shutdown sender, which also wakes the thread
/// so it exits; a spawn failure only delays cancellation (the
/// dropped-body path still stops the task on the next chunk event).
fn spawn_body_watcher(id: u64, cancel: CancelToken, done: oneshot::Receiver<()>) {
    let spawned = std::thread::Builder::new()
        .name("monet-urlsession-cancel".into())
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
                // The task's didCompleteWithError(-999) then reports
                // through on_error, aborting the body with Cancelled.
                bridge::transport_cancel(id);
            }
        });
    if let Err(e) = spawned {
        // Thread-starved environment; degrade to event-driven cleanup.
        let _ = e;
    }
}
