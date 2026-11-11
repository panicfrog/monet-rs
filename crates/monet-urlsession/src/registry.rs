//! Request registry: the Rust half of the bridge's request lifecycle.
//!
//! `execute` allocates an id and parks on a oneshot channel for the
//! response head; Swift data-plane events look the id up here. Terminal
//! events (`on_done`, `on_error`) and the cancel path remove entries, so
//! events for unknown ids — late arrivals after cancellation — are
//! ignored.

use std::collections::{HashMap, VecDeque};
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;

use monet_core::body::{BoundedBodySender, Delivery, bounded as body_bounded};
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, PlatformErrorCode, Result};
use monet_core::headers::HeaderMap;
use monet_core::response::Response;

use crate::bridge;
use crate::valve_log;

/// `NSURLErrorCancelled` — the code a `transport_cancel`ed task reports.
const URL_ERROR_CANCELLED: i32 = -999;

/// The body watcher's wake reasons.
enum Wake {
    /// A chunk did not fit the bounded channel: drain the overflow,
    /// then resume the suspended task.
    Stall,
    /// A terminal event was staged: drain, then apply it.
    Terminal,
}

/// A terminal event, staged by `on_done`/`on_error` and applied by the
/// watcher after the overflow drains — a terminal must not cut in front
/// of stalled data, and applying it may need awaited sends (the bounded
/// channel can be full when the response ends).
enum Terminal {
    Done,
    Fail(Error),
}

struct RequestState {
    /// Resolves the execute future; taken by `on_response_head`.
    head_tx: Option<oneshot::Sender<Result<Response>>>,
    /// Accumulates headers between `on_header` and `on_response_head`.
    headers: HeaderMap,
    /// Body channel half, present between `on_response_head` and the
    /// terminal's application. Bounded (v2/W2b): a full channel
    /// suspends the task (Swift side), so a slow consumer pauses the
    /// download instead of buffering the whole response. The effective
    /// capacity is cap + 1 (futures-channel semantics).
    body_tx: Option<BoundedBodySender>,
    /// Chunks that did not fit the bounded channel. The consumer's pace
    /// is entirely Rust-side (v2/W2b redesign: task.suspend() is not a
    /// reliable pause primitive — see the watcher's doc); the overflow
    /// holds whatever URLSession delivered while the channel was full.
    overflow: Mutex<VecDeque<Bytes>>,
    /// Wakes the body watcher (a stall or a staged terminal). Dropping
    /// the sender — the state's removal — ends the watcher's stream.
    wake_tx: Option<UnboundedSender<Wake>>,
    /// The staged terminal, applied by the watcher post-drain.
    terminal: Mutex<Option<Terminal>>,
    /// The request's cancel token, watched during the body phase.
    cancel: CancelToken,
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
            overflow: Mutex::new(VecDeque::new()),
            wake_tx: None,
            terminal: Mutex::new(None),
            cancel,
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
            wake_rx: UnboundedReceiver<Wake>,
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
                let (body, body_tx) = body_bounded(4);
                state.body_tx = Some(body_tx);
                let (wake_tx, wake_rx) = unbounded();
                state.wake_tx = Some(wake_tx);
                Staged::Deliver {
                    head_tx,
                    headers,
                    body,
                    cancel: state.cancel.clone(),
                    wake_rx,
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
            wake_rx,
        } => {
            if head_tx
                .send(Ok(Response::new(status, headers, body)))
                .is_ok()
            {
                valve_log(&format!("HEAD id={id} delivered; spawning watcher"));
                spawn_body_watcher(id, cancel, wake_rx);
            } else {
                // The execute future is gone (its cancel path won the
                // race): stop the native task and drop the orphaned state.
                valve_log(&format!("HEAD id={id} execute-gone; cancelling"));
                bridge::transport_cancel(id);
                remove(id);
            }
        }
        Staged::Duplicate => {
            valve_log(&format!("HEAD id={id} duplicate ignored"));
        }
    }
}

/// Delivers one body chunk from the delegate's data event. `true` means
/// the consumer is gone (the channel closed): the caller stops the
/// native task. The consumer's pace is enforced entirely on this side
/// (v2/W2b redesign): a Full probe parks the chunk in the overflow and
/// wakes the body watcher, which drains at the consumer's pace — no
/// task.suspend() (the instrumented evidence in the plan's W2b record:
/// balanced suspend/resume pairing with the task never delivering
/// again; a resumed URLSessionTask can simply stop delivering data and
/// completion).
pub(crate) fn on_chunk(id: u64, data: &[u8]) {
    #[derive(PartialEq)]
    enum After {
        Continue,
        /// The channel is full: the chunk joins the overflow; the
        /// watcher drains it at the consumer's pace.
        Overflow,
        ConsumerGone,
    }
    let _ = 0; // (the match arms below stay expression-typed)
    let after = {
        let states = REGISTRY.states.lock().unwrap();
        match states.get(&id) {
            // Unknown id: a late chunk after cancellation — nothing to do.
            None => {
                valve_log(&format!(
                    "CHUNK id={id} late (state gone) len={}",
                    data.len()
                ));
                After::Continue
            }
            Some(state) => match &state.body_tx {
                Some(sender) => match sender.try_send(Bytes::copy_from_slice(data)) {
                    Delivery::Sent => After::Continue,
                    Delivery::Full(chunk) => {
                        valve_log(&format!("CHUNK id={id} full len={}", data.len()));
                        state.overflow.lock().unwrap().push_back(chunk);
                        After::Overflow
                    }
                    Delivery::Closed => {
                        valve_log(&format!("CHUNK id={id} consumer-gone len={}", data.len()));
                        After::ConsumerGone
                    }
                },
                // Chunks before the head violate the ordering contract;
                // ignore rather than crash a delegate thread.
                None => After::Continue,
            },
        }
    };
    match after {
        After::Continue => {}
        // Outside the states lock: notify wakes the watcher, which
        // drains at the consumer's pace.
        After::Overflow => {
            if let Some(wake_tx) = REGISTRY
                .states
                .lock()
                .unwrap()
                .get(&id)
                .and_then(|state| state.wake_tx.as_ref())
            {
                let _ = wake_tx.unbounded_send(Wake::Stall);
            }
        }
        // The consumer dropped the response body: stop the native task.
        // The cancellation error arrives later through on_error.
        After::ConsumerGone => {
            bridge::transport_cancel(id);
        }
    }
}

pub(crate) fn on_done(id: u64) {
    stage_terminal(
        id,
        Terminal::Done,
        "urlsession: request completed without a response head",
    );
}

pub(crate) fn on_error(id: u64, code: i32, message: &[u8]) {
    stage_terminal(id, Terminal::Fail(map_error(code, message)), "");
}

/// One terminal event, either side of the head. Post-head the terminal
/// is staged for the body watcher (it must wait out the overflow, and
/// applying it may need awaited sends on a full channel); pre-head it
/// resolves the head future directly. The first terminal wins — a
/// spurious didComplete after a delivered one must not overwrite it.
fn stage_terminal(id: u64, terminal: Terminal, pre_head_note: &str) {
    // The terminal travels with the act: the staging branch moves it
    // into the slot, the pre-head branch carries it to the head error.
    enum Act {
        Staged,
        PreHead {
            head_tx: Option<oneshot::Sender<Result<Response>>>,
            terminal: Terminal,
        },
    }
    let act = {
        let mut states = REGISTRY.states.lock().unwrap();
        let post_head = states.get(&id).is_some_and(|state| state.body_tx.is_some());
        if post_head {
            let state = states.get_mut(&id).expect("just checked");
            let mut slot = state.terminal.lock().unwrap();
            if slot.is_none() {
                *slot = Some(terminal);
                let wake = state
                    .wake_tx
                    .as_ref()
                    .map(|tx| tx.unbounded_send(Wake::Terminal).is_ok());
                valve_log(&format!("TERMINAL id={id} staged wake={wake:?}"));
            } else {
                valve_log(&format!("TERMINAL id={id} duplicate-slot ignored"));
            }
            Act::Staged
        } else {
            Act::PreHead {
                head_tx: states.remove(&id).and_then(|state| state.head_tx),
                terminal,
            }
        }
    };
    match act {
        Act::Staged => {}
        Act::PreHead {
            head_tx: Some(head_tx),
            terminal,
        } => {
            let error = match terminal {
                Terminal::Fail(error) => error,
                Terminal::Done => Error::transport(pre_head_note),
            };
            let _ = head_tx.send(Err(error));
        }
        Act::PreHead { head_tx: None, .. } => {}
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

/// The body watcher. Once the head is delivered, nothing else polls the
/// cancel token — no runtime here, unlike monet-rustls's pump task — so
/// a parked thread (woken by the token's waker) maps a late
/// cancellation to `transport_cancel`. It also owns the backpressure
/// machinery (v2/W2b): on a stall it drains the overflow into the
/// bounded channel at the consumer's pace and resumes the suspended
/// task; on a staged terminal it drains first, then applies it. The
/// wake stream ends (and the watcher exits) when the state is removed;
/// cancel exits it too.
fn spawn_body_watcher(id: u64, cancel: CancelToken, mut wake_rx: UnboundedReceiver<Wake>) {
    let spawned = std::thread::Builder::new()
        .name("monet-urlsession-body".into())
        .spawn(move || {
            valve_log(&format!("WATCHER id={id} thread-entered"));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                futures_lite::future::block_on(async {
                    use futures_lite::StreamExt as _;
                    // Cancel is observed once: it fires the native cancel,
                    // but the watcher keeps waiting — the terminal the
                    // cancellation produces arrives asynchronously (the
                    // delegate's didCompleteWithError), and applying it is
                    // this loop's job.
                    let mut cancelled = false;
                    loop {
                        enum Step {
                            Wake(Option<Wake>),
                            Cancelled,
                        }
                        let step = if cancelled {
                            Step::Wake(wake_rx.next().await)
                        } else {
                            futures_lite::future::race(
                                async { Step::Wake(wake_rx.next().await) },
                                async {
                                    cancel.cancelled().await;
                                    Step::Cancelled
                                },
                            )
                            .await
                        };
                        match step {
                            // The state is gone (removed elsewhere): exit.
                            Step::Wake(None) => return,
                            Step::Cancelled => {
                                // The task's didCompleteWithError(-999) then
                                // reports through on_error, which stages the
                                // terminal this loop applies.
                                crate::valve_log(&format!("WATCH id={id} cancelled"));
                                bridge::transport_cancel(id);
                                cancelled = true;
                            }
                            Step::Wake(Some(reason)) => {
                                // Every path drains first: a terminal must
                                // not cut in front of stalled data, and a
                                // stall resumes only what it actually
                                // delivered.
                                let reason_tag = match reason {
                                    Wake::Stall => "stall",
                                    Wake::Terminal => "terminal",
                                };
                                valve_log(&format!("WATCHER id={id} woke: {reason_tag}"));
                                match drain_overflow(id).await {
                                    // The consumer is gone: stop the task.
                                    None => {
                                        crate::valve_log(&format!(
                                            "WATCH id={id} {reason_tag} consumer-gone"
                                        ));
                                        bridge::transport_cancel(id);
                                        return;
                                    }
                                    // A stall wake on a live request: the drain
                                    // reached empty (this pass delivered the
                                    // chunks, or a previous pass already did —
                                    // Some(0) must ALSO attempt the resume: a
                                    // suspend that landed after that pass's
                                    // clear, against an opener whose chunk the
                                    // pass drained, is outstanding with only
                                    // this wake left to resume it; the Swift
                                    // count gates the actual resume to
                                    // one-per-suspend). A cancel was observed:
                                    // no resume — the task's terminal is on its
                                    // way (a suspended task's cancel may deliver
                                    // late).
                                    Some(n) if matches!(reason, Wake::Stall) && !cancelled => {
                                        // The drain IS the consumer's pace
                                        // (v2/W2b redesign: no task resume —
                                        // nothing was suspended).
                                        crate::valve_log(&format!(
                                            "WATCH id={id} {reason_tag} drained={n}"
                                        ));
                                    }
                                    Some(n) => {
                                        crate::valve_log(&format!(
                                            "WATCH id={id} {reason_tag} drained={n} terminal"
                                        ));
                                        apply_terminal(id);
                                        return;
                                    }
                                }
                            }
                        }
                    }
                });
            }));
            match result {
                Ok(()) => {}
                Err(payload) => valve_log(&format!(
                    "WATCHER id={id} PANICKED: {:?}",
                    payload.downcast_ref::<&str>().copied()
                )),
            }
        });
    match spawned {
        Ok(_) => valve_log(&format!("WATCHER id={id} spawn-ok")),
        Err(e) => {
            // Thread-starved environment; degrade to event-driven cleanup.
            valve_log(&format!("WATCHER id={id} SPAWN FAILED: {e}"));
        }
    }
}

/// Drains the overflow into the bounded channel, one awaited send per
/// chunk — the consumer's pace is the pause the suspended task is
/// spending. Returns how many chunks were delivered; `None` when the
/// consumer is gone (the channel closed).
async fn drain_overflow(id: u64) -> Option<usize> {
    let mut drained = 0;
    loop {
        let (chunk, sender) = {
            let states = REGISTRY.states.lock().unwrap();
            let Some(state) = states.get(&id) else {
                return Some(drained);
            };
            let chunk = state.overflow.lock().unwrap().pop_front();
            match (chunk, state.body_tx.as_ref()) {
                (Some(chunk), Some(sender)) => (chunk, sender.clone()),
                // Nothing pending (or already terminated): the drain is
                // done.
                (None, _) | (Some(_), None) => return Some(drained),
            }
        };
        // The send awaits outside the registry lock.
        if sender.send(chunk).await.is_err() {
            return None;
        }
        drained += 1;
    }
}

/// Applies the staged terminal: completes or errors the body, then
/// drops the state (which ends the wake stream). The caller has already
/// drained the overflow.
fn apply_terminal(id: u64) {
    let (terminal, sender) = {
        let mut states = REGISTRY.states.lock().unwrap();
        let Some(state) = states.get_mut(&id) else {
            return;
        };
        (state.terminal.lock().unwrap().take(), state.body_tx.take())
    };
    match terminal {
        Some(Terminal::Fail(error)) => {
            if let Some(sender) = sender {
                sender.abort(error);
            }
        }
        // Done, or nothing staged (the state was terminal-less — the
        // wake came from a path that removed it): complete cleanly.
        Some(Terminal::Done) | None => {
            if let Some(sender) = sender {
                sender.complete();
            }
        }
    }
    remove(id);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stall episode's contract (the Swift suspend's re-check and
    /// pairing): the episode opener suspends (returns true), a chunk
    /// arriving mid-episode does not (one suspend per episode —
    /// URLSessionTask's suspend/resume are counted), the drain's
    /// completion clears it, and an unknown id must never read as
    /// stalled (a gone request must not suspend).
    #[test]
    fn full_chunks_overflow_and_the_watcher_drains() {
        let (id, rx) = allocate(CancelToken::new());
        on_response_head(id, 200);
        let response = futures_lite::future::block_on(rx).unwrap().unwrap();

        // Fill the bounded channel (effective capacity 5); everything
        // past it parks in the overflow until the consumer takes.
        for i in 0..5u8 {
            on_chunk(id, format!("chunk{i}").as_bytes());
        }
        on_chunk(id, b"overflow-1");
        on_chunk(id, b"overflow-2");

        // Taking from the body releases capacity; the watcher's drain
        // (woken by the stall) moves the overflow in at this pace.
        use futures_lite::StreamExt as _;
        let mut body = response.body;
        let first = futures_lite::future::block_on(body.next())
            .unwrap()
            .unwrap();
        assert_eq!(&first[..], b"chunk0");

        // Releasing the body unblocks the parked watcher (its send
        // errors out) so the test leaves no thread behind.
        drop(body);
    }
}
