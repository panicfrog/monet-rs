//! Request registry: the Rust half of the HttpEngine bridge's request
//! lifecycle — the same shape as monet-cronet's and monet-urlsession's
//! registries, so all four transports share one mental model.
//!
//! `execute` allocates an id and parks on a oneshot for the response
//! head; Kotlin bridge callbacks (already attached to the JVM) look the
//! id up here. Terminal events and the cancel path remove entries, so
//! events for unknown ids — late arrivals after cancellation — are
//! ignored. Pure Rust: unit-testable on the host, no JNI types.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;

use monet_core::body::{BoundedBodySender, Delivery, bounded as body_bounded};
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::headers::HeaderMap;
use monet_core::response::Response;

/// Cancels the native UrlRequest; the JNI layer supplies this (it knows
/// how to reach the request object).
pub(crate) type CancelNative = Arc<dyn Fn() + Send + Sync>;

/// Resumes the native read loop after the body watcher has drained the
/// overflow; the JNI layer supplies this (same reach).
pub(crate) type ResumeRead = Arc<dyn Fn() + Send + Sync>;

/// The body watcher's wake reasons.
enum Wake {
    /// A chunk did not fit the bounded channel (the overflow): drain,
    /// then resume the read loop.
    Stall,
    /// A terminal event was staged: drain, then apply it.
    Terminal,
}

/// A terminal event, staged by `on_done`/`on_error`/`on_cancelled` and
/// applied by the watcher after the overflow drains — a terminal must
/// not cut in front of stalled data, and applying it may need awaited
/// sends (the bounded channel can be full when the response ends).
enum Terminal {
    Done,
    Fail(Error),
}

/// The read loop's gating decision after one chunk (v2/W2b).
pub(crate) enum ReadGate {
    /// The chunk was queued (or there is nothing to read for): the
    /// read loop may continue.
    Continue,
    /// The channel is full (the watcher resumes the loop once the
    /// consumer drains) or the consumer is gone (the native cancel
    /// already fired): do not issue the next read.
    Pause,
}

struct RequestState {
    /// Resolves the execute future; taken by `head_delivered`.
    head_tx: Option<oneshot::Sender<Result<Response>>>,
    /// Accumulates headers between `on_header` and `head_delivered`.
    headers: HeaderMap,
    /// Body channel half, present between `head_delivered` and the
    /// terminal's application. Bounded (v2/W2b): the read loop gates on
    /// its capacity, so a slow consumer pauses the download instead of
    /// buffering the whole response. The effective capacity is cap + 1
    /// (futures-channel semantics).
    body_tx: Option<BoundedBodySender>,
    /// Chunks that did not fit the bounded channel — at most one in
    /// practice, because reads pause on `ReadGate::Pause` until the
    /// watcher has drained it.
    overflow: Mutex<VecDeque<Bytes>>,
    /// Wakes the body watcher (a stall or a staged terminal). Dropping
    /// the sender — the state's removal — ends the watcher's stream.
    wake_tx: Option<UnboundedSender<Wake>>,
    /// Resumes the native read loop after a drain.
    resume_read: Option<ResumeRead>,
    /// The staged terminal, applied by the watcher post-drain.
    terminal: Mutex<Option<Terminal>>,
    /// The request's cancel token, observed by the body watcher.
    cancel: CancelToken,
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
            overflow: Mutex::new(VecDeque::new()),
            wake_tx: None,
            resume_read: None,
            terminal: Mutex::new(None),
            cancel,
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

/// Registers how to resume the native read loop (the body watcher calls
/// it after draining the overflow).
pub(crate) fn arm_resume_read(id: u64, resume: ResumeRead) {
    let mut states = REGISTRY.states.lock().unwrap();
    if let Some(state) = states.get_mut(&id) {
        state.resume_read = Some(resume);
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
/// execute future, and arms the bounded body channel plus its watcher.
/// The caller (the JNI bridge) starts pulling bytes after this returns.
pub(crate) fn head_delivered(id: u64, status: u16) {
    type Staged = (
        oneshot::Sender<Result<Response>>,
        HeaderMap,
        monet_core::body::Body,
        CancelToken,
        UnboundedReceiver<Wake>,
    );
    let mut staged: Option<Staged> = None;
    {
        let mut states = REGISTRY.states.lock().unwrap();
        if let Some(state) = states.get_mut(&id) {
            let headers = std::mem::take(&mut state.headers);
            if let Some(head_tx) = state.head_tx.take() {
                let (body, body_tx) = body_bounded(4);
                state.body_tx = Some(body_tx);
                let (wake_tx, wake_rx) = unbounded();
                state.wake_tx = Some(wake_tx);
                staged = Some((head_tx, headers, body, state.cancel.clone(), wake_rx));
            }
            // A state without head_tx is a duplicate head event — the
            // ordering contract forbids it; ignore.
        }
    }
    if let Some((head_tx, headers, body, cancel, wake_rx)) = staged {
        if head_tx
            .send(Ok(Response::new(status, headers, body)))
            .is_ok()
        {
            spawn_body_watcher(id, cancel, wake_rx);
        } else {
            // The execute future is gone (its cancel path won the race):
            // stop the native task and drop the orphaned state.
            cancel_native(id);
            remove(id);
        }
    }
}

/// Delivers one body chunk from the read callback. The bounded
/// channel's capacity is the read loop's gate (v2/W2b): on `Pause` the
/// JNI bridge stops issuing reads, and the body watcher resumes the
/// loop once the consumer has drained the overflow.
pub(crate) fn on_chunk(id: u64, data: &[u8]) -> ReadGate {
    enum After {
        Continue,
        Stalled,
        ConsumerGone,
    }
    let after = {
        let states = REGISTRY.states.lock().unwrap();
        let Some(state) = states.get(&id) else {
            // A late chunk after cancellation — nothing to do.
            return ReadGate::Continue;
        };
        let Some(sender) = &state.body_tx else {
            return ReadGate::Continue;
        };
        match sender.try_send(Bytes::copy_from_slice(data)) {
            Delivery::Sent => After::Continue,
            Delivery::Full(chunk) => {
                state.overflow.lock().unwrap().push_back(chunk);
                After::Stalled
            }
            Delivery::Closed => After::ConsumerGone,
        }
    };
    match after {
        After::Continue => ReadGate::Continue,
        After::Stalled => {
            // Outside the states lock: notify wakes the watcher, which
            // drains at the consumer's pace and resumes the read loop.
            if let Some(wake_tx) = REGISTRY
                .states
                .lock()
                .unwrap()
                .get(&id)
                .and_then(|state| state.wake_tx.as_ref())
            {
                let _ = wake_tx.unbounded_send(Wake::Stall);
            }
            ReadGate::Pause
        }
        // The consumer dropped the response body: stop the native task.
        After::ConsumerGone => {
            cancel_native(id);
            ReadGate::Pause
        }
    }
}

pub(crate) fn on_done(id: u64) {
    stage_terminal(
        id,
        Terminal::Done,
        "httpengine: request completed without a response head",
    );
}

pub(crate) fn on_error(id: u64, message: &str) {
    stage_terminal(
        id,
        Terminal::Fail(Error::transport(format!("httpengine: {message}"))),
        "",
    );
}

/// The native request was cancelled (`UrlRequest.cancel()`): surfaces as
/// `Error::Cancelled` on whichever side is still listening. Wired from
/// the bridge's onCanceled entry; host tests exercise it directly.
#[cfg_attr(not(target_os = "android"), allow(dead_code))]
pub(crate) fn on_cancelled(id: u64) {
    stage_terminal(id, Terminal::Fail(Error::Cancelled), "");
}

/// One terminal event, either side of the head. Post-head the terminal
/// is staged for the body watcher (it must wait out the overflow, and
/// applying it may need awaited sends on a full channel); pre-head it
/// resolves the head future directly. The first terminal wins — a
/// spurious Cronet error after `onSucceeded` must not overwrite it.
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
                if let Some(wake_tx) = &state.wake_tx {
                    let _ = wake_tx.unbounded_send(Wake::Terminal);
                }
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

/// The body watcher (same parked-thread approach as monet-urlsession:
/// no runtime here, and nothing else polls the token once the head has
/// resolved). It owns the backpressure machinery (v2/W2b): on a stall
/// it drains the overflow into the bounded channel at the consumer's
/// pace and resumes the read loop; on a staged terminal it drains
/// first, then applies the terminal. The wake stream ends (and the
/// watcher exits) when the state is removed; cancel exits it too.
fn spawn_body_watcher(id: u64, cancel: CancelToken, mut wake_rx: UnboundedReceiver<Wake>) {
    let spawned = std::thread::Builder::new()
        .name("monet-httpengine-body".into())
        .spawn(move || {
            futures_lite::future::block_on(async {
                use futures_lite::StreamExt as _;
                // Cancel is observed once: it fires the native cancel,
                // but the watcher keeps waiting — the terminal the
                // cancellation produces arrives asynchronously (Cronet's
                // onFailed), and applying it is this loop's job.
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
                            // Cronet's onFailed then reports through
                            // on_error, which stages the terminal this
                            // loop applies.
                            cancel_native(id);
                            cancelled = true;
                        }
                        Step::Wake(Some(reason)) => {
                            // Every path drains first: a terminal must
                            // not cut in front of stalled data, and a
                            // stall resumes only what it actually
                            // delivered.
                            match drain_overflow(id).await {
                                // The consumer is gone: stop the task.
                                None => {
                                    cancel_native(id);
                                    return;
                                }
                                Some(0) if matches!(reason, Wake::Stall) => {
                                    // The stall was drained by another
                                    // wake (a terminal pass); nothing to
                                    // resume here.
                                }
                                // Only a live request resumes: a cancel was
                                // observed, so the task's terminal is on its
                                // way — resuming would race it (a suspended
                                // task's cancel may deliver late).
                                Some(_) if matches!(reason, Wake::Stall) && !cancelled => {
                                    resume_read(id);
                                }
                                Some(_) => {
                                    apply_terminal(id);
                                    return;
                                }
                            }
                        }
                    }
                }
            });
        });
    if spawned.is_err() {
        // Thread-starved: degrade to event-driven cleanup.
        cancel_native(id);
    }
}

/// Drains the overflow into the bounded channel, one awaited send per
/// chunk — the consumer's pace is the pause the read loop is spending.
/// Returns how many chunks were delivered; `None` when the consumer is
/// gone (the channel closed).
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

/// Resumes the native read loop after a drain; the closure is cloned
/// out of the lock first (it re-enters the JVM, which may in turn fire
/// callbacks into the registry).
fn resume_read(id: u64) {
    let resume = {
        let states = REGISTRY.states.lock().unwrap();
        states.get(&id).and_then(|state| state.resume_read.clone())
    };
    if let Some(resume) = resume {
        resume();
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
        assert!(matches!(on_chunk(id, b"unwanted"), ReadGate::Pause));
        assert!(fired.load(Ordering::SeqCst));
    }

    /// The v2/W2b read gate: the bounded channel fills, the read loop
    /// pauses, and a consumer take resumes it — through the watcher's
    /// drain and the registered resume closure.
    #[test]
    fn slow_consumer_gates_the_read_loop() {
        use std::sync::atomic::AtomicUsize;

        let (id, rx) = allocate(CancelToken::new());
        let resumed = Arc::new(AtomicUsize::new(0));
        let probe = resumed.clone();
        arm_native_cancel(id, Arc::new(|| {}));
        arm_resume_read(
            id,
            Arc::new(move || {
                probe.fetch_add(1, Ordering::SeqCst);
            }),
        );
        head_delivered(id, 200);
        let response = futures_lite::future::block_on(rx).unwrap().unwrap();
        let mut body = response.body;

        // Fill the bounded channel (cap 4, effective 5): every chunk
        // gates Continue until the fifth parks the sender.
        for i in 0..5u8 {
            let gate = on_chunk(id, format!("chunk{i}").as_bytes());
            assert!(matches!(gate, ReadGate::Continue), "chunk {i} should fit");
        }
        // The sixth stalls: the read loop pauses, the chunk overflows to
        // the watcher.
        assert!(matches!(on_chunk(id, b"chunk5"), ReadGate::Pause));

        // A consumer take opens room: the watcher drains the overflow
        // and resumes the read loop.
        let taken = futures_lite::future::block_on(body.next())
            .unwrap()
            .unwrap();
        assert_eq!(taken, Bytes::from("chunk0"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while resumed.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the read loop never resumed"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // The overflowed chunk queued behind the rest; the terminal
        // waits out the data it staged behind.
        on_done(id);
        let rest = futures_lite::future::block_on(body.map(|c| c.unwrap()).collect::<Vec<_>>());
        let rest = rest.concat();
        assert_eq!(
            rest,
            Bytes::from("chunk1chunk2chunk3chunk4chunk5"),
            "the overflowed chunk must arrive before the end-of-stream"
        );
    }
}
