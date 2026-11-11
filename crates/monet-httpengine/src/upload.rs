//! Streaming-upload state (v2/W2d): the Rust half of the chunked
//! `UploadDataProvider`. The feeder — a parked thread, the body
//! watcher's approach — polls the request [`Body`] and pushes chunks
//! into an unbounded queue; the provider's `read()` pulls from the queue
//! through the JNI native, and an empty queue parks the read until the
//! feeder's notify wakes it.
//!
//! The queue has no bound of its own: the producer side (the RN pump's
//! feed channel) is unbounded by the same W2a decision, so a Rust-side
//! cap would only bound the local copy, not the stream's footprint.
//! Cronet's reads are the true pace — whatever the feeder pushed ahead
//! of them sits here.
//!
//! Lifetime: `create` before the request starts (the provider can
//! `read()` the moment `start()` returns), `close` on Cronet's
//! `close()` — the authoritative end — or a failed setup. The feeder
//! races the body against the cancel token and the close channel's end.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};

use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};

use monet_core::body::Body;
use monet_core::cancel::CancelToken;
use monet_core::error::Result;

/// Wakes the Java provider's parked read — the JNI layer supplies this
/// (it knows how to reach the provider object). Armed inside the setup
/// scope, before the request starts.
pub(crate) type NotifyProvider = Arc<dyn Fn() + Send + Sync>;

/// The provider's take outcome; the JNI native maps it onto the int
/// codes `MonetStreamedProvider.drain` switches on.
#[derive(Debug)]
pub(crate) enum Take {
    /// Bytes written into the read buffer.
    Wrote(usize),
    /// The stream ended: the final, empty chunk.
    End,
    /// The stream failed.
    Failed,
    /// Nothing available yet: the read parks until the feeder notifies.
    Empty,
    /// No such upload (closed or unknown id): the read treats it as the
    /// end — a closed upload must not hang Cronet's read loop.
    Gone,
}

enum Status {
    Open,
    Done,
    Failed,
}

struct UploadState {
    queue: Mutex<VecDeque<Bytes>>,
    status: Mutex<Status>,
    /// Fires the provider's `onDataAvailable` (a parked read's wake).
    notify: Option<NotifyProvider>,
    /// Ends the feeder on close; its drop (the state's removal) ends the
    /// stream too — the same shape as the registry's wake channel.
    close_tx: Option<UnboundedSender<()>>,
}

static UPLOADS: LazyLock<Mutex<HashMap<u64, UploadState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Registers one streaming upload; called before the request starts.
/// The close receiver is the feeder's third race arm.
pub(crate) fn create(id: u64) -> UnboundedReceiver<()> {
    let (close_tx, close_rx) = unbounded();
    UPLOADS.lock().unwrap().insert(
        id,
        UploadState {
            queue: Mutex::new(VecDeque::new()),
            status: Mutex::new(Status::Open),
            notify: None,
            close_tx: Some(close_tx),
        },
    );
    close_rx
}

/// Arms the parked-read wake; must happen before `UrlRequest.start()`.
pub(crate) fn arm_notify(id: u64, notify: NotifyProvider) {
    let mut uploads = UPLOADS.lock().unwrap();
    if let Some(state) = uploads.get_mut(&id) {
        state.notify = Some(notify);
    }
}

/// Fires the provider wake; the closure is cloned out of the lock first
/// because it re-enters the JVM.
fn notify(id: u64) {
    let notify = {
        let uploads = UPLOADS.lock().unwrap();
        uploads.get(&id).and_then(|state| state.notify.clone())
    };
    if let Some(notify) = notify {
        notify();
    }
}

/// Pushes one chunk (empty chunks are dropped — a zero-byte take would
/// read as the end on the Java side) and wakes a parked read.
pub(crate) fn push(id: u64, chunk: Bytes) {
    if chunk.is_empty() {
        return;
    }
    {
        let uploads = UPLOADS.lock().unwrap();
        let Some(state) = uploads.get(&id) else {
            return;
        };
        state.queue.lock().unwrap().push_back(chunk);
    }
    notify(id);
}

/// The stream ended: the parked (or next) read sees the final chunk.
pub(crate) fn finish(id: u64) {
    {
        let uploads = UPLOADS.lock().unwrap();
        let Some(state) = uploads.get(&id) else {
            return;
        };
        *state.status.lock().unwrap() = Status::Done;
    }
    notify(id);
}

/// The stream failed: the read errors out (the producer's own error is
/// its side of the story — the fetch fails with Cronet's wrapping).
pub(crate) fn fail(id: u64) {
    {
        let uploads = UPLOADS.lock().unwrap();
        let Some(state) = uploads.get(&id) else {
            return;
        };
        *state.status.lock().unwrap() = Status::Failed;
    }
    notify(id);
}

/// The provider's take: fills `out` from the queue's front — a chunk
/// larger than the buffer leaves its remainder for the next read.
pub(crate) fn take_chunk(id: u64, out: &mut [u8]) -> Take {
    let uploads = UPLOADS.lock().unwrap();
    let Some(state) = uploads.get(&id) else {
        return Take::Gone;
    };
    let mut queue = state.queue.lock().unwrap();
    if let Some(chunk) = queue.front_mut() {
        let count = chunk.len().min(out.len());
        out[..count].copy_from_slice(&chunk[..count]);
        if count == chunk.len() {
            queue.pop_front();
        } else {
            *chunk = chunk.slice(count..);
        }
        return Take::Wrote(count);
    }
    match *state.status.lock().unwrap() {
        Status::Open => Take::Empty,
        Status::Done => Take::End,
        Status::Failed => Take::Failed,
    }
}

/// Ends the upload: stops the feeder (the close channel fires, then
/// ends with the state's removal) and frees the queue. Cronet's
/// `close()` is the authoritative caller; a failed setup calls it too.
pub(crate) fn close(id: u64) {
    if let Some(state) = UPLOADS.lock().unwrap().remove(&id)
        && let Some(close_tx) = state.close_tx
    {
        let _ = close_tx.unbounded_send(());
    }
}

/// The feeder (the body watcher's parked-thread approach — no runtime
/// here): polls the request [`Body`] into the queue until the body ends
/// or errors, the request is cancelled, or the upload closes.
/// `drained` is what [`Body::take_complete`] already pulled — it leads
/// the queue.
pub(crate) fn spawn_feeder(
    id: u64,
    mut body: Body,
    cancel: CancelToken,
    mut close_rx: UnboundedReceiver<()>,
    drained: Vec<u8>,
) {
    let spawned = std::thread::Builder::new()
        .name("monet-httpengine-upload".into())
        .spawn(move || {
            if !drained.is_empty() {
                push(id, Bytes::from(drained));
            }
            futures_lite::future::block_on(async {
                use futures_lite::StreamExt;
                loop {
                    enum Step {
                        Body(Option<Result<Bytes>>),
                        Cancelled,
                        Closed,
                    }
                    let step = futures_lite::future::race(
                        async { Step::Body(body.next().await) },
                        futures_lite::future::race(
                            async {
                                cancel.cancelled().await;
                                Step::Cancelled
                            },
                            async {
                                close_rx.next().await;
                                Step::Closed
                            },
                        ),
                    )
                    .await;
                    match step {
                        Step::Body(Some(Ok(bytes))) => push(id, bytes),
                        Step::Body(Some(Err(_))) => {
                            fail(id);
                            return;
                        }
                        Step::Body(None) => {
                            finish(id);
                            return;
                        }
                        // Cancelled: the native cancel's onCanceled closes
                        // the provider, which ends this upload's state —
                        // this side just stops. Closed: it already has.
                        Step::Cancelled | Step::Closed => return,
                    }
                }
            });
        });
    if spawned.is_err() {
        // Thread-starved: fail the upload rather than park Cronet's read
        // forever (a parked read wakes only on the feeder's notify).
        fail(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::StreamExt;

    fn wrote(out: &mut [u8]) -> Take {
        take_chunk(1, out)
    }

    #[test]
    fn chunks_flow_in_order_and_split_across_reads() {
        let close_rx = create(1);
        std::mem::forget(close_rx); // the state stays for the takes below
        push(1, Bytes::from_static(b"alpha"));
        push(1, Bytes::from_static(b"beta"));

        let mut out = [0u8; 3];
        assert!(matches!(wrote(&mut out), Take::Wrote(3)));
        assert_eq!(&out, b"alp");
        // The chunk's tail (shorter than the buffer) leads the next
        // read, then "beta" takes its turn.
        assert!(matches!(wrote(&mut out), Take::Wrote(2)));
        assert_eq!(&out[..2], b"ha");
        assert!(matches!(wrote(&mut out), Take::Wrote(3)));
        assert_eq!(&out, b"bet");
        assert!(matches!(wrote(&mut out), Take::Wrote(1)));
        assert_eq!(&out[..1], b"a");
        assert!(matches!(wrote(&mut out), Take::Empty));
    }

    #[test]
    fn empty_chunks_are_dropped() {
        let close_rx = create(2);
        std::mem::forget(close_rx);
        push(2, Bytes::new());
        assert!(matches!(take_chunk(2, &mut []), Take::Empty));
        finish(2);
        assert!(matches!(take_chunk(2, &mut [0u8; 8]), Take::End));
    }

    #[test]
    fn failed_stream_errors_the_read() {
        let close_rx = create(3);
        std::mem::forget(close_rx);
        fail(3);
        assert!(matches!(take_chunk(3, &mut [0u8; 8]), Take::Failed));
    }

    #[test]
    fn close_ends_the_upload() {
        let close_rx = create(4);
        push(4, Bytes::from_static(b"leftover"));
        close(4);
        assert!(matches!(take_chunk(4, &mut [0u8; 8]), Take::Gone));
        // The feeder's close arm: the wake fires, then the channel ends
        // with the state.
        let mut close_rx = close_rx;
        assert_eq!(
            futures_lite::future::block_on(close_rx.next()),
            Some(()),
            "close must wake a parked feeder"
        );
        assert_eq!(
            futures_lite::future::block_on(close_rx.next()),
            None,
            "the close channel must end when the state is removed"
        );
    }

    #[test]
    fn notify_fires_on_push_and_terminals() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let close_rx = create(5);
        std::mem::forget(close_rx);
        let calls = Arc::new(AtomicUsize::new(0));
        let probe = calls.clone();
        arm_notify(
            5,
            Arc::new(move || {
                probe.fetch_add(1, Ordering::SeqCst);
            }),
        );
        push(5, Bytes::from_static(b"x"));
        finish(5);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn feeder_streams_a_live_body_then_finishes() {
        let close_rx = create(6);
        let (body, sender) = monet_core::body::channel();
        sender.send_chunk("first").unwrap();
        spawn_feeder(
            6,
            body,
            CancelToken::new(),
            close_rx,
            b"drained-prefix".to_vec(),
        );
        sender.send_chunk("second").unwrap();
        sender.complete();

        // The queue: drained prefix, then both chunks, then the end.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut seen = Vec::new();
        loop {
            let mut out = [0u8; 64];
            match take_chunk(6, &mut out) {
                Take::Wrote(count) => seen.extend_from_slice(&out[..count]),
                Take::End => break,
                Take::Empty => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the feeder never finished the upload"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                other => panic!("unexpected take: {other:?}"),
            }
        }
        assert_eq!(seen, b"drained-prefixfirstsecond".to_vec());
    }

    #[test]
    fn feeder_fails_a_errored_body() {
        let close_rx = create(7);
        let (body, sender) = monet_core::body::channel();
        sender.abort(monet_core::error::Error::Body("producer broke".into()));
        spawn_feeder(7, body, CancelToken::new(), close_rx, Vec::new());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !matches!(take_chunk(7, &mut [0u8; 8]), Take::Failed) {
            assert!(
                std::time::Instant::now() < deadline,
                "the feeder never reported the failure"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
