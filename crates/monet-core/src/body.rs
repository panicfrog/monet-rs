//! Streaming request/response bodies.
//!
//! A [`Body`] is a runtime-agnostic stream of `Bytes` chunks. Transports own
//! the sender half and push chunks as the network delivers them; the
//! consumer polls the [`Body`] as a [`Stream`]. This is the core of the
//! "response head first, body streamed afterwards" contract every transport
//! must honor.
//!
//! Two channel flavors (v2/W2b):
//!
//! - [`channel()`] is unbounded, so pushing never blocks a transport
//!   callback thread — the shape request bodies and sync feeders need
//!   (the RN layer paces upload producers itself).
//! - [`bounded()`] caps the queue: an async feeder awaiting [`BoundedBodySender::send`]
//!   pauses at capacity, which is how a slow consumer propagates to
//!   network reads (the response-side contract — a consumer that stops
//!   polling must stop the download, not buffer the whole body). Sync
//!   feeders probe with [`BoundedBodySender::try_send`] and arrange
//!   their own resume (a stalled read loop, a suspended task).
//!
//! The channel ends — `poll_next` returns `None` — when the sender is
//! dropped or calls `complete`.

use bytes::Bytes;
use futures_channel::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, unbounded};
use futures_core::Stream;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::error::{Error, Result};

/// The receiving half of a streaming body.
pub struct Body {
    receiver: BodyReceiver,
}

enum BodyReceiver {
    Unbounded(UnboundedReceiver<Result<Bytes>>),
    Bounded {
        receiver: Receiver<Result<Bytes>>,
        /// The abort slot of a bounded pair: `abort` cannot queue behind
        /// a full buffer (an error must not wait out the data), so it
        /// lands here and surfaces at end-of-stream.
        aborted: Arc<Mutex<Option<Error>>>,
    },
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body").finish_non_exhaustive()
    }
}

/// The sending half, held by a transport while the body downloads.
pub struct BodySender {
    sender: UnboundedSender<Result<Bytes>>,
}

/// The sending half of a bounded channel (response bodies). Cheap to
/// clone: clones share one channel (and one sender handle), and
/// `complete`/`abort` close it for all of them — a drain loop can hold
/// a clone while the registry owns another.
#[derive(Clone)]
pub struct BoundedBodySender {
    /// `poll_ready`/`try_send` take `&mut self` on futures-channel's
    /// sender; the registry/callback callers reach this through shared
    /// locks. One handle behind an Arc, so every clone parks and unparks
    /// the same sender.
    sender: Arc<Mutex<Sender<Result<Bytes>>>>,
    aborted: Arc<Mutex<Option<Error>>>,
}

/// Creates a connected body channel. The transport keeps the sender and
/// pushes chunks after delivering the response head.
pub fn channel() -> (Body, BodySender) {
    let (sender, receiver) = unbounded();
    (
        Body {
            receiver: BodyReceiver::Unbounded(receiver),
        },
        BodySender { sender },
    )
}

/// [`channel()`] with a capacity — the response-side flavor. An async
/// feeder awaiting [`BoundedBodySender::send`] pauses when the queue is
/// full; a sync feeder probes with [`BoundedBodySender::try_send`] and
/// keeps the chunk on [`Delivery::Full`]. The effective capacity is
/// `cap + 1` (futures-channel's semantics: the send that crosses the
/// buffer still lands, then parks the sender).
pub fn bounded(cap: usize) -> (Body, BoundedBodySender) {
    let (sender, receiver) = futures_channel::mpsc::channel(cap);
    let aborted = Arc::new(Mutex::new(None));
    (
        Body {
            receiver: BodyReceiver::Bounded {
                receiver,
                aborted: aborted.clone(),
            },
        },
        BoundedBodySender {
            sender: Arc::new(Mutex::new(sender)),
            aborted,
        },
    )
}

/// The outcome of [`Body::take_complete`] — how "finished" a body is
/// without parking on its producer.
#[derive(Debug)]
pub enum Completeness {
    /// The stream was already complete; holds every byte of it.
    Complete(Vec<u8>),
    /// The producer is still live. The caller re-feeds the bytes that
    /// had already landed (a channel has no peek — the probe consumed
    /// them) ahead of the stream.
    Live {
        /// The bytes the probe pulled before the producer's liveness
        /// showed; the caller prepends them to the incremental feed.
        drained: Vec<u8>,
    },
    /// The stream already failed; the terminal error.
    Failed(Error),
}

impl Body {
    /// Drains a body that is already complete, without parking on its
    /// producer. Transports use this to pick the wire shape: a complete
    /// body is rewindable and carries a length, while a live one must
    /// stream — and a failure that already arrived fails the request
    /// outright instead of stalling an upload that can never start.
    pub fn take_complete(&mut self) -> Completeness {
        // A noop waker: a Pending poll must not register a wake the real
        // consumer would rely on — the channel re-registers on the next
        // (real) poll, so the probe leaves no residue.
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut drained = Vec::new();
        loop {
            // Body is Unpin (channel receivers and shared slots only).
            match Pin::new(&mut *self).poll_next(&mut cx) {
                Poll::Ready(Some(Ok(bytes))) => drained.extend_from_slice(&bytes),
                Poll::Ready(Some(Err(error))) => return Completeness::Failed(error),
                Poll::Ready(None) => return Completeness::Complete(drained),
                Poll::Pending => return Completeness::Live { drained },
            }
        }
    }

    /// A body of one chunk, already complete.
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Body {
        let (body, sender) = channel();
        sender
            .send_chunk(bytes)
            .expect("fresh channel accepts a chunk");
        sender.complete();
        body
    }

    /// A body of several chunks, already complete.
    pub fn from_chunks(chunks: impl IntoIterator<Item = impl Into<Bytes>>) -> Body {
        let (body, sender) = channel();
        for chunk in chunks {
            sender
                .send_chunk(chunk)
                .expect("fresh channel accepts a chunk");
        }
        sender.complete();
        body
    }

    /// An empty body.
    pub fn empty() -> Body {
        let (body, sender) = channel();
        sender.complete();
        body
    }
}

impl Stream for Body {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.receiver {
            BodyReceiver::Unbounded(receiver) => Pin::new(receiver).poll_next(cx),
            BodyReceiver::Bounded { receiver, aborted } => {
                match Pin::new(receiver).poll_next(cx) {
                    // End of stream: a bounded abort could not queue behind
                    // a full buffer, so its error surfaces here — once,
                    // then the stream ends.
                    Poll::Ready(None) => {
                        let error = aborted.lock().unwrap().take();
                        match error {
                            Some(error) => Poll::Ready(Some(Err(error))),
                            None => Poll::Ready(None),
                        }
                    }
                    poll => poll,
                }
            }
        }
    }
}

impl BodySender {
    /// Pushes one chunk. Fails only if the channel is closed (receiver gone,
    /// or the stream was already aborted).
    pub fn send_chunk(&self, chunk: impl Into<Bytes>) -> Result<()> {
        self.sender
            .unbounded_send(Ok(chunk.into()))
            .map_err(|_| Error::Body("body channel closed".into()))
    }

    /// Ends the stream successfully. Equivalent to dropping the sender.
    pub fn complete(self) {
        drop(self);
    }

    /// Ends the stream with an error, which the consumer's next poll sees,
    /// then closes the channel: no chunks can follow an abort. Fails
    /// silently if the receiver is already gone.
    pub fn abort(&self, error: Error) {
        let _ = self.sender.unbounded_send(Err(error));
        self.sender.close_channel();
    }
}

impl BoundedBodySender {
    /// Pushes one chunk, waiting for room at capacity — the async
    /// feeder's natural pause point (a transport pump awaiting this
    /// stops reading its network stream, and the flow control
    /// downstream follows). The park/wake is futures-channel's own:
    /// the receiver unparks a sender on every take. Fails if the
    /// channel is closed.
    pub async fn send(&self, chunk: impl Into<Bytes>) -> Result<()> {
        let chunk = chunk.into();
        std::future::poll_fn(|cx| {
            let mut sender = self.sender.lock().unwrap();
            match sender.poll_ready(cx) {
                Poll::Ready(Ok(())) => match sender.start_send(Ok(chunk.clone())) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(_) => Poll::Ready(Err(Error::Body("body channel closed".into()))),
                },
                Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Body("body channel closed".into()))),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }

    /// The sync feeder's probe: [`Delivery::Full`] means the chunk was
    /// NOT queued — the caller keeps it and arranges its own resume
    /// (a stalled read loop, a suspended task), because the consumer
    /// will only make room at its own pace.
    pub fn try_send(&self, chunk: Bytes) -> Delivery {
        match self.sender.lock().unwrap().try_send(Ok(chunk)) {
            Ok(()) => Delivery::Sent,
            Err(error) if error.is_disconnected() => Delivery::Closed,
            Err(error) => match error.into_inner() {
                Ok(chunk) => Delivery::Full(chunk),
                Err(_) => Delivery::Closed,
            },
        }
    }

    /// Ends the stream successfully: the buffered chunks drain, then
    /// the consumer sees end-of-stream. Close-based, so it holds for
    /// every clone of the sender (a drain loop may hold one).
    pub fn complete(&self) {
        self.sender.lock().unwrap().close_channel();
    }

    /// Ends the stream with an error. The error lands in the abort slot
    /// (it cannot queue behind a full buffer) and surfaces after the
    /// buffered chunks drain.
    pub fn abort(&self, error: Error) {
        *self.aborted.lock().unwrap() = Some(error);
        self.sender.lock().unwrap().close_channel();
    }
}

/// The outcome of a sync [`BoundedBodySender::try_send`] probe.
#[derive(Debug)]
pub enum Delivery {
    /// The chunk is queued.
    Sent,
    /// The channel is at capacity; the chunk is handed back — hold it
    /// and resume when the consumer has drained.
    Full(Bytes),
    /// The receiver is gone: stop the request (the v1 semantic of a
    /// failed `send_chunk` — the consumer dropped the body).
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::StreamExt;

    #[test]
    fn chunks_arrive_in_order() {
        let (body, sender) = channel();
        sender.send_chunk("a").unwrap();
        sender.send_chunk("b").unwrap();
        sender.complete();
        let out = futures_lite::future::block_on(body.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(out, ["a", "b"]);
    }

    #[test]
    fn empty_body_yields_nothing() {
        let body = Body::empty();
        let out = futures_lite::future::block_on(body.map(|c| c.unwrap()).collect::<Vec<Bytes>>());
        assert!(out.is_empty());
    }

    #[test]
    fn abort_surfaces_error_then_terminates() {
        let (body, sender) = channel();
        sender.send_chunk("ok").unwrap();
        sender.abort(Error::Body("broken".into()));

        let mut body = body;
        let first = futures_lite::future::block_on(body.next())
            .unwrap()
            .unwrap();
        assert_eq!(first, Bytes::from("ok"));
        let second = futures_lite::future::block_on(body.next()).unwrap();
        assert!(matches!(second, Err(Error::Body(m)) if m.contains("broken")));
        // abort closed the channel: the stream ends after the error, no hang
        assert!(futures_lite::future::block_on(body.next()).is_none());
        // and further sends are rejected
        assert!(sender.send_chunk("late").is_err());
    }

    #[test]
    fn from_bytes_and_from_chunks() {
        let one = Body::from_bytes("hello");
        let out = futures_lite::future::block_on(one.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(out, ["hello"]);

        let many = Body::from_chunks(["a", "b", "c"]);
        let out = futures_lite::future::block_on(many.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(out, ["a", "b", "c"]);
    }

    #[test]
    fn sender_reports_dropped_receiver() {
        let (body, sender) = channel();
        drop(body);
        assert!(matches!(sender.send_chunk("x"), Err(Error::Body(_))));
    }

    #[test]
    fn bounded_try_send_signals_capacity() {
        // futures-channel's capacity is cap + 1: the send that crosses
        // the buffer still lands, then parks the sender.
        let (mut body, sender) = bounded(2);
        assert!(matches!(sender.try_send(Bytes::from("a")), Delivery::Sent));
        assert!(matches!(sender.try_send(Bytes::from("b")), Delivery::Sent));
        assert!(matches!(sender.try_send(Bytes::from("c")), Delivery::Sent));
        // Parked: the chunk comes back — the caller holds it.
        match sender.try_send(Bytes::from("d")) {
            Delivery::Full(chunk) => assert_eq!(chunk, Bytes::from("d")),
            other => panic!("expected Full, got {other:?}"),
        }
        // One receive unparks exactly one send.
        let first = futures_lite::future::block_on(body.next())
            .unwrap()
            .unwrap();
        assert_eq!(first, Bytes::from("a"));
        assert!(matches!(sender.try_send(Bytes::from("d")), Delivery::Sent));
        drop(sender);
        let rest = futures_lite::future::block_on(body.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(rest, ["b", "c", "d"]);
    }

    #[test]
    fn bounded_send_awaits_capacity() {
        let (body, sender) = bounded(1);
        // Fill to the effective capacity (cap + 1 = 2).
        sender.try_send(Bytes::from("a"));
        sender.try_send(Bytes::from("b"));
        let sending = std::thread::spawn(move || {
            // Blocks until the receiver makes room, then completes the
            // stream — the async feeder's pause point.
            futures_lite::future::block_on(async {
                sender.send(Bytes::from("c")).await.unwrap();
                sender.complete();
            });
        });
        // Give the sender time to park on the full channel.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let out = futures_lite::future::block_on(body.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(out, ["a", "b", "c"]);
        sending.join().unwrap();
    }

    #[test]
    fn bounded_abort_surfaces_after_buffered_data() {
        let (body, sender) = bounded(1);
        sender.try_send(Bytes::from("a"));
        sender.try_send(Bytes::from("b"));
        // Full buffer: the abort cannot queue — it must still surface,
        // behind the buffered chunks, not be lost to a clean EOF.
        sender.abort(Error::Body("late failure".into()));

        let mut body = body;
        let drained = futures_lite::future::block_on(body.next())
            .unwrap()
            .unwrap();
        assert_eq!(drained, Bytes::from("a"));
        let second = futures_lite::future::block_on(body.next())
            .unwrap()
            .unwrap();
        assert_eq!(second, Bytes::from("b"));
        let terminal = futures_lite::future::block_on(body.next()).unwrap();
        assert!(
            matches!(&terminal, Err(Error::Body(m)) if m.contains("late failure")),
            "got: {terminal:?}"
        );
        assert!(futures_lite::future::block_on(body.next()).is_none());
    }

    #[test]
    fn bounded_reports_dropped_receiver() {
        let (body, sender) = bounded(1);
        drop(body);
        assert!(matches!(
            sender.try_send(Bytes::from("x")),
            Delivery::Closed
        ));
        let send = futures_lite::future::block_on(sender.send(Bytes::from("y")));
        assert!(send.is_err());
    }

    #[test]
    fn take_complete_drains_a_settled_body() {
        let mut body = Body::from_chunks(["a", "b"]);
        match body.take_complete() {
            Completeness::Complete(bytes) => assert_eq!(bytes, b"ab".to_vec()),
            other => panic!("expected Complete, got {other:?}"),
        }
        let mut empty = Body::empty();
        match empty.take_complete() {
            Completeness::Complete(bytes) => assert!(bytes.is_empty()),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn take_complete_hands_back_live_drained_bytes() {
        let (mut body, sender) = channel();
        // Queued before the probe, but the producer is still live: the
        // probe must return the bytes it consumed, not lose them.
        sender.send_chunk("early").unwrap();
        match body.take_complete() {
            Completeness::Live { drained } => assert_eq!(drained, b"early".to_vec()),
            other => panic!("expected Live, got {other:?}"),
        }
        sender.send_chunk("late").unwrap();
        drop(sender);
        let rest = futures_lite::future::block_on(body.map(|c| c.unwrap()).collect::<Vec<Bytes>>());
        assert_eq!(rest, [Bytes::from("late")]);
    }

    #[test]
    fn take_complete_surfaces_an_already_failed_stream() {
        let (mut body, sender) = channel();
        sender.send_chunk("x").unwrap();
        sender.abort(Error::Body("producer died".into()));
        match body.take_complete() {
            Completeness::Failed(error) => {
                assert!(matches!(error, Error::Body(m) if m.contains("producer died")));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }
}
