//! Streaming request/response bodies.
//!
//! A [`Body`] is a runtime-agnostic stream of `Bytes` chunks. Transports own
//! the [`BodySender`] half and push chunks as the network delivers them; the
//! consumer polls the [`Body`] as a [`Stream`]. This is the core of the
//! "response head first, body streamed afterwards" contract every transport
//! must honor.
//!
//! Chunks travel over an unbounded channel, so pushing never blocks a
//! transport callback thread; backpressure is a frontend concern (the RN
//! layer paces JS delivery, like nitro-fetch's `onData`/`Promise<bool>`
//! protocol). The channel ends — `poll_next` returns `None` — when the
//! sender is dropped or calls [`BodySender::complete`].

use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_core::Stream;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{Error, Result};

/// The receiving half of a streaming body.
pub struct Body {
    receiver: UnboundedReceiver<Result<Bytes>>,
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

/// Creates a connected body channel. The transport keeps the sender and
/// pushes chunks after delivering the response head.
pub fn channel() -> (Body, BodySender) {
    let (sender, receiver) = unbounded();
    (Body { receiver }, BodySender { sender })
}

impl Body {
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
        Pin::new(&mut self.receiver).poll_next(cx)
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
}
