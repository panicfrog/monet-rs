//! WebSocket message semantics and the channel-based stream shared by every
//! frontend.
//!
//! [`WebSocketStream`] is the consumer-facing half; [`WebSocketDriver`] is
//! what a transport drives: it reads messages the user sends and pushes
//! messages the network receives. Native transports (the iOS Swift shim) and
//! Rust transports (tokio-tungstenite) both pump these channels, so the
//! frontend shape is identical everywhere.

use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};

use crate::error::{Error, Result};

/// A close frame's code and reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseFrame {
    /// WebSocket close status code (e.g. 1000 for normal closure).
    pub code: u16,
    /// Human-readable reason.
    pub reason: String,
}

/// One WebSocket frame, in the granularity frontends consume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsMessage {
    /// A UTF-8 text message.
    Text(String),
    /// A binary message.
    Binary(Bytes),
    /// A ping (transport usually answers automatically; surfaced for tests
    /// and custom protocols).
    Ping(Bytes),
    /// A pong.
    Pong(Bytes),
    /// A close frame. `None` payload means abnormal closure without a code.
    Close(Option<CloseFrame>),
}

/// The frontend-facing half of an open WebSocket.
pub struct WebSocketStream {
    incoming: UnboundedReceiver<WsMessage>,
    outgoing: UnboundedSender<WsMessage>,
}

/// The transport-facing half of an open WebSocket.
pub struct WebSocketDriver {
    /// Messages the user has sent (the transport writes them to the network).
    pub user_to_network: UnboundedReceiver<WsMessage>,
    /// The transport pushes received messages here.
    pub network_to_user: UnboundedSender<WsMessage>,
}

/// Creates a connected socket pair.
pub fn channel() -> (WebSocketStream, WebSocketDriver) {
    let (to_network_tx, to_network_rx) = unbounded();
    let (to_user_tx, to_user_rx) = unbounded();
    (
        WebSocketStream {
            incoming: to_user_rx,
            outgoing: to_network_tx,
        },
        WebSocketDriver {
            user_to_network: to_network_rx,
            network_to_user: to_user_tx,
        },
    )
}

impl WebSocketStream {
    /// Waits for the next received message.
    ///
    /// `Ok(None)` means the transport closed the socket (a `Close` frame is
    /// delivered as a message first when the peer sent one).
    pub async fn next_message(&mut self) -> Result<Option<WsMessage>> {
        Ok(std::future::poll_fn(|cx| {
            use futures_core::Stream;
            std::pin::Pin::new(&mut self.incoming).poll_next(cx)
        })
        .await)
    }

    /// Sends a message. Unbounded, so this never blocks; flow control is the
    /// transport's concern.
    pub fn send(&mut self, message: WsMessage) -> Result<()> {
        self.outgoing
            .unbounded_send(message)
            .map_err(|_| Error::Body("websocket driver dropped".into()))
    }

    /// Sends a close frame and finishes the sending side.
    pub fn close(&mut self, code: u16, reason: impl Into<String>) -> Result<()> {
        let result = self.send(WsMessage::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })));
        self.outgoing.close_channel();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::StreamExt as _;

    #[test]
    fn messages_round_trip_both_directions() {
        let (mut stream, mut driver) = channel();
        driver
            .network_to_user
            .unbounded_send(WsMessage::Text("hi".into()))
            .unwrap();
        stream
            .send(WsMessage::Binary(Bytes::from_static(b"\x01")))
            .unwrap();

        let received = futures_lite::future::block_on(stream.next_message()).unwrap();
        assert_eq!(received, Some(WsMessage::Text("hi".into())));
        let echoed = futures_lite::future::block_on(driver.user_to_network.next());
        assert_eq!(echoed, Some(WsMessage::Binary(Bytes::from_static(b"\x01"))));
    }

    #[test]
    fn driver_close_terminates_stream() {
        let (mut stream, driver) = channel();
        driver
            .network_to_user
            .unbounded_send(WsMessage::Close(Some(CloseFrame {
                code: 1000,
                reason: "bye".into(),
            })))
            .unwrap();
        drop(driver);
        let first = futures_lite::future::block_on(stream.next_message()).unwrap();
        assert!(matches!(first, Some(WsMessage::Close(Some(f))) if f.code == 1000));
        let end = futures_lite::future::block_on(stream.next_message()).unwrap();
        assert_eq!(end, None);
    }

    #[test]
    fn stream_close_reaches_driver() {
        let (mut stream, mut driver) = channel();
        stream.close(1000, "done").unwrap();
        let frame = futures_lite::future::block_on(driver.user_to_network.next());
        assert!(matches!(frame, Some(WsMessage::Close(Some(f))) if f.reason == "done"));
        // channel closed after close frame
        assert!(futures_lite::future::block_on(driver.user_to_network.next()).is_none());
    }
}
