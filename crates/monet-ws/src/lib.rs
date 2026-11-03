//! Pure-Rust WebSocket transport for monet-core.
//!
//! On Android this is the primary WebSocket (Cronet has none); everywhere
//! else it is the fallback. It runs tokio-tungstenite + rustls on a private
//! single-threaded runtime, so no async runtime leaks into the public API:
//! callers get [`WebSocketTransport`](monet_core::transport::WebSocketTransport)
//! futures and the shared channel-based `WebSocketStream`.
//!
//! TLS trust and pinning (including the Android platform-verifier
//! initialization caveat) live in the shared `monet-tls` crate.

#![allow(unsafe_code)] // rustls danger APIs for the custom verifier.

mod transport;

pub use monet_tls::TlsTrust;
pub use transport::{MonetWsTransport, WsConfig};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use monet_core::ws::{CloseFrame, WsMessage};

    #[test]
    fn message_mapping_round_trips() {
        let cases = vec![
            WsMessage::Text("hello".into()),
            WsMessage::Binary(Bytes::from_static(b"\x01\x02")),
            WsMessage::Ping(Bytes::from_static(b"ping")),
            WsMessage::Pong(Bytes::from_static(b"pong")),
            WsMessage::Close(Some(CloseFrame {
                code: 1000,
                reason: "bye".into(),
            })),
            WsMessage::Close(None),
        ];
        for message in cases {
            let back = to_ws(to_tungstenite(message.clone()));
            assert_eq!(back, message);
        }
    }

    #[test]
    fn close_code_mapping() {
        let tm = to_tungstenite(WsMessage::Close(Some(CloseFrame {
            code: 4400,
            reason: "policy".into(),
        })));
        match tm {
            Tm::Close(Some(frame)) => {
                assert_eq!(u16::from(frame.code), 4400);
                assert_eq!(frame.reason.as_str(), "policy");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    use tokio_tungstenite::tungstenite::Message as Tm;
    use transport::{to_tungstenite, to_ws};
}
