//! Pure-Rust WebSocket transport for monet-core.
//!
//! On Android this is the primary WebSocket (Cronet has none); everywhere
//! else it is the fallback. It runs tokio-tungstenite + rustls on a private
//! single-threaded runtime, so no async runtime leaks into the public API:
//! callers get [`WebSocketTransport`](monet_core::transport::WebSocketTransport)
//! futures and the shared channel-based `WebSocketStream`.
//!
//! # Trust and pinning
//!
//! - Default: the embedded Mozilla CA bundle (`webpki-roots` feature) —
//!   works everywhere, matches nitro-fetch's libwebsockets approach.
//! - Optional: the platform trust store (`platform-verifier` feature) via
//!   rustls-platform-verifier — Android system CAs plus
//!   `network_security_config`, iOS/macOS keychain. **On Android the
//!   embedder must initialize it before any connection**: call
//!   `rustls_platform_verifier::android::init_hosted` (or the JNI variant)
//!   from the app's bootstrap — monet-rn will do this. Until then, keep the
//!   default `webpki-roots`.
//! - Pinned hosts (from [`monet_core::pin::PinConfig`]) additionally require
//!   a matching SPKI hash in the presented chain, with monet-core's walk-up
//!   matching — one implementation shared with the HTTP transports.

#![allow(unsafe_code)] // rustls danger APIs for the custom verifier.

mod spki;
mod transport;
mod verifier;

pub use transport::{MonetWsTransport, WsConfig};
pub use verifier::TlsTrust;

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
