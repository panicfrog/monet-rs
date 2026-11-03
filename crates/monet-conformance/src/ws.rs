//! The WebSocket conformance battery: a script-driven tokio-tungstenite
//! server and one check function per contract behavior. The close-code
//! checks are the reason the iOS transport hand-rolls frames —
//! URLSessionWebSocketTask maps 1012-1014 to 1005 and drops the reason,
//! which would fail this battery.

use std::sync::mpsc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use monet_core::error::Error;
use monet_core::method::Method;
use monet_core::request::Request;
use monet_core::transport::WebSocketTransport;
use monet_core::ws::{WebSocketStream, WsMessage};
use monet_core::{CancelToken, Url};
use tokio_tungstenite::tungstenite::Message;

use crate::wait_for;

/// What the server does once connected.
#[derive(Clone)]
enum Script {
    /// Echo every text/binary message back.
    Echo,
    /// Send a close frame with this code/reason right away.
    ServerClose(u16, &'static str),
    /// Send a ping, wait for the client's pong, then report it.
    PingThenReport,
}

/// What the server observed.
#[derive(Debug)]
pub enum Seen {
    /// The handshake completed.
    Connected,
    /// The close frame the client sent (code, reason).
    Closed(u16, String),
    /// The client answered the ping with a pong.
    Ponged,
}

/// Spawns a server accepting `connections` sockets on a loopback port,
/// returning its URL and the queue of what it observed.
fn spawn_server(script: Script, connections: usize) -> (Url, mpsc::Receiver<Seen>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (seen_tx, seen_rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("monet-conformance-ws".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                for _ in 0..connections {
                    let (stream, _) = match listener.accept().await {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let script = script.clone();
                    let seen_tx = seen_tx.clone();
                    let served = tokio::spawn(async move {
                        // Negotiate the subprotocol like a well-behaved
                        // server: strict clients (tungstenite) fail the
                        // handshake if a protocol was requested but none
                        // is selected in the response.
                        // (result_large_err: the error type is
                        // tungstenite's callback signature, never built.)
                        #[allow(clippy::result_large_err)]
                        let mut ws = tokio_tungstenite::accept_hdr_async(
                            stream,
                            |request: &tokio_tungstenite::tungstenite::http::Request<()>,
                             mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                                let offered = request
                                    .headers()
                                    .get("sec-websocket-protocol")
                                    .and_then(|value| value.to_str().ok())
                                    .unwrap_or_default();
                                if offered.split(',').any(|token| token.trim() == "chat") {
                                    response.headers_mut().insert(
                                        "sec-websocket-protocol",
                                        "chat".parse().unwrap(),
                                    );
                                }
                                Ok(response)
                            },
                        )
                        .await
                        .unwrap();
                        let _ = seen_tx.send(Seen::Connected);
                        match script {
                            Script::Echo => {
                                while let Some(Ok(message)) = ws.next().await {
                                    match message {
                                        Message::Text(text) => {
                                            ws.send(Message::Text(text)).await.unwrap();
                                        }
                                        Message::Binary(bytes) => {
                                            ws.send(Message::Binary(bytes)).await.unwrap();
                                        }
                                        Message::Close(frame) => {
                                            let (code, reason) = frame
                                                .map(|f| {
                                                    (u16::from(f.code), f.reason.to_string())
                                                })
                                                .unwrap_or((1005, String::new()));
                                            let _ = seen_tx.send(Seen::Closed(code, reason));
                                            let _ = ws.close(None).await;
                                            return;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            Script::ServerClose(code, reason) => {
                                use tokio_tungstenite::tungstenite::protocol::CloseFrame;
                                use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
                                ws.send(Message::Close(Some(CloseFrame {
                                    code: CloseCode::from(code),
                                    reason: reason.into(),
                                })))
                                .await
                                .unwrap();
                            }
                            Script::PingThenReport => {
                                ws.send(Message::Ping(bytes::Bytes::from_static(b"probe")))
                                    .await
                                    .unwrap();
                                while let Some(Ok(message)) = ws.next().await {
                                    if let Message::Pong(_) = message {
                                        let _ = seen_tx.send(Seen::Ponged);
                                        let _ = ws.close(None).await;
                                        return;
                                    }
                                }
                            }
                        }
                    });
                    let _ = served.await;
                }
            });
        })
        .unwrap();
    let addr = addr_rx.recv().unwrap();
    (Url::parse(&format!("ws://{addr}/socket")).unwrap(), seen_rx)
}

/// Drains the handshake notification before an observation check reads
/// its own event.
fn seen_connected(seen: &mpsc::Receiver<Seen>) {
    assert!(matches!(
        seen.recv_timeout(Duration::from_secs(5)),
        Ok(Seen::Connected)
    ));
}

/// Connects with the protocol header every transport must carry.
fn connect(t: &dyn WebSocketTransport, url: &Url) -> WebSocketStream {
    let request = Request::new(Method::Get, url.clone())
        .with_header("sec-websocket-protocol", "chat, superchat")
        .unwrap();
    wait_for("websocket open", t.connect(request, CancelToken::new())).unwrap()
}

/// Text and binary messages round-trip through the echo server.
pub fn text_and_binary_round_trip(t: &dyn WebSocketTransport) {
    let (url, _seen) = spawn_server(Script::Echo, 1);
    let mut stream = connect(t, &url);

    stream.send(WsMessage::Text("hello".into())).unwrap();
    let echoed = wait_for("text echo", stream.next_message()).unwrap();
    assert_eq!(echoed, Some(WsMessage::Text("hello".into())));

    stream
        .send(WsMessage::Binary(bytes::Bytes::from_static(
            b"\x00\x01\x02",
        )))
        .unwrap();
    let echoed = wait_for("binary echo", stream.next_message()).unwrap();
    assert_eq!(
        echoed,
        Some(WsMessage::Binary(bytes::Bytes::from_static(
            b"\x00\x01\x02"
        )))
    );
}

/// The server closes with 1012 and a reason: the client must see exactly
/// that code and reason, then the stream must end. (1005/1006/1012-1014
/// fidelity is the contract's sharpest edge.)
pub fn server_close_code_and_reason_round_trip_exactly(t: &dyn WebSocketTransport) {
    let (url, _seen) = spawn_server(Script::ServerClose(1012, "service restart"), 1);
    let mut stream = connect(t, &url);

    let closed = wait_for("close frame", stream.next_message()).unwrap();
    match closed {
        Some(WsMessage::Close(Some(frame))) => {
            assert_eq!(frame.code, 1012);
            assert_eq!(frame.reason, "service restart");
        }
        other => panic!("expected a close frame with code 1012, got: {other:?}"),
    }
    let end = wait_for("stream end", stream.next_message()).unwrap();
    assert_eq!(end, None);
}

/// The client's close code and reason reach the server.
pub fn client_close_reaches_server(t: &dyn WebSocketTransport) {
    let (url, seen) = spawn_server(Script::Echo, 1);
    let mut stream = connect(t, &url);
    seen_connected(&seen);
    stream.close(4321, "client done").unwrap();

    match seen.recv_timeout(Duration::from_secs(5)).unwrap() {
        Seen::Closed(code, reason) => {
            assert_eq!(code, 4321);
            assert_eq!(reason, "client done");
        }
        other => panic!("expected the server to see the close, got: {other:?}"),
    }
}

/// A received ping is surfaced to the user and answered automatically.
pub fn ping_surfaced_and_auto_ponged(t: &dyn WebSocketTransport) {
    let (url, seen) = spawn_server(Script::PingThenReport, 1);
    let mut stream = connect(t, &url);
    seen_connected(&seen);

    let ping = wait_for("ping message", stream.next_message()).unwrap();
    assert_eq!(
        ping,
        Some(WsMessage::Ping(bytes::Bytes::from_static(b"probe")))
    );
    match seen.recv_timeout(Duration::from_secs(5)).unwrap() {
        Seen::Ponged => {}
        other => panic!("expected the server to see the auto-pong, got: {other:?}"),
    }
}

/// A token cancelled before `connect` fails fast.
pub fn pre_cancelled_fails_fast(t: &dyn WebSocketTransport) {
    let (url, _seen) = spawn_server(Script::Echo, 1);
    let cancel = CancelToken::new();
    cancel.cancel();
    let request = Request::new(Method::Get, url);
    let err = wait_for("pre-cancelled outcome", t.connect(request, cancel)).unwrap_err();
    assert!(matches!(err, Error::Cancelled), "got: {err:?}");
}

/// A refused connection maps to a transport error.
pub fn connection_refused_maps_to_transport_error(t: &dyn WebSocketTransport) {
    let url = Url::parse("ws://127.0.0.1:1/socket").unwrap();
    let request = Request::new(Method::Get, url);
    let err = wait_for("refused outcome", t.connect(request, CancelToken::new())).unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

/// The core battery: round-trips, close fidelity, cancellation, and the
/// refused-connection mapping. The two server-reporting checks
/// (`client_close_reaches_server`, `ping_surfaced_and_auto_ponged`)
/// spawn their own servers and are called separately.
pub fn run_suite(t: &dyn WebSocketTransport) {
    text_and_binary_round_trip(t);
    server_close_code_and_reason_round_trip_exactly(t);
    pre_cancelled_fails_fast(t);
    connection_refused_maps_to_transport_error(t);
}
