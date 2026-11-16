//! Integration tests against a local tokio-tungstenite server, driven
//! through the real NWConnection shim. The close-code cases are the ones
//! that justified hand-rolled frames: URLSessionWebSocketTask maps
//! 1012-1014 to 1005 and drops the reason, and these tests pin that
//! monet-core's CloseFrame round-trips exactly.

use std::future::Future;
use std::sync::mpsc;
use std::time::Duration;

use monet_core::error::Error;
use monet_core::method::Method;
use monet_core::request::Request;
use monet_core::transport::WebSocketTransport as _;
use monet_core::ws::WsMessage;
use monet_core::{CancelToken, Url};
use monet_urlsession::UrlsessionTransport;
use tokio_tungstenite::tungstenite::Message;

const STEP_TIMEOUT: Duration = Duration::from_secs(10);

/// Drives `fut` to completion, panicking with `what` on timeout — hangs
/// surface as named failures, not silent waits.
fn wait_for<T>(what: &str, fut: impl Future<Output = T>) -> T {
    futures_lite::future::block_on(async {
        let outcome = futures_lite::future::or(async { Ok(fut.await) }, async {
            futures_timer::Delay::new(STEP_TIMEOUT).await;
            Err(())
        })
        .await;
        match outcome {
            Ok(value) => value,
            Err(()) => panic!("timed out after {STEP_TIMEOUT:?} waiting for {what}"),
        }
    })
}

/// What the server does once connected.
enum Script {
    /// Echo every text/binary message back.
    Echo,
    /// Send a close frame with this code/reason right away.
    ServerClose(u16, &'static str),
    /// Send a ping, wait for the client's pong, then report it.
    PingThenReport,
    /// Send this many one-byte binary messages back to back (one flush),
    /// then wait for the client to go.
    Burst(usize),
}

/// What the test server observed.
#[derive(Debug)]
enum Seen {
    Connected,
    /// The close frame the client sent (code, reason).
    Closed(u16, String),
    /// The pong the client answered with.
    Ponged,
}

/// Spawns a server thread accepting `connections` sockets.
fn spawn_server(script: Script, connections: usize) -> (Url, mpsc::Receiver<Seen>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (seen_tx, seen_rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("monet-urlsession-ws-test-server".into())
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
                    let seen_tx = seen_tx.clone();
                    let script = script.clone();
                    let served = tokio::spawn(async move {
                        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                        let _ = seen_tx.send(Seen::Connected);
                        use futures_util::{SinkExt, StreamExt};
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
                                                .map(|f| (u16::from(f.code), f.reason.to_string()))
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
                                use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
                                use tokio_tungstenite::tungstenite::protocol::CloseFrame;
                                let close = CloseFrame {
                                    code: CloseCode::from(code),
                                    reason: reason.into(),
                                };
                                ws.send(Message::Close(Some(close))).await.unwrap();
                            }
                            Script::Burst(count) => {
                                for i in 0..count {
                                    ws.feed(Message::Binary(vec![(i % 251) as u8].into()))
                                        .await
                                        .unwrap();
                                }
                                ws.flush().await.unwrap();
                                while let Some(Ok(_)) = ws.next().await {}
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

impl Clone for Script {
    fn clone(&self) -> Self {
        match self {
            Script::Echo => Script::Echo,
            Script::ServerClose(code, reason) => Script::ServerClose(*code, reason),
            Script::PingThenReport => Script::PingThenReport,
            Script::Burst(count) => Script::Burst(*count),
        }
    }
}

fn transport() -> UrlsessionTransport {
    UrlsessionTransport::new()
}

fn connect(url: &Url) -> monet_core::ws::WebSocketStream {
    let request = Request::new(Method::Get, url.clone())
        .with_header("sec-websocket-protocol", "chat, superchat")
        .unwrap();
    wait_for(
        "websocket open",
        transport().connect(request, CancelToken::new()),
    )
    .unwrap()
}

#[test]
fn text_and_binary_round_trip() {
    let (url, _seen) = spawn_server(Script::Echo, 1);
    let mut stream = connect(&url);

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

#[test]
fn subprotocol_and_custom_headers_reach_server() {
    // The handshake must carry the requested protocol and headers; the
    // echo server rejects unknown subprotocols, so a successful echo
    // proves the Sec-WebSocket-Protocol header arrived intact.
    let (url, _seen) = spawn_server(Script::Echo, 1);
    let mut stream = connect(&url);
    stream.send(WsMessage::Text("ping".into())).unwrap();
    let echoed = wait_for("echo with protocol", stream.next_message()).unwrap();
    assert_eq!(echoed, Some(WsMessage::Text("ping".into())));
}

#[test]
fn server_close_code_and_reason_round_trip_exactly() {
    // The case URLSessionWebSocketTask cannot do: 1012 with a reason.
    let (url, _seen) = spawn_server(Script::ServerClose(1012, "service restart"), 1);
    let mut stream = connect(&url);

    let closed = wait_for("close frame", stream.next_message()).unwrap();
    match closed {
        Some(WsMessage::Close(Some(frame))) => {
            assert_eq!(frame.code, 1012);
            assert_eq!(frame.reason, "service restart");
        }
        other => panic!("expected a close frame with code 1012, got: {other:?}"),
    }
    // The stream ends after the close frame.
    let end = wait_for("stream end", stream.next_message()).unwrap();
    assert_eq!(end, None);
}

#[test]
fn client_close_reaches_server() {
    let (url, seen) = spawn_server(Script::Echo, 1);
    let mut stream = connect(&url);
    // Consume the handshake notification, then close.
    assert!(matches!(
        seen.recv_timeout(Duration::from_secs(5)),
        Ok(Seen::Connected)
    ));
    stream.close(4321, "client done").unwrap();

    match seen.recv_timeout(Duration::from_secs(5)).unwrap() {
        Seen::Closed(code, reason) => {
            assert_eq!(code, 4321);
            assert_eq!(reason, "client done");
        }
        other => panic!("expected the server to see the close, got: {other:?}"),
    }
}

#[test]
fn ping_is_surfaced_and_ponged() {
    let (url, seen) = spawn_server(Script::PingThenReport, 1);
    let mut stream = connect(&url);
    assert!(matches!(
        seen.recv_timeout(Duration::from_secs(5)),
        Ok(Seen::Connected)
    ));

    // The shim surfaces the ping to the user...
    let ping = wait_for("ping message", stream.next_message()).unwrap();
    assert_eq!(
        ping,
        Some(WsMessage::Ping(bytes::Bytes::from_static(b"probe")))
    );
    // ...and answered it without the user doing anything.
    match seen.recv_timeout(Duration::from_secs(5)).unwrap() {
        Seen::Ponged => {}
        other => panic!("expected the server to see the auto-pong, got: {other:?}"),
    }
}

#[test]
fn pre_cancelled_token_fails_fast() {
    let (url, _seen) = spawn_server(Script::Echo, 1);
    let cancel = CancelToken::new();
    cancel.cancel();
    let request = Request::new(Method::Get, url);
    let err = wait_for("cancel", transport().connect(request, cancel)).unwrap_err();
    assert!(matches!(err, Error::Cancelled));
}

#[test]
fn connection_refused_maps_to_transport_error() {
    // Nothing listens on port 1 of loopback.
    let url = Url::parse("ws://127.0.0.1:1/socket").unwrap();
    let request = Request::new(Method::Get, url);
    let err = wait_for("refused", transport().connect(request, CancelToken::new())).unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

/// A burst of small frames: the parser used to drop each frame with
/// `removeFirst`, shifting the whole rest of the buffer per frame —
/// quadratic in frames per read (a read is up to 4MB).
#[test]
fn a_burst_of_small_frames_parses_in_linear_time() {
    const COUNT: usize = 1_000_000;
    let (url, _seen) = spawn_server(Script::Burst(COUNT), 1);
    let mut stream = connect(&url);
    let started = std::time::Instant::now();
    let received = futures_lite::future::block_on(futures_lite::future::or(
        async {
            let mut received = 0usize;
            while received < COUNT {
                match stream.next_message().await {
                    Ok(Some(WsMessage::Binary(bytes))) => {
                        assert_eq!(bytes[..], [(received % 251) as u8], "message {received}");
                        received += 1;
                    }
                    other => panic!("message {received}: {other:?}"),
                }
            }
            received
        },
        async {
            futures_timer::Delay::new(Duration::from_secs(20)).await;
            0
        },
    ));
    let elapsed = started.elapsed();
    assert_eq!(received, COUNT, "the burst did not arrive within 20s");
    // The quadratic parser took ~10s here; the linear one ~1.2s.
    assert!(
        elapsed < Duration::from_secs(5),
        "the burst took {elapsed:?}"
    );
}
