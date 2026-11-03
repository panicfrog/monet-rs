//! Round-trip tests against a local (non-TLS) tungstenite echo server.

use futures_lite::StreamExt as _;
use futures_lite::future::block_on;
use futures_util::SinkExt as _;
use monet_core::error::Error;
use monet_core::method::Method;
use monet_core::transport::WebSocketTransport as _;
use monet_core::ws::WsMessage;
use monet_core::{CancelToken, Request, Url};
use monet_ws::{MonetWsTransport, WsConfig};

/// Spawns an echo server on a random port; returns its address.
fn spawn_echo_server() -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
    use std::sync::mpsc;
    let (addr_tx, addr_rx) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("monet-ws-test-server".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                // Echo until the client closes, then close politely.
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_close() {
                        break;
                    }
                    ws.send(message).await.unwrap();
                }
                let _ = ws.close(None).await;
            });
        })
        .unwrap();
    (addr_rx.recv().unwrap(), thread)
}

#[test]
fn echo_round_trip_then_server_close() {
    let (addr, server) = spawn_echo_server();
    let transport = MonetWsTransport::new(WsConfig::default()).unwrap();

    let mut request = Request::new(Method::Get, Url::parse(&format!("ws://{addr}/")).unwrap());
    request.headers.append("x-test", "1").unwrap();

    let mut stream = block_on(transport.connect(request, CancelToken::new())).unwrap();

    // Text and binary both survive the round trip.
    stream.send(WsMessage::Text("hello".into())).unwrap();
    let echoed = block_on(stream.next_message()).unwrap();
    assert_eq!(echoed, Some(WsMessage::Text("hello".into())));

    stream
        .send(WsMessage::Binary(bytes::Bytes::from_static(b"\x01\x02")))
        .unwrap();
    let echoed = block_on(stream.next_message()).unwrap();
    assert_eq!(
        echoed,
        Some(WsMessage::Binary(bytes::Bytes::from_static(b"\x01\x02")))
    );

    // Client closes; the server loop ends and closes its side.
    stream.close(1000, "done").unwrap();
    let end = block_on(stream.next_message()).unwrap();
    assert_eq!(end, None);

    server.join().unwrap();
}

#[test]
fn pre_cancelled_token_fails_fast() {
    let transport = MonetWsTransport::new(WsConfig::default()).unwrap();
    let cancel = CancelToken::new();
    cancel.cancel();
    let request = Request::new(Method::Get, Url::parse("ws://127.0.0.1:1/").unwrap());
    let err = block_on(transport.connect(request, cancel)).unwrap_err();
    assert!(matches!(err, Error::Cancelled));
}
