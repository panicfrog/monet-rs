//! Integration tests against a local hyper server.

use futures_lite::StreamExt as _;
use futures_lite::future::block_on;
use http_body_util::BodyExt;
use http_body_util::combinators::BoxBody as BoxedBody;
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request as HyperRequest, Response as HyperResponse};
use hyper_util::rt::{TokioExecutor, TokioIo};
use monet_core::error::Error;
use monet_core::method::Method;
use monet_core::request::Request;
use monet_core::transport::Transport as _;
use monet_core::{CancelToken, Url};
use monet_rustls::{MonetRustlsConfig, MonetRustlsTransport};
use std::sync::mpsc;
use std::time::Duration;

/// What the test server saw.
#[derive(Clone)]
struct Seen {
    method: String,
    uri: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

/// Spawns a local server that records request metadata; `chunked` responses
/// stream in three parts with pauses so chunk boundaries are observable.
fn spawn_server(chunked: bool) -> (Url, mpsc::Receiver<Seen>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (seen_tx, seen_rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("monet-rustls-test-server".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                let service = service_fn(move |req: HyperRequest<Incoming>| {
                    let seen_tx = seen_tx.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let body = body.collect().await.unwrap().to_bytes();
                        let _ = seen_tx.send(Seen {
                            method: parts.method.as_str().to_string(),
                            uri: parts.uri.to_string(),
                            headers: parts
                                .headers
                                .iter()
                                .map(|(n, v)| {
                                    (
                                        n.to_string(),
                                        String::from_utf8_lossy(v.as_bytes()).into_owned(),
                                    )
                                })
                                .collect(),
                            body: body.to_vec(),
                        });
                        let body: BoxedBody<bytes::Bytes, std::convert::Infallible> = if chunked {
                            <http_body_util::StreamBody<_> as BodyExt>::boxed(
                                http_body_util::StreamBody::new(stream_body()),
                            )
                        } else {
                            <http_body_util::Full<bytes::Bytes> as BodyExt>::boxed(
                                http_body_util::Full::new(bytes::Bytes::from_static(b"plain")),
                            )
                        };
                        Ok::<_, std::convert::Infallible>(HyperResponse::new(body))
                    }
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        })
        .unwrap();
    let addr = addr_rx.recv().unwrap();
    (
        Url::parse(&format!("http://{addr}/things")).unwrap(),
        seen_rx,
    )
}

/// A body of three chunks with pauses, so the client sees three frames.
fn stream_body()
-> impl futures_lite::Stream<Item = Result<Frame<bytes::Bytes>, std::convert::Infallible>> {
    futures_lite::stream::iter([b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]).then(
        |chunk| async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            Ok(Frame::data(bytes::Bytes::from(chunk)))
        },
    )
}

#[test]
fn get_streams_chunked_body() {
    let (url, _seen) = spawn_server(true);
    let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();

    let response =
        block_on(transport.execute(Request::new(Method::Get, url), CancelToken::new())).unwrap();
    assert_eq!(response.status, 200);

    let chunks = block_on(response.body.map(|c| c.unwrap()).collect::<Vec<_>>());
    let sizes: Vec<usize> = chunks.iter().map(|c| c.len()).collect();
    assert_eq!(sizes, [5, 4, 5], "chunk boundaries survive the bridge");
    let joined: Vec<u8> = chunks.concat();
    assert_eq!(joined, b"alphabetagamma".to_vec());
}

#[test]
fn post_reaches_server_with_headers_and_body() {
    let (url, seen) = spawn_server(false);
    let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();

    let request = Request::new(Method::Post, url)
        .with_header("content-type", "application/json")
        .unwrap()
        .with_body_bytes(br#"{"hello":"hyper"}"#);
    let response = block_on(transport.execute(request, CancelToken::new())).unwrap();
    assert_eq!(response.status, 200);

    let seen = seen.recv().unwrap();
    assert_eq!(seen.method, "POST");
    assert_eq!(seen.uri, "/things");
    assert_eq!(
        seen.headers
            .iter()
            .find(|(n, _)| n == "content-type")
            .unwrap()
            .1,
        "application/json"
    );
    assert_eq!(seen.body, br#"{"hello":"hyper"}"#.to_vec());
}

#[test]
fn pre_cancelled_token_fails_fast() {
    let (url, _seen) = spawn_server(false);
    let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();
    let cancel = CancelToken::new();
    cancel.cancel();
    let err = block_on(transport.execute(Request::new(Method::Get, url), cancel)).unwrap_err();
    assert!(matches!(err, Error::Cancelled));
}

#[test]
fn connection_failure_maps_to_transport_error() {
    // Nothing listens on port 1 of loopback.
    let url = Url::parse("http://127.0.0.1:1/").unwrap();
    let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();
    let err = block_on(transport.execute(Request::new(Method::Get, url), CancelToken::new()))
        .unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

#[test]
fn plain_body_arrives_intact() {
    let (url, _seen) = spawn_server(false);
    let transport = MonetRustlsTransport::new(MonetRustlsConfig::default()).unwrap();
    let request = Request::new(Method::Get, url)
        .with_header("accept", "*/*")
        .unwrap();
    let response = block_on(transport.execute(request, CancelToken::new())).unwrap();
    let chunks = block_on(response.body.map(|c| c.unwrap()).collect::<Vec<_>>());
    assert_eq!(chunks.concat(), b"plain".to_vec());
}
