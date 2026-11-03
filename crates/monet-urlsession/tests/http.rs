//! Integration tests against a local hyper server, driven through the
//! real URLSession stack (the macOS host's) — same shape as
//! monet-rustls's `local_server.rs`, plus redirect/timeout/cancel
//! scenarios. Plain HTTP to loopback is exempt from App Transport
//! Security, so no test-bundle exceptions are needed.

use std::future::Future;
use std::sync::mpsc;
use std::time::Duration;

use futures_lite::StreamExt;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::combinators::BoxBody;
use hyper::body::{Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper::{Request as HyperRequest, Response as HyperResponse, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use monet_core::error::Error;
use monet_core::method::Method;
use monet_core::request::Request;
use monet_core::transport::Transport as _;
use monet_core::{CancelToken, Url};
use monet_urlsession::UrlsessionTransport;

/// What the test server saw on one request.
#[derive(Clone)]
struct Seen {
    method: String,
    uri: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

/// What a connection replies with.
#[derive(Clone)]
enum Reply {
    /// One fixed body.
    Plain(&'static str),
    /// A body streamed in chunks with pauses, so boundaries are observable.
    Chunked,
    /// 302 with a Location header.
    Redirect(&'static str),
    /// Hold the response head back (service sleeps before responding).
    DelayedHead(u64),
    /// A large first chunk immediately, then a pause before a small tail —
    /// for mid-body cancellation. The head travels with the first bytes
    /// (URLSession coalesces small delayed chunks into one data event, so
    /// the first chunk must be big enough to force delivery on its own).
    StalledBody(u64),
}

/// Spawns a server that accepts up to `connections` requests, recording
/// each. Extra accepts (e.g. a redirect the client never makes) simply
/// park; test threads are abandoned at process exit, like monet-rustls's.
fn spawn_server(reply: Reply, connections: usize) -> (Url, mpsc::Receiver<Seen>) {
    let (addr_tx, addr_rx) = mpsc::channel();
    let (seen_tx, seen_rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("monet-urlsession-test-server".into())
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
                    let reply = reply.clone();
                    let seen_tx = seen_tx.clone();
                    let service = service_fn(move |req: HyperRequest<Incoming>| {
                        let reply = reply.clone();
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
                            Ok::<_, std::convert::Infallible>(
                                respond(&reply, parts.uri.path()).await,
                            )
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                }
            });
        })
        .unwrap();
    let addr = addr_rx.recv().unwrap();
    (
        Url::parse(&format!("http://{addr}/things")).unwrap(),
        seen_rx,
    )
}

/// Builds the response for a reply mode. In redirect mode the target
/// path answers plain (a terminal response), every other path redirects —
/// otherwise a following client would loop forever.
async fn respond(
    reply: &Reply,
    path: &str,
) -> HyperResponse<BoxBody<Bytes, std::convert::Infallible>> {
    match reply {
        Reply::Plain(body) => ok_response(
            StatusCode::OK,
            boxed(Full::new(Bytes::from_static(body.as_bytes()))),
        ),
        Reply::Chunked => {
            let stream = futures_lite::stream::iter([
                b"alpha".to_vec(),
                b"beta".to_vec(),
                b"gamma".to_vec(),
            ])
            .then(|chunk| async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(Frame::data(Bytes::from(chunk)))
            });
            ok_response(
                StatusCode::OK,
                boxed(http_body_util::StreamBody::new(stream)),
            )
        }
        Reply::Redirect(location) if path == *location => ok_response(
            StatusCode::OK,
            boxed(Full::new(Bytes::from_static(b"final body"))),
        ),
        Reply::Redirect(location) => {
            let mut response = ok_response(
                StatusCode::FOUND,
                boxed(Full::new(Bytes::from_static(b"moved"))),
            );
            response
                .headers_mut()
                .insert("location", location.parse().unwrap());
            response
        }
        Reply::DelayedHead(ms) => {
            tokio::time::sleep(Duration::from_millis(*ms)).await;
            ok_response(
                StatusCode::OK,
                boxed(Full::new(Bytes::from_static(b"late head"))),
            )
        }
        Reply::StalledBody(ms) => {
            let delay = Duration::from_millis(*ms);
            let stream = futures_lite::stream::iter(0..2).then(move |i| async move {
                if i > 0 {
                    tokio::time::sleep(delay).await;
                }
                // 512 KiB then a short tail: the first chunk is too big to
                // be held back or coalesced, the tail is what gets
                // cancelled.
                Ok(Frame::data(Bytes::from(if i == 0 {
                    vec![b'a'; 512 * 1024]
                } else {
                    b"tail".to_vec()
                })))
            });
            ok_response(
                StatusCode::OK,
                boxed(http_body_util::StreamBody::new(stream)),
            )
        }
    }
}

fn ok_response(
    status: StatusCode,
    body: BoxBody<Bytes, std::convert::Infallible>,
) -> HyperResponse<BoxBody<Bytes, std::convert::Infallible>> {
    HyperResponse::builder().status(status).body(body).unwrap()
}

fn boxed<B>(body: B) -> BoxBody<Bytes, std::convert::Infallible>
where
    B: http_body::Body<Data = Bytes, Error = std::convert::Infallible> + Send + Sync + 'static,
{
    BodyExt::boxed(body)
}

/// Every transport interaction waits through this: a stuck `block_on`
/// otherwise hangs the test forever with no hint of which step died.
const STEP_TIMEOUT: Duration = Duration::from_secs(10);

/// Drives `fut` to completion on this thread, panicking with `what` if
/// it doesn't finish within [`STEP_TIMEOUT`] — so hangs surface as
/// named failures instead of silent waits.
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

#[test]
fn get_streams_chunked_body() {
    let (url, _seen) = spawn_server(Reply::Chunked, 1);
    let transport = UrlsessionTransport::new();

    let response = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200);

    let chunks = wait_for(
        "body chunks",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    // URLSession may coalesce data events, so the chunk count is >= 1;
    // content and ordering are exact.
    assert!(!chunks.is_empty(), "no body chunks arrived");
    let joined: Vec<u8> = chunks.concat();
    assert_eq!(joined, b"alphabetagamma".to_vec());
}

#[test]
fn post_reaches_server_with_headers_and_body() {
    let (url, seen) = spawn_server(Reply::Plain("ok"), 1);
    let transport = UrlsessionTransport::new();

    let request = Request::new(Method::Post, url)
        .with_header("content-type", "application/json")
        .unwrap()
        .with_body_bytes(br#"{"hello":"urlsession"}"#);
    let response = wait_for(
        "response head",
        transport.execute(request, CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200);

    let seen = seen.recv_timeout(Duration::from_secs(5)).unwrap();
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
    assert_eq!(seen.body, br#"{"hello":"urlsession"}"#.to_vec());
}

#[test]
fn response_head_and_headers_arrive() {
    let (url, _seen) = spawn_server(Reply::Redirect("/final"), 2);
    let transport = UrlsessionTransport::new().with_follow_redirects(false);

    // Not following: the 3xx head is the response.
    let response = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 302);
    assert_eq!(response.headers.get("location").unwrap(), "/final");
    let body = wait_for(
        "body chunks",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(body.concat(), b"moved".to_vec());
}

#[test]
fn redirect_is_followed_by_default() {
    let (url, seen) = spawn_server(Reply::Redirect("/final"), 2);
    let transport = UrlsessionTransport::new();

    let response = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap();
    // Followed all the way to the terminal response.
    assert_eq!(response.status, 200);
    let body = wait_for(
        "body chunks",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(body.concat(), b"final body".to_vec());
    // Both legs hit the server: the original and the redirect target.
    let first = seen.recv_timeout(Duration::from_secs(5)).unwrap();
    let second = seen.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(first.uri, "/things");
    assert_eq!(second.uri, "/final");
}

#[test]
fn pre_cancelled_token_fails_fast() {
    let (url, _seen) = spawn_server(Reply::Plain("x"), 1);
    let transport = UrlsessionTransport::new();
    let cancel = CancelToken::new();
    cancel.cancel();
    let err = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), cancel),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Cancelled));
}

#[test]
fn cancel_before_head_maps_to_cancelled() {
    let (url, _seen) = spawn_server(Reply::DelayedHead(500), 1);
    let transport = UrlsessionTransport::new();
    let cancel = CancelToken::new();

    let racer = {
        let cancel = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancel.cancel();
        })
    };
    let err = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), cancel),
    )
    .unwrap_err();
    racer.join().unwrap();
    assert!(matches!(err, Error::Cancelled), "got: {err:?}");
}

#[test]
fn cancel_after_head_aborts_body() {
    let (url, _seen) = spawn_server(Reply::StalledBody(500), 1);
    let transport = UrlsessionTransport::new();
    let cancel = CancelToken::new();

    // The head arrives with the first (large) chunk; the tail is held
    // back, so the cancellation below lands mid-body.
    let response = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), cancel.clone()),
    )
    .unwrap();
    assert_eq!(response.status, 200);

    let mut body = response.body;
    let first = wait_for("first body chunk", body.next()).unwrap().unwrap();
    assert!(!first.is_empty(), "no body data arrived with the head");

    cancel.cancel();
    // Buffered first-chunk pieces may still drain through; the stream
    // must then end with Cancelled, never with the stalled tail.
    let mut received = first.len();
    loop {
        match wait_for("body after cancel", body.next()) {
            Some(Ok(chunk)) => received += chunk.len(),
            Some(Err(Error::Cancelled)) => break,
            other => panic!("expected cancelled body error, got: {other:?}"),
        }
    }
    assert!(
        received < 512 * 1024 + 4,
        "the stalled tail arrived despite cancellation ({received} bytes)"
    );
}

#[test]
fn timeout_maps_to_transport_error() {
    let (url, _seen) = spawn_server(Reply::DelayedHead(500), 1);
    let transport = UrlsessionTransport::new().with_timeout_ms(100);

    let err = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Transport { code: Some(ref c), .. } if c.platform == "urlsession" && c.code == -1001),
        "got: {err:?}"
    );
}

#[test]
fn connection_failure_maps_to_transport_error() {
    // Nothing listens on port 1 of loopback.
    let url = Url::parse("http://127.0.0.1:1/").unwrap();
    let transport = UrlsessionTransport::new();
    let err = wait_for(
        "response head",
        transport.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

#[test]
fn plain_body_arrives_intact() {
    let (url, _seen) = spawn_server(Reply::Plain("plain"), 1);
    let transport = UrlsessionTransport::new();
    let request = Request::new(Method::Get, url)
        .with_header("accept", "*/*")
        .unwrap();
    let response = wait_for(
        "response head",
        transport.execute(request, CancelToken::new()),
    )
    .unwrap();
    let chunks = wait_for(
        "body chunks",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(chunks.concat(), b"plain".to_vec());
}
