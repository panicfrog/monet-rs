//! The HTTP conformance battery: a path-routed local server and one
//! check function per contract behavior. Checks run sequentially against
//! a shared server; each one that cares about what the server saw drains
//! its own observations from the queue.

use std::sync::Mutex;
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
use monet_core::transport::Transport;
use monet_core::{CancelToken, Url};

use crate::wait_for;

/// What the server saw on one request.
#[derive(Debug, Clone)]
pub struct Seen {
    /// Request method.
    pub method: String,
    /// Request path + query.
    pub uri: String,
    /// Request headers.
    pub headers: Vec<(String, String)>,
    /// Request body bytes.
    pub body: Vec<u8>,
}

/// A local hyper server exposing one route per check.
pub struct HttpServer {
    base: Url,
    /// The raw queue plus a backlog of observations other checks let
    /// behind — checks correlate by path, not by order.
    seen: Mutex<SeenQueue>,
}

struct SeenQueue {
    receiver: mpsc::Receiver<Seen>,
    backlog: std::collections::VecDeque<Seen>,
}

impl HttpServer {
    /// Spawns the server on a loopback port.
    pub fn spawn() -> HttpServer {
        let (addr_tx, addr_rx) = mpsc::channel();
        let (seen_tx, seen_rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("monet-conformance-http".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    addr_tx.send(listener.local_addr().unwrap()).unwrap();
                    loop {
                        let (stream, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(_) => break,
                        };
                        let seen_tx = seen_tx.clone();
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
                                Ok::<_, std::convert::Infallible>(
                                    respond(parts.uri.path(), body.to_vec()).await,
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
        HttpServer {
            base: Url::parse(&format!("http://{addr}")).unwrap(),
            seen: Mutex::new(SeenQueue {
                receiver: seen_rx,
                backlog: std::collections::VecDeque::new(),
            }),
        }
    }

    /// The URL for a route.
    pub fn url(&self, path: &str) -> Url {
        self.base.join(path).unwrap()
    }

    /// The observation for the request that hit `path`, skipping (and
    /// keeping) entries from other checks — checks correlate by path,
    /// never by queue order.
    fn seen_for(&self, path: &str) -> Seen {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut queue = self.seen.lock().unwrap();
        if let Some(position) = queue.backlog.iter().position(|seen| seen.uri == path) {
            return queue.backlog.remove(position).unwrap();
        }
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            match queue.receiver.recv_timeout(remaining) {
                Ok(seen) if seen.uri == path => return seen,
                Ok(other) => queue.backlog.push_back(other),
                Err(_) => panic!("no request recorded for {path}"),
            }
        }
    }
}

/// Answers one route. `request_body` is the body the check sent — the
/// echo route reflects it.
async fn respond(
    path: &str,
    request_body: Vec<u8>,
) -> HyperResponse<BoxBody<Bytes, std::convert::Infallible>> {
    match path {
        "/plain" => ok(
            StatusCode::OK,
            boxed(Full::new(Bytes::from_static(b"plain"))),
        ),
        "/chunked" => {
            let stream = futures_lite::stream::iter([
                b"alpha".to_vec(),
                b"beta".to_vec(),
                b"gamma".to_vec(),
            ])
            .then(|chunk| async move {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok(Frame::data(Bytes::from(chunk)))
            });
            ok(
                StatusCode::OK,
                boxed(http_body_util::StreamBody::new(stream)),
            )
        }
        // The echo route answers with the request body so the check can
        // verify it survived the transport.
        "/echo" => ok(StatusCode::OK, boxed(Full::new(Bytes::from(request_body)))),
        "/redirect" => {
            let mut response = ok(
                StatusCode::FOUND,
                boxed(Full::new(Bytes::from_static(b"moved"))),
            );
            // A route unique to this check so observation correlation by
            // path stays exact.
            response
                .headers_mut()
                .insert("location", "/redirect-final".parse().unwrap());
            response
        }
        "/redirect-final" => ok(
            StatusCode::OK,
            boxed(Full::new(Bytes::from_static(b"plain"))),
        ),
        // The service itself sleeps before responding: a genuinely slow
        // HEAD. (A slow first body chunk doesn't delay the head — hyper
        // flushes it immediately; only URLSession's delivery coalesces
        // them.)
        "/slow-head" => {
            tokio::time::sleep(Duration::from_millis(500)).await;
            ok(
                StatusCode::OK,
                boxed(Full::new(Bytes::from_static(b"late head"))),
            )
        }
        // A big first chunk right away, then the tail after 500ms — for
        // mid-body cancellation. (Unknown-length responses travel with
        // their first frame; small delayed chunks get coalesced by some
        // transports, so the first chunk must be big enough to force
        // delivery on its own.)
        "/stalled" => {
            let stream = futures_lite::stream::iter(0..2).then(|i| async move {
                if i > 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Ok(Frame::data(Bytes::from(if i == 0 {
                    vec![b'a'; 512 * 1024]
                } else {
                    b"tail".to_vec()
                })))
            });
            ok(
                StatusCode::OK,
                boxed(http_body_util::StreamBody::new(stream)),
            )
        }
        _ => ok(StatusCode::NOT_FOUND, boxed(Full::new(Bytes::new()))),
    }
}

fn ok(
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

fn get(url: Url) -> Request {
    Request::new(Method::Get, url)
}

/// GET `/plain`: status 200, body intact.
pub fn get_plain_body(t: &dyn Transport, server: &HttpServer) {
    let response = wait_for(
        "plain body response",
        t.execute(get(server.url("/plain")), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200);
    let chunks = wait_for(
        "plain body",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(chunks.concat(), b"plain".to_vec());
}

/// GET `/chunked`: content exact. Chunk boundaries are transport-owned
/// (URLSession coalesces; hyper preserves) and are deliberately not
/// asserted.
pub fn get_chunked_content(t: &dyn Transport, server: &HttpServer) {
    let response = wait_for(
        "chunked response head",
        t.execute(get(server.url("/chunked")), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200);
    let chunks = wait_for(
        "chunked body",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert!(!chunks.is_empty(), "no body chunks arrived");
    assert_eq!(chunks.concat(), b"alphabetagamma".to_vec());
}

/// POST `/echo`: method, path, headers, and body reach the server, and
/// the body content round-trips.
pub fn post_reaches_server(t: &dyn Transport, server: &HttpServer) {
    let request = Request::new(Method::Post, server.url("/echo"))
        .with_header("content-type", "application/json")
        .unwrap()
        .with_body_bytes(br#"{"conformance":"post"}"#);
    let response = wait_for("post response", t.execute(request, CancelToken::new())).unwrap();
    assert_eq!(response.status, 200);
    let body = wait_for(
        "post echo body",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(body.concat(), br#"{"conformance":"post"}"#.to_vec());

    let seen = server.seen_for("/echo");
    assert_eq!(seen.method, "POST");
    assert_eq!(
        seen.headers
            .iter()
            .find(|(n, _)| n == "content-type")
            .unwrap()
            .1,
        "application/json"
    );
    assert_eq!(seen.body, br#"{"conformance":"post"}"#.to_vec());
}

/// GET `/redirect`: a 3xx is followed by default to the terminal
/// response, and both legs reach the server.
pub fn redirect_followed_by_default(t: &dyn Transport, server: &HttpServer) {
    let response = wait_for(
        "redirect outcome",
        t.execute(get(server.url("/redirect")), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200, "redirect was not followed");
    let body = wait_for(
        "redirect final body",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(body.concat(), b"plain".to_vec());
    assert_eq!(server.seen_for("/redirect").method, "GET");
    assert_eq!(server.seen_for("/redirect-final").method, "GET");
}

/// A token cancelled before `execute` fails fast without touching the
/// network.
pub fn pre_cancelled_fails_fast(t: &dyn Transport, server: &HttpServer) {
    let cancel = CancelToken::new();
    cancel.cancel();
    let err = wait_for(
        "pre-cancelled outcome",
        t.execute(get(server.url("/plain")), cancel),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Cancelled), "got: {err:?}");
}

/// A connection refused before any response maps to a transport error,
/// never a hang and never `Cancelled`.
pub fn connection_refused_maps_to_transport_error(t: &dyn Transport) {
    let url = Url::parse("http://127.0.0.1:1/").unwrap();
    let err = wait_for("refused outcome", t.execute(get(url), CancelToken::new())).unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

/// Cancel after the head: buffered chunks may drain, then the body must
/// end with `Cancelled` — and the stalled tail must never arrive.
pub fn cancel_after_head_aborts_body(t: &dyn Transport, server: &HttpServer) {
    let cancel = CancelToken::new();
    let response = wait_for(
        "stalled response head",
        t.execute(get(server.url("/stalled")), cancel.clone()),
    )
    .unwrap();
    assert_eq!(response.status, 200);

    let mut body = response.body;
    let first = wait_for("stalled first chunk", body.next())
        .unwrap()
        .unwrap();
    assert!(!first.is_empty(), "no data arrived with the head");

    cancel.cancel();
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

/// The response head must respect the transport's timeout
/// configuration: `/slow-head` answers after 500ms, the transport is
/// configured for 100ms. Run this with a timeout-configured instance.
pub fn timeout_maps_to_transport_error(t: &dyn Transport, server: &HttpServer) {
    let err = wait_for(
        "timeout outcome",
        t.execute(get(server.url("/slow-head")), CancelToken::new()),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

/// The core battery: everything except the two checks that need their
/// own transport configuration (`connection_refused…` works on any
/// instance but takes no server; `timeout…` needs a timeout-configured
/// instance).
pub fn run_suite(t: &dyn Transport, server: &HttpServer) {
    get_plain_body(t, server);
    get_chunked_content(t, server);
    post_reaches_server(t, server);
    redirect_followed_by_default(t, server);
    pre_cancelled_fails_fast(t, server);
    cancel_after_head_aborts_body(t, server);
}

/// Convenience: spawn a server and run the core battery plus the
/// refused-connection check.
pub fn run(t: &dyn Transport) {
    let server = HttpServer::spawn();
    run_suite(t, &server);
    connection_refused_maps_to_transport_error(t);
}
