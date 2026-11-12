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
    addr: std::net::SocketAddr,
    /// The raw queue plus a backlog of observations other checks let
    /// behind — checks correlate by path, not by order.
    seen: Mutex<SeenQueue>,
}

struct SeenQueue {
    receiver: mpsc::Receiver<Seen>,
    backlog: std::collections::VecDeque<Seen>,
}

impl HttpServer {
    /// Spawns the plain-HTTP server on a loopback port.
    pub fn spawn() -> HttpServer {
        Self::spawn_on("127.0.0.1:0")
    }

    /// Spawns the plain-HTTP server on the given address. The server bin
    /// behind the JS battery binds `0.0.0.0:<port>` so emulators
    /// (`10.0.2.2`) and LAN devices hit the same routes as the host.
    pub fn spawn_on(addr: &str) -> HttpServer {
        Self::spawn_inner(
            addr,
            |stream| async move { Ok::<_, std::io::Error>(stream) },
        )
    }

    /// Spawns the server with a custom accept step wrapping each TCP
    /// stream (the TLS scenarios hand in a handshake) on the given bind.
    pub(crate) fn spawn_with<Accept, Fut, S>(bind: &str, accept: Accept) -> HttpServer
    where
        Accept: Fn(tokio::net::TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = std::io::Result<S>> + Send,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        Self::spawn_inner(bind, accept)
    }

    fn spawn_inner<Accept, Fut, S>(bind: &str, accept: Accept) -> HttpServer
    where
        Accept: Fn(tokio::net::TcpStream) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = std::io::Result<S>> + Send,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let (addr_tx, addr_rx) = mpsc::channel();
        let (seen_tx, seen_rx) = mpsc::channel();
        // Bind on the caller's side so a bad address (a port already in
        // use, say) surfaces here with a clear panic location instead of
        // a dropped-channel panic inside the server thread.
        let std_listener = std::net::TcpListener::bind(bind).unwrap();
        std_listener.set_nonblocking(true).unwrap();
        // The concurrency gate: `/held`'s tail is released only once `/fast`
        // has been seen — the held-download check builds its deadlock-proof
        // on it (see `held_body_does_not_block_new_requests`).
        let gate = std::sync::Arc::new(tokio::sync::Notify::new());
        std::thread::Builder::new()
            .name("monet-conformance-http".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
                    addr_tx.send(listener.local_addr().unwrap()).unwrap();
                    loop {
                        let (stream, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(_) => break,
                        };
                        let io = match accept(stream).await {
                            Ok(io) => io,
                            Err(_) => continue,
                        };
                        let seen_tx = seen_tx.clone();
                        let gate = gate.clone();
                        let service = service_fn(move |req: HyperRequest<Incoming>| {
                            let seen_tx = seen_tx.clone();
                            let gate = gate.clone();
                            async move {
                                let (parts, body) = req.into_parts();
                                let body = body.collect().await.unwrap().to_bytes();
                                // The rig's result channel: the battery app
                                // reports each check's outcome here and the
                                // server prints it (its stdout is the log the
                                // harness reads — RN's console.log never
                                // reaches os_log, and Metro's terminal is not
                                // the rig's to read).
                                if parts.uri.path() == "/report" {
                                    println!(
                                        "REPORT {} {}",
                                        parts.uri.query().unwrap_or(""),
                                        parts.method.as_str()
                                    );
                                }
                                if parts.uri.path() == "/upload" {
                                    // The upload wire-form probe: what the
                                    // request actually carried.
                                    let header = |name: &str| {
                                        parts
                                            .headers
                                            .get(name)
                                            .and_then(|v| v.to_str().ok())
                                            .unwrap_or("-")
                                            .to_string()
                                    };
                                    println!(
                                        "UPLOAD len={} te={} body_bytes={}",
                                        header("content-length"),
                                        header("transfer-encoding"),
                                        body.len()
                                    );
                                }
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
                                    respond(
                                        parts.method.as_str(),
                                        parts.uri.path(),
                                        parts.uri.query().unwrap_or(""),
                                        &parts.headers,
                                        body.to_vec(),
                                        &gate,
                                    )
                                    .await,
                                )
                            }
                        });
                        // Each connection is served concurrently: the
                        // concurrency checks hold one connection open
                        // mid-body while a second request needs another.
                        tokio::spawn(async move {
                            let _ =
                                hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                                    .serve_connection(TokioIo::new(io), service)
                                    .await;
                        });
                    }
                });
            })
            .unwrap();
        let addr = addr_rx.recv().unwrap();
        HttpServer {
            base: Url::parse(&format!("http://{addr}")).unwrap(),
            addr,
            seen: Mutex::new(SeenQueue {
                receiver: seen_rx,
                backlog: std::collections::VecDeque::new(),
            }),
        }
    }

    /// The address the server actually bound (meaningful when the caller
    /// asked for port 0).
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.addr
    }

    /// The URL for a route.
    pub fn url(&self, path: &str) -> Url {
        self.base.join(path).unwrap()
    }

    /// The URL for a route addressed through a different scheme and host
    /// — the TLS checks connect as `https://localhost` (monet-core's pin
    /// config takes hostnames, not IPs) against the same loopback
    /// listener.
    pub fn url_on(&self, scheme: &str, host: &str, path: &str) -> Url {
        let port = self.base.port_or_known_default().unwrap_or(80);
        Url::parse(&format!("{scheme}://{host}:{port}{path}")).unwrap()
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

/// The leaf SPKI SHA-256 (base64) of the running TLS battery server, if
/// one is up — set by
/// [`spawn_https_on`](crate::tls::spawn_https_on), served at
/// `/leaf-spki` so an on-device client can pin the fresh leaf (every
/// start mints a new leaf key, so the hash is served, never baked in).
static TLS_LEAF_SPKI: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// The /flaky route's visit counts (v2/W5) — keyed by path+query so
/// concurrent checks arm independent counters.
static FLAKY_VISITS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, usize>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Registers the TLS battery server's leaf SPKI for `/leaf-spki`.
pub(crate) fn set_tls_leaf_spki(hash: String) {
    let _ = TLS_LEAF_SPKI.set(hash);
}

/// The battery CA's DER (base64), if a TLS variant is running — set by
/// [`spawn_https_on`](crate::tls::spawn_https_on), served at `/ca` so an
/// on-device battery can hand the CA to configureTrust (the Rust stacks
/// cannot see the network_security_config anchor the platform stacks
/// read). Same shape as `/leaf-spki`: bare base64, 404 without a TLS
/// server.
static TLS_CA: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Registers the battery CA for `/ca`.
pub(crate) fn set_tls_ca(der_base64: String) {
    let _ = TLS_CA.set(der_base64);
}

/// Whether responses close the connection after the body. The TLS battery
/// server turns this on: clients pool connections per (host, port) —
/// URLSession included — and a fetch served over a pooled TLS connection
/// performs no handshake, so no server-trust challenge and no pin
/// evaluation: the pin checks would ride whatever connection a previous
/// fetch established. Closing after each response forces a full
/// handshake (and the challenge, and the pin evaluation) on every fetch.
/// (The in-process Rust batteries never need this — each check spawns a
/// fresh server, so every connection is a fresh one.)
static TLS_CONNECTION_CLOSE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Requests handled since server start — the mock check reads it around
/// a mocked fetch and asserts it only moved by the reads themselves.
static REQUESTS_HANDLED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Arms `Connection: close` on every response — the TLS battery server.
pub(crate) fn set_tls_connection_close() {
    TLS_CONNECTION_CLOSE.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Answers one route. `request_body` is the body the check sent — the
/// echo route reflects it. `gate` is the concurrency gate (see `/fast`).
async fn respond(
    method: &str,
    path: &str,
    query: &str,
    request_headers: &hyper::HeaderMap,
    request_body: Vec<u8>,
    gate: &std::sync::Arc<tokio::sync::Notify>,
) -> HyperResponse<BoxBody<Bytes, std::convert::Infallible>> {
    // Every handled request counts (v2/W3): a mocked fetch must not move
    // it — the mock check's zero-connection observable.
    REQUESTS_HANDLED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut response =
        respond_route(method, path, query, request_headers, request_body, gate).await;
    if TLS_CONNECTION_CLOSE.load(std::sync::atomic::Ordering::Relaxed) {
        response
            .headers_mut()
            .insert("connection", "close".parse().unwrap());
    }
    response
}

/// The route table — [`respond`] adds the TLS battery's connection close.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
async fn respond_route(
    method: &str,
    path: &str,
    query: &str,
    request_headers: &hyper::HeaderMap,
    request_body: Vec<u8>,
    gate: &std::sync::Arc<tokio::sync::Notify>,
) -> HyperResponse<BoxBody<Bytes, std::convert::Infallible>> {
    match path {
        // The request counter (v2/W3): the mock check's observable.
        "/request-count" => ok(
            StatusCode::OK,
            boxed(Full::new(Bytes::from(
                REQUESTS_HANDLED
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_string(),
            ))),
        ),
        "/plain" => ok(
            StatusCode::OK,
            boxed(Full::new(Bytes::from_static(b"plain"))),
        ),
        // The retry-policy target (v2/W5): fails with 500 the first n
        // visits of this exact path, then answers 200 — the battery's
        // flaky upstream. The visit count keys by the full path+query,
        // so each check arms its own counter.
        "/flaky" => {
            let key = if query.is_empty() {
                path.to_string()
            } else {
                format!("{path}?{query}")
            };
            let mut flaky = FLAKY_VISITS.lock().unwrap();
            let visits = flaky.entry(key).or_insert(0);
            *visits += 1;
            let fail_first: usize = query
                .split('=')
                .nth(1)
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            if *visits <= fail_first {
                ok(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    boxed(Full::new(Bytes::from_static(b"flaky"))),
                )
            } else {
                ok(
                    StatusCode::OK,
                    boxed(Full::new(Bytes::from_static(b"recovered"))),
                )
            }
        }
        // Always 500 — the policy's give-up path: the last response is
        // the fetch's result, not an error.
        "/always-500" => ok(
            StatusCode::INTERNAL_SERVER_ERROR,
            boxed(Full::new(Bytes::from_static(b"always broken"))),
        ),
        // The TLS battery server's leaf SPKI (base64) — 404 when no TLS
        // variant is running, i.e. from every in-process battery server.
        "/leaf-spki" => match TLS_LEAF_SPKI.get() {
            Some(hash) => ok(StatusCode::OK, boxed(Full::new(Bytes::from(hash.clone())))),
            None => ok(StatusCode::NOT_FOUND, boxed(Full::new(Bytes::new()))),
        },
        // The TLS battery's CA (base64 DER) — same 404 convention, same
        // purpose family: on-device configuration material the battery
        // fetches over plain http (configureTrust's roots).
        "/ca" => match TLS_CA.get() {
            Some(der) => ok(StatusCode::OK, boxed(Full::new(Bytes::from(der.clone())))),
            None => ok(StatusCode::NOT_FOUND, boxed(Full::new(Bytes::new()))),
        },
        // The observable for the JS GC check: how many WebSocket
        // connections are currently open on the battery server's
        // /gc-probe route (same process, other port — the count travels
        // via the static; see ws.rs). The JS battery watches a dropped
        // socket's connection close through this.
        "/ws-open" => ok(
            StatusCode::OK,
            boxed(Full::new(Bytes::from(
                crate::ws::GC_PROBE_OPEN
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_string(),
            ))),
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
        // The slow-consumer battery's body (v2/W2b): enough chunks, fast
        // enough, to fill a bounded response channel and engage the
        // pull gate while the JS consumer deliberately lags — 24 × 4KB,
        // one every 10ms (the check's consumer takes one per 60ms).
        "/backpressure" => {
            let stream = futures_lite::stream::iter(
                (0..24u8).map(|i| Ok(Frame::data(Bytes::from(vec![b'a' + (i % 26); 4096])))),
            )
            .then(|frame| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                frame
            });
            ok(
                StatusCode::OK,
                boxed(http_body_util::StreamBody::new(stream)),
            )
        }
        // The hook-retry target: 401 unless the request carries the
        // "refreshed" token, with an `x-retry` response header so a
        // response hook can key its decision on more than the status.
        // The JS check's flow: token1 → 401 → hook refreshes + retries →
        // token2 → 200.
        "/auth-guard" => {
            let authorized = request_headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value == "Bearer token2");
            if authorized {
                ok(StatusCode::OK, boxed(Full::new(Bytes::from_static(b"ok"))))
            } else {
                let mut response = ok(
                    StatusCode::UNAUTHORIZED,
                    boxed(Full::new(Bytes::from_static(b"stale token"))),
                );
                response
                    .headers_mut()
                    .insert("x-retry", "yes".parse().unwrap());
                response
            }
        }
        // The echo route answers with the request body so the check can
        // verify it survived the transport.
        "/echo" => {
            // Reflect what the server saw (method, headers, body) so
            // transports can be probed end-to-end without the observation
            // queue; the POST check still asserts via seen_for.
            let mut reflection = format!("{{\"method\":\"{method}\",\"headers\":[");
            let mut first = true;
            for (name, value) in request_headers {
                if !first {
                    reflection.push(',');
                }
                first = false;
                reflection.push_str(&format!(
                    "\"{}:{}\"",
                    name,
                    String::from_utf8_lossy(value.as_bytes())
                ));
            }
            reflection.push_str("],\"body\":\"");
            reflection.push_str(&json_escape(&String::from_utf8_lossy(&request_body)));
            reflection.push_str("\"}");
            ok(StatusCode::OK, boxed(Full::new(Bytes::from(reflection))))
        }
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
        // The concurrency-gate routes. `/fast` answers immediately and
        // releases `/held`'s tail; both exist only for the concurrency
        // checks, so their paths stay unique to those checks.
        "/fast" => {
            gate.notify_one();
            ok(
                StatusCode::OK,
                boxed(Full::new(Bytes::from_static(b"fast"))),
            )
        }
        // Head plus a big first chunk right away (big so transports that
        // coalesce small chunks still deliver it on its own), then the
        // tail held until `/fast` is seen — mid-body, held open.
        "/held" => {
            let gate = std::sync::Arc::clone(gate);
            let stream = futures_lite::stream::iter(0..2).then(move |i| {
                let gate = std::sync::Arc::clone(&gate);
                async move {
                    if i > 0 {
                        gate.notified().await;
                    }
                    Ok(Frame::data(Bytes::from(if i == 0 {
                        vec![b'a'; 512 * 1024]
                    } else {
                        b"omega".to_vec()
                    })))
                }
            });
            ok(
                StatusCode::OK,
                boxed(http_body_util::StreamBody::new(stream)),
            )
        }
        // Answers only after the whole upload arrived (every request body
        // is collected above before responding) — an upload endpoint whose
        // response itself proves the request finished. The mode header is
        // the wire-shape observable (v2/W2d): a body the transport
        // buffered carries Content-Length; a live stream went out chunked
        // (hyper consumes transfer-encoding, so the presence of
        // content-length is the discriminator).
        "/upload" => {
            let mut response = ok(
                StatusCode::OK,
                boxed(Full::new(Bytes::from_static(b"uploaded"))),
            );
            let mode = if request_headers.contains_key("content-length") {
                "length"
            } else {
                "chunked"
            };
            response
                .headers_mut()
                .insert("x-upload-mode", mode.parse().unwrap());
            response
        }
        _ => ok(StatusCode::NOT_FOUND, boxed(Full::new(Bytes::new()))),
    }
}

/// JSON-string escapes one string (the /echo reflection's body —
/// multipart and binary bodies carry CR/LF and quotes that a raw
/// interpolation would break).
fn json_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
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
    // /echo reflects the request as JSON (method, headers, body); the
    // observation queue below stays the authoritative header assertion.
    let reflection = String::from_utf8_lossy(&body.concat()).into_owned();
    assert!(
        reflection.contains(r#""method":"POST""#),
        "method not reflected: {reflection}"
    );
    assert!(
        reflection.contains("conformance"),
        "body not reflected: {reflection}"
    );

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

/// A per-request deadline shorter than the (unconfigured) default must
/// fire: `/slow-head` answers after 500ms, the request asks for 100ms.
/// Run this with a transport that has no timeout configured — the
/// request's own value is the only deadline in play.
pub fn per_request_timeout_shorter_wins(t: &dyn Transport, server: &HttpServer) {
    let request = get(server.url("/slow-head")).with_timeout_ms(100);
    let err = wait_for(
        "per-request timeout outcome",
        t.execute(request, CancelToken::new()),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

/// A per-request deadline longer than the configured one must win: the
/// transport is configured for 100ms, the request asks for 5000ms, and
/// `/slow-head` (answering after 500ms) resolves in full. Run this
/// with a timeout-configured instance (the same one
/// [`timeout_maps_to_transport_error`] uses).
pub fn per_request_timeout_longer_wins(t: &dyn Transport, server: &HttpServer) {
    let request = get(server.url("/slow-head")).with_timeout_ms(5000);
    let response = wait_for(
        "per-request extended deadline",
        t.execute(request, CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200, "the configured timeout fired instead");
    let body = wait_for(
        "late head body",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(body.concat(), b"late head".to_vec());
}

/// A per-request `follow_redirects: false` delivers the 3xx itself:
/// status 302 with the location header readable, no second hop.
pub fn per_request_no_follow_delivers_redirect(t: &dyn Transport, server: &HttpServer) {
    let request = get(server.url("/redirect")).with_follow_redirects(false);
    let response = wait_for(
        "no-follow redirect outcome",
        t.execute(request, CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 302, "the redirect was followed");
    let location = response
        .headers
        .get("location")
        .expect("the 3xx carries its location header");
    assert_eq!(location, "/redirect-final");
    // The redirect-final hop never happened.
    assert_eq!(server.seen_for("/redirect").method, "GET");
    let seen_final = server.seen.lock().unwrap();
    assert!(
        !seen_final
            .backlog
            .iter()
            .any(|seen| seen.uri == "/redirect-final"),
        "the no-follow request still made the second hop"
    );
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

/// While a download's body is held open (`/held`'s tail is released only
/// when `/fast` is seen), a second request must complete: an in-flight
/// transfer never blocks new requests from going out. If the transport
/// serialized requests, this check would time out — the held tail cannot
/// arrive before `/fast` is seen, and `/fast` cannot be seen before this
/// very request goes out.
pub fn held_body_does_not_block_new_requests(t: &dyn Transport, server: &HttpServer) {
    let held = wait_for(
        "held response head",
        t.execute(get(server.url("/held")), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(held.status, 200);
    let mut body = held.body;
    let first = wait_for("held first chunk", body.next()).unwrap().unwrap();
    assert!(!first.is_empty(), "no data arrived with the head");

    // While the tail is held open, a second request completes.
    let fast = wait_for(
        "fast response head",
        t.execute(get(server.url("/fast")), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(fast.status, 200);
    let fast_body = wait_for(
        "fast body",
        fast.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(fast_body.concat(), b"fast".to_vec());

    // The tail is released only by the `/fast` request above — it arrives
    // now and the held download completes.
    let rest = wait_for("held tail", body.map(|c| c.unwrap()).collect::<Vec<_>>());
    let all: Vec<u8> = first.iter().copied().chain(rest.concat()).collect();
    assert_eq!(all.len(), 512 * 1024 + b"omega".len());
    assert_eq!(&all[all.len() - 5..], b"omega");
}

/// While an upload is in flight — its body fed slowly, so the request
/// cannot possibly have finished — a second request must complete. The
/// streaming transports have the upload on the wire; the buffered-upload
/// transports hold the request while draining it; either way the
/// in-flight request must not block new ones. If the transport
/// serialized requests, this check would time out: the upload cannot
/// finish before the remaining chunks are fed, which happens only below.
pub fn slow_upload_does_not_block_new_requests(t: &dyn Transport, server: &HttpServer) {
    let (body, feed) = monet_core::body::channel();
    let mut request = Request::new(Method::Post, server.url("/upload"))
        .with_header("content-type", "application/octet-stream")
        .unwrap();
    request.body = body;
    feed.send_chunk("part-").unwrap();

    std::thread::scope(|scope| {
        let upload =
            scope.spawn(|| wait_for("upload outcome", t.execute(request, CancelToken::new())));
        // Let the request reach the wire (or the drain start).
        std::thread::sleep(Duration::from_millis(150));

        let plain = wait_for(
            "second response head",
            t.execute(get(server.url("/plain")), CancelToken::new()),
        )
        .unwrap();
        assert_eq!(plain.status, 200);
        let plain_body = wait_for(
            "second body",
            plain.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
        );
        assert_eq!(plain_body.concat(), b"plain".to_vec());

        feed.send_chunk("two").unwrap();
        feed.complete();
        let uploaded = upload.join().unwrap().unwrap();
        assert_eq!(uploaded.status, 200);
        let uploaded_body = wait_for(
            "uploaded body",
            uploaded.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
        );
        assert_eq!(uploaded_body.concat(), b"uploaded".to_vec());

        // The wire shape (v2/W2c/W2d): a live-streamed body goes out
        // chunked — no transport may buffer it into a Content-Length
        // send (that would mean collecting the whole stream first).
        let mode = uploaded
            .headers
            .get("x-upload-mode")
            .expect("the /upload route reports the wire mode");
        assert_eq!(mode, "chunked", "a streamed upload must not carry a length");
    });

    let seen = server.seen_for("/upload");
    assert_eq!(seen.method, "POST");
    assert_eq!(seen.body, b"part-two".to_vec());
}

/// The core battery: everything except the two checks that need their
/// own transport configuration (`connection_refused…` works on any
/// instance but takes no server; `timeout…` needs a timeout-configured
/// instance).
pub fn run_suite(t: &dyn Transport, server: &HttpServer) {
    per_request_timeout_shorter_wins(t, server);
    per_request_no_follow_delivers_redirect(t, server);
    get_plain_body(t, server);
    get_chunked_content(t, server);
    post_reaches_server(t, server);
    redirect_followed_by_default(t, server);
    pre_cancelled_fails_fast(t, server);
    cancel_after_head_aborts_body(t, server);
    held_body_does_not_block_new_requests(t, server);
    slow_upload_does_not_block_new_requests(t, server);
}

/// Convenience: spawn a server and run the core battery plus the
/// refused-connection check.
pub fn run(t: &dyn Transport) {
    let server = HttpServer::spawn();
    run_suite(t, &server);
    connection_refused_maps_to_transport_error(t);
}
