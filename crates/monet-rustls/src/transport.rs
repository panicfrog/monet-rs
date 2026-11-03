//! [`Transport`] over hyper + rustls.

use std::sync::Arc;
use std::sync::mpsc as std_mpsc;

use futures_channel::oneshot;
use futures_lite::StreamExt;
use futures_lite::future::race;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Method as HyperMethod, Request as HyperRequest, Uri};
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use monet_core::body::{Body, BodySender, channel as body_channel};
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::pin::PinConfig;
use monet_core::request::Request;
use monet_core::response::Response;
use monet_core::transport::{BoxFuture, Transport};
use monet_tls::{TlsTrust, client_config};

type HttpConnector = hyper_util::client::legacy::connect::HttpConnector;
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxRequestBody = BoxBody<bytes::Bytes, BoxError>;
type HttpClient = Client<HttpsConnector<HttpConnector>, BoxRequestBody>;

/// Configuration for [`MonetRustlsTransport`].
#[derive(Clone)]
pub struct MonetRustlsConfig {
    /// Trust source for TLS. Defaults to the embedded Mozilla bundle.
    pub trust: TlsTrust,
    /// Public-key pins; pinned hosts additionally require a matching SPKI
    /// hash in the presented chain.
    pub pins: PinConfig,
    /// Advertise HTTP/2 over ALPN. Enabled by default.
    pub http2: bool,
    /// Deadline for receiving the response head, in milliseconds (the
    /// URLSession/iOS transport's `timeoutInterval` equivalent). `None`
    /// waits indefinitely, like hyper itself.
    pub timeout_ms: Option<u64>,
}

impl Default for MonetRustlsConfig {
    fn default() -> Self {
        MonetRustlsConfig {
            trust: TlsTrust::default(),
            pins: PinConfig::default(),
            http2: true,
            timeout_ms: None,
        }
    }
}

/// A `Transport` running hyper on its own tokio thread.
pub struct MonetRustlsTransport {
    handle: tokio::runtime::Handle,
    client: HttpClient,
    timeout_ms: Option<u64>,
    // Dropping this unparks the runtime thread, shutting it down.
    _shutdown: oneshot::Sender<()>,
}

impl MonetRustlsTransport {
    /// Starts the runtime thread and builds the client with `config`.
    pub fn new(config: MonetRustlsConfig) -> Result<Self> {
        let (handle, _shutdown) = spawn_runtime()?;

        let tls = client_config(&config.trust, &config.pins)?;
        let tls = Arc::try_unwrap(tls).unwrap_or_else(|arc| (*arc).clone());
        let mut connector = HttpConnector::new();
        connector.enforce_http(false); // https URIs go through the https connector
        // The connector builder is typestated; both arms collapse to the
        // same concrete connector at wrap_connector.
        let https = if config.http2 {
            HttpsConnector::<HttpConnector>::builder()
                .with_tls_config(tls)
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .wrap_connector(connector)
        } else {
            HttpsConnector::<HttpConnector>::builder()
                .with_tls_config(tls)
                .https_or_http()
                .enable_http1()
                .wrap_connector(connector)
        };

        let mut builder = Client::builder(TokioExecutor::new());
        if config.http2 {
            builder.http2_adaptive_window(true);
        }
        let client = builder.build(https);

        Ok(MonetRustlsTransport {
            handle,
            client,
            timeout_ms: config.timeout_ms,
            _shutdown,
        })
    }
}

impl Transport for MonetRustlsTransport {
    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>> {
        let handle = self.handle.clone();
        let client = self.client.clone();
        let timeout_ms = self.timeout_ms;
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let (done_tx, done_rx) = oneshot::channel::<Result<Response>>();
            let spawn_handle = handle.clone();
            handle.spawn(async move {
                let _ =
                    done_tx.send(run_execute(client, req, cancel, spawn_handle, timeout_ms).await);
            });
            done_rx
                .await
                .map_err(|_| Error::transport("monet-rustls: runtime dropped during execute"))?
        })
    }
}

/// Boots the private single-threaded runtime; the returned sender parks the
/// thread until dropped. (Same pattern as monet-ws; extract into a shared
/// helper if a third consumer appears.)
fn spawn_runtime() -> Result<(tokio::runtime::Handle, oneshot::Sender<()>)> {
    let (handle_tx, handle_rx) = std_mpsc::channel::<Result<tokio::runtime::Handle>>();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    std::thread::Builder::new()
        .name("monet-rustls".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = handle_tx.send(Err(Error::transport(format!(
                        "monet-rustls: runtime build failed: {e}"
                    ))));
                    return;
                }
            };
            let _ = handle_tx.send(Ok(runtime.handle().clone()));
            runtime.block_on(async {
                let _ = shutdown_rx.await;
            });
        })
        .map_err(|e| Error::transport(format!("monet-rustls: spawn failed: {e}")))?;
    let handle = handle_rx
        .recv()
        .map_err(|_| Error::transport("monet-rustls: runtime thread died"))??;
    Ok((handle, shutdown_tx))
}

/// Upper bound on redirect hops (nitro-fetch's policy).
const MAX_REDIRECTS: usize = 20;

async fn run_execute(
    client: HttpClient,
    req: Request,
    cancel: CancelToken,
    handle: tokio::runtime::Handle,
    timeout_ms: Option<u64>,
) -> Result<Response> {
    let mut url = req.url;
    let mut method = req.method;
    // The stream can only be sent once: hop 0 gets it, later hops are
    // bodyless (see the 307/308 note below).
    let mut body = Some(req.body);
    let mut headers = req.headers;

    for _ in 0..=MAX_REDIRECTS {
        let hyper_method = HyperMethod::from_bytes(method.as_str().as_bytes())
            .map_err(|e| Error::transport(format!("monet-rustls: method: {e}")))?;
        let uri: Uri = url
            .as_str()
            .parse()
            .map_err(|e| Error::transport(format!("monet-rustls: URI: {e}")))?;

        let mut builder = HyperRequest::builder().method(hyper_method).uri(uri);
        let builder_headers = builder
            .headers_mut()
            .expect("fresh request builder has headers");
        for (name, value) in headers.iter_flattened() {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| Error::transport(format!("monet-rustls: header name: {e}")))?;
            let value = HeaderValue::from_str(value)
                .map_err(|e| Error::transport(format!("monet-rustls: header value: {e}")))?;
            builder_headers.append(name, value);
        }

        let stream = body.take().unwrap_or_else(Body::empty);
        let request = builder
            .body(request_body(stream))
            .map_err(|e| Error::transport(format!("monet-rustls: request build: {e}")))?;

        let response = send_once(&client, request, &cancel, timeout_ms).await?;

        // Redirect following (WHATWG-flavored, like the other transports):
        // 301/302/303 switch to GET, dropping the body; 307/308 preserve
        // the method but a consumed stream cannot be replayed, so bodied
        // methods get the 3xx response delivered instead of a doomed
        // retry. Authorization never crosses an origin change.
        let status = response.status();
        if ![301u16, 302, 303, 307, 308].contains(&status.as_u16()) {
            return Ok(into_response(response, &handle, cancel));
        }
        let Some(location) = response
            .headers()
            .get("location")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
        else {
            return Ok(into_response(response, &handle, cancel));
        };
        let next = url.join(&location).map_err(|e| {
            Error::transport(format!("monet-rustls: redirect location {location:?}: {e}"))
        })?;
        let cross_origin = next.host() != url.host()
            || next.port_or_known_default() != url.port_or_known_default()
            || next.scheme() != url.scheme();
        if cross_origin {
            headers.remove("authorization");
        }
        let preserve_method = status.as_u16() == 307 || status.as_u16() == 308;
        if preserve_method
            && method != monet_core::Method::Get
            && method != monet_core::Method::Head
        {
            // The body (if any) was already consumed; deliver as-is.
            return Ok(into_response(response, &handle, cancel));
        }
        if !preserve_method
            && method != monet_core::Method::Get
            && method != monet_core::Method::Head
        {
            // 301/302/303: fetch semantics switch bodied methods to GET.
            method = monet_core::Method::Get;
        }
        url = next;
    }
    Err(Error::transport(
        "monet-rustls: too many redirects (> {MAX_REDIRECTS})",
    ))
}

/// One request/response exchange: cancel race plus the optional head
/// timeout.
async fn send_once(
    client: &HttpClient,
    request: HyperRequest<BoxRequestBody>,
    cancel: &CancelToken,
    timeout_ms: Option<u64>,
) -> Result<hyper::Response<Incoming>> {
    enum Step<T> {
        Done(T),
        Cancelled,
        TimedOut,
    }
    let with_deadline = async {
        match timeout_ms {
            Some(ms) => {
                match tokio::time::timeout(std::time::Duration::from_millis(ms), async {
                    Step::Done(client.request(request).await)
                })
                .await
                {
                    Ok(step) => step,
                    Err(_elapsed) => Step::TimedOut,
                }
            }
            None => Step::Done(client.request(request).await),
        }
    };
    match race(with_deadline, async {
        cancel.cancelled().await;
        Step::Cancelled
    })
    .await
    {
        Step::Done(Ok(response)) => Ok(response),
        Step::Done(Err(e)) => Err(Error::transport(format!(
            "monet-rustls: request failed: {}",
            error_chain(&e)
        ))),
        Step::TimedOut => Err(Error::transport("monet-rustls: request timed out")),
        Step::Cancelled => Err(Error::Cancelled),
    }
}

/// Flattens an error and its whole source chain into one message —
/// hyper-util's top-level Display ("client error (Connect)") hides the
/// rustls reason (including the pin-rejection marker), which callers
/// branch on.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}

/// Maps a hyper response onto the core shape and starts the body pump.
fn into_response(
    response: hyper::Response<Incoming>,
    handle: &tokio::runtime::Handle,
    cancel: CancelToken,
) -> Response {
    let mut headers_out = monet_core::HeaderMap::new();
    for (name, value) in response.headers().iter() {
        let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        let _ = headers_out.append(name.as_str(), value);
    }
    let status = response.status().as_u16();

    let (body, sender) = body_channel();
    handle.spawn(pump_body(response.into_body(), sender, cancel));
    Response::new(status, headers_out, body)
}

/// Bridges our streaming request body into a hyper body: each chunk becomes
/// a data frame, so uploads stream (unlike monet-cronet's buffered v1). An
/// exhausted channel yields an empty body.
fn request_body(body: Body) -> BoxRequestBody {
    let stream = body.map(|chunk| chunk.map(Frame::data).map_err(|e| Box::new(e) as BoxError));
    <StreamBody<_> as BodyExt>::boxed(StreamBody::new(stream))
}

/// Pumps the hyper response body into our channel, chunk by chunk; the
/// cancel token aborts the stream mid-body.
async fn pump_body(mut incoming: Incoming, sender: BodySender, cancel: CancelToken) {
    enum Step<T> {
        Frame(Option<Result<T, hyper::Error>>),
        Cancelled,
    }
    loop {
        let frame = match race(async { Step::Frame(incoming.frame().await) }, async {
            cancel.cancelled().await;
            Step::Cancelled
        })
        .await
        {
            Step::Frame(frame) => frame,
            Step::Cancelled => {
                sender.abort(Error::Cancelled);
                return;
            }
        };
        match frame {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data()
                    && sender.send_chunk(data).is_err()
                {
                    return; // consumer dropped the response body
                }
                // trailers are ignored in v1
            }
            Some(Err(e)) => {
                sender.abort(Error::transport(format!("monet-rustls: body failed: {e}")));
                return;
            }
            None => break,
        }
    }
    sender.complete();
}
