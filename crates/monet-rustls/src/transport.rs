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
}

impl Default for MonetRustlsConfig {
    fn default() -> Self {
        MonetRustlsConfig {
            trust: TlsTrust::default(),
            pins: PinConfig::default(),
            http2: true,
        }
    }
}

/// A `Transport` running hyper on its own tokio thread.
pub struct MonetRustlsTransport {
    handle: tokio::runtime::Handle,
    client: HttpClient,
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
            _shutdown,
        })
    }
}

impl Transport for MonetRustlsTransport {
    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>> {
        let handle = self.handle.clone();
        let client = self.client.clone();
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let (done_tx, done_rx) = oneshot::channel::<Result<Response>>();
            let spawn_handle = handle.clone();
            handle.spawn(async move {
                let _ = done_tx.send(run_execute(client, req, cancel, spawn_handle).await);
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

async fn run_execute(
    client: HttpClient,
    req: Request,
    cancel: CancelToken,
    handle: tokio::runtime::Handle,
) -> Result<Response> {
    let method = HyperMethod::from_bytes(req.method.as_str().as_bytes())
        .map_err(|e| Error::transport(format!("monet-rustls: method: {e}")))?;
    let uri: Uri = req
        .url
        .as_str()
        .parse()
        .map_err(|e| Error::transport(format!("monet-rustls: URI: {e}")))?;

    let mut builder = HyperRequest::builder().method(method).uri(uri);
    let headers = builder
        .headers_mut()
        .expect("fresh request builder has headers");
    for (name, value) in req.headers.iter_flattened() {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::transport(format!("monet-rustls: header name: {e}")))?;
        let value = HeaderValue::from_str(value)
            .map_err(|e| Error::transport(format!("monet-rustls: header value: {e}")))?;
        headers.append(name, value);
    }

    let request = builder
        .body(request_body(req.body))
        .map_err(|e| Error::transport(format!("monet-rustls: request build: {e}")))?;

    enum Step<T> {
        Done(T),
        Cancelled,
    }
    let response = match race(async { Step::Done(client.request(request).await) }, async {
        cancel.cancelled().await;
        Step::Cancelled
    })
    .await
    {
        Step::Done(Ok(response)) => response,
        Step::Done(Err(e)) => {
            return Err(Error::transport(format!(
                "monet-rustls: request failed: {e}"
            )));
        }
        Step::Cancelled => return Err(Error::Cancelled),
    };

    let mut headers_out = monet_core::HeaderMap::new();
    for (name, value) in response.headers().iter() {
        let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
        let _ = headers_out.append(name.as_str(), value);
    }
    let status = response.status().as_u16();

    let (body, sender) = body_channel();
    handle.spawn(pump_body(response.into_body(), sender, cancel));

    Ok(Response::new(status, headers_out, body))
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
