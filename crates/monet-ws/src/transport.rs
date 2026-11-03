//! [`WebSocketTransport`] over tokio-tungstenite + rustls.
//!
//! The crate owns a dedicated single-threaded tokio runtime (one parked
//! thread); every connection and its pump task run there while the public
//! API stays runtime-agnostic — callers poll plain futures and the
//! channel-based [`WebSocketStream`], never tokio types.

use std::sync::mpsc as std_mpsc;

use core::pin::Pin;
use futures_channel::oneshot;
use futures_lite::StreamExt;
use futures_lite::future::race;
use futures_util::SinkExt as _;
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::pin::PinConfig;
use monet_core::request::Request;
use monet_core::transport::{BoxFuture, WebSocketTransport as WsTransportTrait};
use monet_core::ws::{WebSocketStream, WsMessage, channel as ws_channel};
use tokio_tungstenite::WebSocketStream as TungsteniteWs;
use tokio_tungstenite::tungstenite::Message as Tm;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{Connector, connect_async_tls_with_config};

use crate::verifier::{TlsTrust, client_config};

/// Configuration for [`MonetWsTransport`].
#[derive(Clone, Default)]
pub struct WsConfig {
    /// Trust source for TLS. Defaults to the embedded Mozilla bundle.
    pub trust: TlsTrust,
    /// Public-key pins; pinned hosts additionally require a matching SPKI
    /// hash in the presented chain (fail-closed, matching monet-core's
    /// walk-up semantics).
    pub pins: PinConfig,
}

/// A `WebSocketTransport` running on its own tokio thread.
pub struct MonetWsTransport {
    handle: tokio::runtime::Handle,
    config: WsConfig,
    // Dropping this unparks the runtime thread, shutting it down.
    _shutdown: oneshot::Sender<()>,
}

impl MonetWsTransport {
    /// Starts the runtime thread with `config`.
    pub fn new(config: WsConfig) -> Result<Self> {
        let (handle_tx, handle_rx) = std_mpsc::channel::<Result<tokio::runtime::Handle>>();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        std::thread::Builder::new()
            .name("monet-ws".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = handle_tx.send(Err(Error::transport(format!(
                            "monet-ws: runtime build failed: {e}"
                        ))));
                        return;
                    }
                };
                let _ = handle_tx.send(Ok(runtime.handle().clone()));
                // Park until the transport is dropped, driving spawned tasks.
                runtime.block_on(async {
                    let _ = shutdown_rx.await;
                });
            })
            .map_err(|e| Error::transport(format!("monet-ws: spawn failed: {e}")))?;

        let handle = handle_rx
            .recv()
            .map_err(|_| Error::transport("monet-ws: runtime thread died"))??;

        // Build the rustls config now so trust/pin misconfiguration
        // surfaces at construction, not on first connect.
        client_config(&config.trust, &config.pins)?;

        Ok(MonetWsTransport {
            handle,
            config,
            _shutdown: shutdown_tx,
        })
    }
}

impl WsTransportTrait for MonetWsTransport {
    fn connect(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<WebSocketStream>> {
        let handle = self.handle.clone();
        let config = self.config.clone();
        Box::pin(async move {
            let (done_tx, done_rx) = oneshot::channel::<Result<WebSocketStream>>();
            let spawn_handle = handle.clone();
            handle.spawn(async move {
                let _ = done_tx.send(run_connect(req, cancel, config, spawn_handle).await);
            });
            done_rx
                .await
                .map_err(|_| Error::transport("monet-ws: runtime dropped during connect"))?
        })
    }
}

async fn run_connect(
    req: Request,
    cancel: CancelToken,
    config: WsConfig,
    handle: tokio::runtime::Handle,
) -> Result<WebSocketStream> {
    // The request URL is the ws:// or wss:// endpoint; its headers ride
    // along on the upgrade request.
    let mut request = req
        .url
        .to_string()
        .into_client_request()
        .map_err(|e| Error::transport(format!("monet-ws: invalid websocket URL: {e}")))?;
    for (name, value) in req.headers.iter_flattened() {
        use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::transport(format!("monet-ws: header name: {e}")))?;
        let value = HeaderValue::from_str(value)
            .map_err(|e| Error::transport(format!("monet-ws: header value: {e}")))?;
        request.headers_mut().append(name, value);
    }

    let connector = match req.url.scheme() {
        "wss" | "https" => Some(Connector::Rustls(client_config(
            &config.trust,
            &config.pins,
        )?)),
        _ => None,
    };

    enum Step<T> {
        Connected(T),
        Cancelled,
    }
    let connect_fut = connect_async_tls_with_config(request, None, false, connector);
    let (ws, _response) = match race(async { Step::Connected(connect_fut.await) }, async {
        cancel.cancelled().await;
        Step::Cancelled
    })
    .await
    {
        Step::Connected(Ok(pair)) => pair,
        Step::Connected(Err(e)) => {
            return Err(Error::transport(format!("monet-ws: handshake failed: {e}")));
        }
        Step::Cancelled => return Err(Error::Cancelled),
    };

    let (stream, driver) = ws_channel();
    handle.spawn(pump(
        ws,
        driver.user_to_network,
        driver.network_to_user,
        cancel,
    ));
    Ok(stream)
}

/// Bridges the tokio websocket and the user's futures-channel halves until
/// either side closes or the request is cancelled.
async fn pump<S>(
    mut ws: TungsteniteWs<S>,
    mut user_to_network: futures_channel::mpsc::UnboundedReceiver<WsMessage>,
    network_to_user: futures_channel::mpsc::UnboundedSender<WsMessage>,
    cancel: CancelToken,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    enum Event {
        User(Option<WsMessage>),
        Net(Option<tokio_tungstenite::tungstenite::Result<Tm>>),
        Cancelled,
    }
    loop {
        let event = race(
            race(async { Event::User(user_to_network.next().await) }, async {
                Event::Net(ws.next().await)
            }),
            async {
                cancel.cancelled().await;
                Event::Cancelled
            },
        )
        .await;
        match event {
            // The user dropped the stream: close the socket politely.
            Event::User(None) | Event::Cancelled => {
                let _ = ws.close(None).await;
                break;
            }
            Event::User(Some(message)) => {
                if ws.send(to_tungstenite(message)).await.is_err() {
                    break;
                }
            }
            // The server closed (or errored): dropping network_to_user ends
            // the user's stream with a final Close message if one arrived.
            Event::Net(None) | Event::Net(Some(Err(_))) => break,
            Event::Net(Some(Ok(message))) => {
                let _ = network_to_user.unbounded_send(to_ws(message));
            }
        }
    }
    drop(network_to_user);
}

/// monet-core message → tungstenite message.
pub(crate) fn to_tungstenite(message: WsMessage) -> Tm {
    match message {
        WsMessage::Text(text) => Tm::Text(text.into()),
        WsMessage::Binary(bytes) => Tm::Binary(bytes),
        WsMessage::Ping(bytes) => Tm::Ping(bytes),
        WsMessage::Pong(bytes) => Tm::Pong(bytes),
        WsMessage::Close(frame) => {
            Tm::Close(
                frame.map(|f| tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: f.code.into(),
                    reason: f.reason.into(),
                }),
            )
        }
    }
}

/// tungstenite message → monet-core message.
pub(crate) fn to_ws(message: Tm) -> WsMessage {
    match message {
        Tm::Text(text) => WsMessage::Text(text.as_str().to_owned()),
        Tm::Binary(bytes) => WsMessage::Binary(bytes),
        Tm::Ping(bytes) => WsMessage::Ping(bytes),
        Tm::Pong(bytes) => WsMessage::Pong(bytes),
        Tm::Close(frame) => WsMessage::Close(frame.map(|f| WsClose {
            code: f.code.into(),
            reason: f.reason.as_str().to_owned(),
        })),
        Tm::Frame(frame) => WsMessage::Binary(bytes::Bytes::copy_from_slice(frame.payload())),
    }
}

// WsMessage's CloseFrame vs tungstenite's CloseFrame share a name.
use monet_core::ws::CloseFrame as WsClose;

// Keep Pin referenced for the pump future's invariance needs.
#[allow(dead_code)]
type _Pinned<T> = Pin<Box<T>>;
