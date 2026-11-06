//! Monet — monet-core's Client behind a Rust TurboModule.
//!
//! This impl block IS the spec: the method table, auto-registration and
//! the TypeScript bindings (`generated/rust-turbo/Monet.ts`) are all
//! generated from it. The fetch/WebSocket surface follows the
//! callback-signal + zero-copy-pull protocol (see docs/plans/2026-11-monet-rn.md):
//! async methods can't carry JSI types (HRTB constraint) and callbacks
//! can't move big binaries, so streaming = callback signal + synchronous
//! zero-copy pull.
//!
//! JS usage:
//!   import { getMonet } from 'monet-rn/generated/rust-turbo/Monet';

use std::sync::Arc;
use std::sync::RwLock;

use jsi::RuntimeHandle;
use jsi_turbo::RustTurboModule;
use monet_core::Client;
use monet_core::Method;
use monet_core::Request;
use monet_core::pin::PinConfig;

use crate::fetch::FetchRegistry;
use crate::ws::WsCommand;
use crate::ws::WsRegistry;
use futures_lite::StreamExt as _;

#[cfg(target_os = "android")]
mod bootstrap;
mod fetch;
mod platform;
/// The WebSocket channel: the registry lands with the fetch channel so
/// the module's state story is complete; the ws_* methods and the pump
/// wiring arrive in M4 (fields read then).
/// The WebSocket registry: pure Rust (jsi + core types only), compiled
/// unconditionally — the transport selection in platform.rs is what the
/// `websocket` feature gates.
mod ws;

/// The module state: one Client over the platform-selected transport,
/// rebuilt when configuration changes, plus the fetch/socket registries
/// shared with the pump tasks.
pub struct Monet {
    client: RwLock<Arc<Client>>,
    fetches: FetchRegistry,
    sockets: WsRegistry,
}

impl Monet {
    pub fn create() -> anyhow::Result<Self> {
        Ok(Monet {
            client: RwLock::new(Arc::new(Client::new(platform::http_transport(
                &PinConfig::default(),
            )))),
            fetches: FetchRegistry::new(),
            sockets: WsRegistry::new(),
        })
    }

    /// Rebuilds the transport + client after a configuration change
    /// (pins). iOS pins live in the Swift shim (process-global,
    /// last-wins); Android pins go into the transport configs, so a
    /// rebuild re-installs them there.
    fn rebuild(&self, pins: &PinConfig) -> anyhow::Result<()> {
        let client = Client::new(platform::http_transport(pins));
        *self.client.write().unwrap() = Arc::new(client);
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, jsi_turbo::TurboStruct, Clone, Debug)]
pub struct HeaderKV {
    pub name: String,
    pub value: String,
}

#[derive(serde::Serialize, serde::Deserialize, jsi_turbo::TurboStruct, Debug)]
// The generated TypeScript bindings spell fields in camelCase
// (timeoutMs, followRedirects); the serde mapping must agree or every
// multi-word field arrives as an unknown key (whose IgnoredAny value
// would hit the deserializer's unsupported-type path).
#[serde(rename_all = "camelCase")]
pub struct FetchOptions {
    pub url: String,
    pub method: String,
    pub headers: Vec<HeaderKV>,
    /// v1 records these for forward compatibility; the runtime applies
    /// the client-level configuration only (per-request lands with the
    /// core-level timeout/redirect knob).
    pub timeout_ms: f64,
    pub follow_redirects: bool,
}

/// One head event as delivered to JS.
#[derive(serde::Serialize, Clone)]
struct HeadEvent {
    status: f64,
    headers: Vec<HeaderKV>,
}

/// One error event; `kind` mirrors the core error discriminant.
#[derive(serde::Serialize, Clone)]
struct ErrorEvent {
    kind: String,
    message: String,
}

/// One WS message signal: the type flag; the bytes follow through
/// `ws_take_message`.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct WsMessageEvent {
    is_binary: bool,
}

/// One WS close event — the code/reason fidelity the whole hand-rolled
/// framing exists to preserve.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct WsCloseEvent {
    code: f64,
    reason: String,
    was_clean: bool,
}

impl From<monet_core::Error> for ErrorEvent {
    fn from(error: monet_core::Error) -> Self {
        let kind = match &error {
            monet_core::Error::Cancelled => "Cancelled",
            monet_core::Error::Hook { .. } => "Hook",
            _ => "Transport",
        };
        ErrorEvent {
            kind: kind.to_string(),
            message: error.to_string(),
        }
    }
}

#[RustTurboModule("Monet", ctor = "Monet::create")]
impl Monet {
    /// Smoke check: the module is alive end to end.
    pub fn ping(&self, _rt: &mut RuntimeHandle) -> anyhow::Result<String> {
        Ok("monet-rn 0.1.0".into())
    }

    /// The native stack this build linked (per-platform cfg).
    #[turbo(getter)]
    pub fn get_transport_stack(&self) -> anyhow::Result<String> {
        Ok(platform::transport_stack().into())
    }

    // ---- configuration (client-level) ----

    /// Installs a pinning configuration (nitro-fetch-pinning.json format,
    /// fail-closed: a malformed config is an error, never a silent no-op).
    pub fn configure_pins(&self, json: String) -> anyhow::Result<()> {
        let pins = PinConfig::from_json(&json)
            .map_err(|e| anyhow::anyhow!("invalid pinning config: {e}"))?;
        self.rebuild(&pins)
    }

    pub fn clear_pins(&self) -> anyhow::Result<()> {
        self.rebuild(&PinConfig::default())
    }

    // ---- fetch: head callback first, body pulled in chunks ----

    /// Starts a fetch. The four callbacks fire from the pump task
    /// (marshalled back to the JS thread by the TurboCallback
    /// dispatcher): `on_head` once with status + headers, `on_chunk` as
    /// a signal that a chunk is queued (JS then calls `take_chunk`),
    /// then exactly one of `on_done` / `on_error`.
    pub fn fetch<'rt>(
        &self,
        rt: &mut RuntimeHandle<'rt>,
        options: FetchOptions,
        body: Option<jsi::JsiArrayBuffer<'rt>>,
        on_head: jsi_turbo::TurboCallback,
        on_chunk: jsi_turbo::TurboCallback,
        on_done: jsi_turbo::TurboCallback,
        on_error: jsi_turbo::TurboCallback,
    ) -> anyhow::Result<f64> {
        // `FromStr::Err` is `Infallible` — the match is exhaustive by
        // construction; unknown verbs land in `Method::Other`.
        let method = options.method.parse::<Method>().unwrap_or_else(|_| unreachable!());
        let url = options
            .url
            .parse::<monet_core::Url>()
            .map_err(|e| anyhow::anyhow!("invalid URL: {e}"))?;

        let mut request = Request::new(method, url);
        for header in &options.headers {
            request
                .headers
                .append(&header.name, &header.value)
                .map_err(|e| anyhow::anyhow!("invalid header: {e}"))?;
        }
        if let Some(buffer) = &body {
            // The upload is fully buffered in v1 (nitro parity): the JS
            // ArrayBuffer is copied here — the borrow ends with the call.
            request.body = monet_core::body::Body::from_bytes(buffer.data(rt).to_vec());
        }

        let (fetch_id, cancel) = self.fetches.allocate();
        let fetches = self.fetches.clone();
        let on_head = Arc::new(on_head);
        let on_chunk = Arc::new(on_chunk);
        let on_done = Arc::new(on_done);
        let on_error = Arc::new(on_error);

        // The transport future runs detached on the smol global executor
        // (every monet transport is a runtime-agnostic BoxFuture). The
        // client is only needed for hook plumbing (empty in v1), so the
        // pump drives the transport directly — cancellation stays
        // transport-level, which the Client pipeline doesn't expose.
        use monet_core::Transport as _;
        let transport = platform::http_transport(&PinConfig::default());
        let execute = transport.execute(request, cancel);
        async_global_executor::spawn(async move {
            match execute.await {
                Ok(response) => {
                    let status = f64::from(response.status);
                    let headers = response
                        .headers
                        .iter_flattened()
                        .map(|(name, value)| HeaderKV {
                            name: name.to_string(),
                            value: value.to_string(),
                        })
                        .collect();
                    on_head.call(HeadEvent { status, headers });

                    use futures_lite::StreamExt;
                    let mut body = response.body;
                    let mut failure: Option<monet_core::Error> = None;
                    while let Some(chunk) = body.next().await {
                        match chunk {
                            Ok(bytes) => {
                                if fetches.push_chunk(fetch_id, bytes.to_vec()) {
                                    on_chunk.call_with_no_args();
                                } else {
                                    // State gone (cleanup after terminal):
                                    // stop pulling.
                                    return;
                                }
                            }
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        }
                    }
                    fetches.finish(fetch_id);
                    match failure {
                        None => on_done.call_with_no_args(),
                        Some(error) => on_error.call(ErrorEvent::from(error)),
                    }
                }
                Err(error) => {
                    fetches.finish(fetch_id);
                    on_error.call(ErrorEvent::from(error));
                }
            }
        })
        .detach();

        Ok(fetch_id)
    }

    /// Takes one queued chunk (zero-copy into a JS ArrayBuffer).
    /// `None` means the queue is empty — check `body_done` to tell "no
    /// data yet" from "finished and drained".
    pub fn take_chunk<'rt>(
        &self,
        rt: &mut RuntimeHandle<'rt>,
        fetch_id: f64,
    ) -> anyhow::Result<Option<jsi::JsiArrayBuffer<'rt>>> {
        Ok(self.fetches.take_chunk(fetch_id, rt))
    }

    /// `true` when the fetch has terminated (success or error) and the
    /// chunk queue is drained.
    pub fn body_done(&self, fetch_id: f64) -> anyhow::Result<bool> {
        Ok(self.fetches.is_done(fetch_id))
    }

    /// Drops a finished fetch's state (JS signals it's done pulling).
    pub fn cleanup_fetch(&self, fetch_id: f64) -> anyhow::Result<()> {
        self.fetches.cleanup(fetch_id);
        Ok(())
    }

    /// Aborts a fetch: cancels the transport-level token; the pump sees
    /// the error and reports `on_error` with kind Cancelled.
    pub fn abort(&self, fetch_id: f64) -> anyhow::Result<bool> {
        Ok(self.fetches.abort(fetch_id))
    }

    // ---- WebSocket: open callback first, messages pulled in chunks ----

    /// Opens a WebSocket. `on_open` fires when the handshake completes;
    /// then `on_message` signals queued data (JS pulls with
    /// `ws_take_message`, the event carries `{isBinary}`), and exactly
    /// one of `on_close` / `on_error` terminates. The close event
    /// carries `{code, reason, wasClean}` — the fidelity the
    /// URLSession-based iOS transport cannot provide.
    pub fn ws_connect(
        &self,
        url: String,
        protocols: Vec<String>,
        headers: Vec<HeaderKV>,
        on_open: jsi_turbo::TurboCallback,
        on_message: jsi_turbo::TurboCallback,
        on_close: jsi_turbo::TurboCallback,
        on_error: jsi_turbo::TurboCallback,
    ) -> anyhow::Result<f64> {
        let url = url
            .parse::<monet_core::Url>()
            .map_err(|e| anyhow::anyhow!("invalid URL: {e}"))?;
        let mut request = Request::new(Method::Get, url);
        // Subprotocols ride as a header (monet-ws semantics); the server
        // echoes the negotiated one, which strict clients check.
        if !protocols.is_empty() {
            request
                .headers
                .append("sec-websocket-protocol", &protocols.join(", "))?;
        }
        for header in &headers {
            request
                .headers
                .append(&header.name, &header.value)
                .map_err(|e| anyhow::anyhow!("invalid header: {e}"))?;
        }

        let (conn_id, cancel, command_rx) = self.sockets.allocate();
        let sockets = self.sockets.clone();
        let on_open = Arc::new(on_open);
        let on_message = Arc::new(on_message);
        let on_close = Arc::new(on_close);
        let on_error = Arc::new(on_error);

        let transport = platform::ws_transport(&PinConfig::default())?;
        let connect = transport.connect(request, cancel);
        async_global_executor::spawn(async move {
            // Hold the transport for the connection's lifetime: its
            // private runtime drives both the connect future and the
            // socket pump — dropping it would tear them down.
            let _keep_alive = transport;
            match connect.await {
                Ok(mut stream) => {
                    on_open.call_with_no_args();
                    ws_pump(
                        conn_id,
                        &mut stream,
                        command_rx,
                        &sockets,
                        &on_message,
                        &on_close,
                        &on_error,
                    )
                    .await;
                }
                Err(error) => {
                    sockets.finish(conn_id);
                    on_error.call(ErrorEvent::from(error));
                }
            }
        })
        .detach();

        Ok(conn_id)
    }

    /// Takes one queued WS message as a zero-copy ArrayBuffer. The
    /// isBinary flag arrived on the `on_message` signal that preceded
    /// this pull — the signal/data pairs are strictly FIFO, so the TS
    /// layer pairs them by order (same shape as the fetch channel's
    /// on_chunk/take_chunk).
    pub fn ws_take_message<'rt>(
        &self,
        rt: &mut RuntimeHandle<'rt>,
        conn_id: f64,
    ) -> anyhow::Result<Option<jsi::JsiArrayBuffer<'rt>>> {
        Ok(self.sockets.take_message(conn_id, rt).map(|(buffer, _)| buffer))
    }

    /// Sends a text (`is_binary == false`) or binary message.
    pub fn ws_send<'rt>(
        &self,
        _rt: &mut RuntimeHandle<'rt>,
        conn_id: f64,
        message: jsi::JsiArrayBuffer<'rt>,
        is_binary: bool,
    ) -> anyhow::Result<bool> {
        // The upload is copied here (v1 parity with fetch): the borrow
        // ends with the call.
        let data = message.data(_rt).to_vec();
        Ok(self.sockets.send_command(
            conn_id,
            WsCommand::Send { data, is_binary },
        ))
    }

    /// Sends a ping (the transport answers received pings by itself).
    pub fn ws_send_ping(&self, conn_id: f64) -> anyhow::Result<bool> {
        Ok(self.sockets.send_command(
            conn_id,
            WsCommand::Send {
                data: Vec::new(),
                is_binary: true,
            },
        ))
    }

    /// Starts the close handshake with this code and reason; the peer's
    /// answering close frame fires `on_close`.
    pub fn ws_close(&self, conn_id: f64, code: f64, reason: String) -> anyhow::Result<bool> {
        let code = u16::try_from(code as u64).unwrap_or(1000);
        Ok(self
            .sockets
            .send_command(conn_id, WsCommand::Close { code, reason }))
    }

    /// Aborts a connection: cancels the transport-level token.
    pub fn ws_abort(&self, conn_id: f64) -> anyhow::Result<bool> {
        Ok(self.sockets.abort(conn_id))
    }
}

/// The WS pump: delivers received messages into the queue with signals,
/// applies user commands to the stream, and terminates exactly once.
async fn ws_pump(
    conn_id: f64,
    stream: &mut monet_core::ws::WebSocketStream,
    mut command_rx: futures_channel::mpsc::UnboundedReceiver<WsCommand>,
    sockets: &crate::ws::WsRegistry,
    on_message: &jsi_turbo::TurboCallback,
    on_close: &jsi_turbo::TurboCallback,
    on_error: &jsi_turbo::TurboCallback,
) {
    use futures_lite::StreamExt as _;
    use monet_core::ws::WsMessage;
    loop {
        // Two-way select: a received message or a user command. The
        // cancel token lives with the transport (handed over in
        // `connect`): firing it drops the transport pump, which ends
        // the stream — `Ok(None)` below carries the abnormal close.
        enum Step {
            Message(monet_core::Result<Option<WsMessage>>),
            Command(Option<WsCommand>),
        }
        let step = futures_lite::future::race(
            async { Step::Message(stream.next_message().await) },
            async { Step::Command(command_rx.next().await) },
        )
        .await;
        match step {
            Step::Message(Ok(Some(WsMessage::Text(text)))) => {
                if sockets.push_message(conn_id, text.into_bytes(), false) {
                    on_message.call(WsMessageEvent { is_binary: false });
                } else {
                    return;
                }
            }
            Step::Message(Ok(Some(WsMessage::Binary(bytes)))) => {
                if sockets.push_message(conn_id, bytes.to_vec(), true) {
                    on_message.call(WsMessageEvent { is_binary: true });
                } else {
                    return;
                }
            }
            Step::Message(Ok(Some(WsMessage::Ping(bytes)))) => {
                let _ = bytes;
                // Surfaced for tests/custom protocols; the transport
                // already answered.
                let _ = sockets.push_message(conn_id, Vec::new(), true);
                on_message.call(WsMessageEvent { is_binary: true });
            }
            Step::Message(Ok(Some(WsMessage::Pong(_)))) => {
                // Fire-and-forget: surfaced like a message for symmetry.
                let _ = sockets.push_message(conn_id, Vec::new(), true);
                on_message.call(WsMessageEvent { is_binary: true });
            }
            Step::Message(Ok(Some(WsMessage::Close(frame)))) => {
                sockets.finish(conn_id);
                match frame {
                    Some(frame) => on_close.call(WsCloseEvent {
                        code: f64::from(frame.code),
                        reason: frame.reason,
                        was_clean: true,
                    }),
                    None => on_close.call(WsCloseEvent {
                        code: 1006.0,
                        reason: String::new(),
                        was_clean: false,
                    }),
                }
                return;
            }
            Step::Message(Ok(None)) => {
                // Stream ended without a close frame: abnormal shutdown.
                sockets.finish(conn_id);
                on_close.call(WsCloseEvent {
                    code: 1006.0,
                    reason: String::new(),
                    was_clean: false,
                });
                return;
            }
            Step::Message(Err(error)) => {
                sockets.finish(conn_id);
                on_error.call(ErrorEvent::from(error));
                return;
            }
            Step::Command(Some(WsCommand::Send { data, is_binary })) => {
                let message = if is_binary {
                    WsMessage::Binary(bytes::Bytes::from(data))
                } else {
                    WsMessage::Text(String::from_utf8_lossy(&data).into_owned())
                };
                if stream.send(message).is_err() {
                    sockets.finish(conn_id);
                    on_error.call(ErrorEvent::from(monet_core::Error::Body(
                        "websocket driver dropped".into(),
                    )));
                    return;
                }
            }
            Step::Command(Some(WsCommand::Close { code, reason })) => {
                if stream.close(code, reason).is_err() {
                    sockets.finish(conn_id);
                    on_error.call(ErrorEvent::from(monet_core::Error::Body(
                        "websocket driver dropped".into(),
                    )));
                    return;
                }
                // The close frame is on the wire; the peer's answer
                // arrives as a Close message above.
            }
            Step::Command(None) => {
                // The command channel closed (state cleaned up after a
                // terminal event raced us): nothing left to do.
                return;
            }
        }
    }
}
