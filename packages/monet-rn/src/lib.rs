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
use monet_core::pin::PinConfig;
use monet_core::Client;
use monet_core::Method;
use monet_core::Request;

use crate::fetch::FetchRegistry;
use crate::hooks::Pending;
use crate::ws::MonetWsConnection;
use crate::ws::WsCommand;
use crate::ws::WsQueue;
use monet_core::HookRegistry;

#[cfg(target_os = "android")]
mod bootstrap;
mod fetch;
/// The JS hook/observer channel: TurboCallbacks adapted onto core's hook
/// traits, with the pending-map resolve protocol (see docs/plans/2026-11-hooks.md).
mod hooks;
mod platform;
/// The WebSocket connection object: pure Rust (jsi + core types only),
/// compiled unconditionally — the transport selection in platform.rs is
/// what the `websocket` feature gates. The connection's lifetime is the
/// JS object's (NativeState): drop = disconnect.
mod ws;

/// The hard cap on hook-driven retries per fetch: a buggy response hook
/// that always retries must fail the fetch, not loop forever.
const MAX_HOOK_RETRIES: u32 = 3;

/// The module state: one shared transport (behind the client for future
/// hook plumbing, and directly for the fetch pumps — one engine, one
/// connection pool, one lifecycle), rebuilt when configuration changes,
/// plus the fetch registry and the hook registry. Hooks live here, not
/// on the client: a pin rebuild swaps the client wholesale, and hook
/// registrations are app-lifetime state that must survive it. WebSocket
/// connections carry their own state — no registry needed (see ws.rs);
/// they build their transport from the live trust config (pins and
/// custom roots), which is why it is retained here alongside the
/// rebuilt HTTP transport.
pub struct Monet {
    transport: RwLock<Arc<dyn monet_core::Transport>>,
    client: RwLock<Arc<Client>>,
    trust: RwLock<platform::TrustConfig>,
    fetches: FetchRegistry,
    hooks: RwLock<HookRegistry>,
    pending: Arc<Pending>,
}

impl Monet {
    pub fn create() -> anyhow::Result<Self> {
        let trust = platform::TrustConfig {
            pins: PinConfig::default(),
            roots: Vec::new(),
        };
        let transport = platform::http_transport(&trust);
        Ok(Monet {
            client: RwLock::new(Arc::new(Client::new(transport.clone()))),
            transport: RwLock::new(transport),
            trust: RwLock::new(trust),
            fetches: FetchRegistry::new(),
            hooks: RwLock::new(HookRegistry::new()),
            pending: Pending::shared(),
        })
    }

    /// Rebuilds the transport + client after a configuration change
    /// (pins, trust roots). iOS pins and roots live in the Swift shim
    /// (process-global, last-wins); Android pins and roots go into the
    /// transport configs, so a rebuild re-installs them there. In-flight
    /// fetches keep the old transport alive through their Arc clones;
    /// it shuts down (engine `shutdown()` on Android) once the last one
    /// finishes. Hook registrations are deliberately untouched.
    fn rebuild(&self, trust: platform::TrustConfig) -> anyhow::Result<()> {
        let transport = platform::http_transport(&trust);
        let client = Client::new(transport.clone());
        *self.transport.write().unwrap() = transport;
        *self.client.write().unwrap() = Arc::new(client);
        *self.trust.write().unwrap() = trust;
        Ok(())
    }

    /// A cheap snapshot of the hook chains for a pump task: the clone is
    /// arcs-only, and taking it drops the read guard before any await
    /// (std guards are not `Send` across awaits).
    fn hook_snapshot(&self) -> HookRegistry {
        self.hooks.read().unwrap().clone()
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
    /// The call-site metadata bag: rides the request's extensions
    /// (client-side only, never on the wire; the call site can
    /// additionally send it as a header when the server needs it too),
    /// reported by every hook/observer event of this fetch — the flow
    /// tag that strings a series of requests together.
    #[serde(default)]
    pub metadata: Vec<HeaderKV>,
    /// The per-request knobs (v2/W1): 0 / true keep the transport's
    /// default; anything else rides the core Request's Option overrides
    /// and wins over configured values.
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

/// One WS message signal: the type flag; the bytes follow through the
/// connection object's `take_message`.
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
        let mut trust = self.trust.read().unwrap().clone();
        trust.pins = pins;
        self.rebuild(trust)
    }

    pub fn clear_pins(&self) -> anyhow::Result<()> {
        let mut trust = self.trust.read().unwrap().clone();
        trust.pins = PinConfig::default();
        self.rebuild(trust)
    }

    /// Sets the app-supplied trust roots (each a PEM block or bare
    /// base64 DER) that TLS handshakes anchor alongside the platform
    /// store — network_security_config semantics (system CAs stay
    /// trusted). Fail-closed: an entry that does not decode is an error
    /// and installs nothing.
    ///
    /// Platform reach (the HttpEngine carve-out is on record in the
    /// plans): monet-ws and the monet-rustls fetch fallback take the
    /// roots; the platform HttpEngine has no custom-roots API, so that
    /// fetch path still trusts the system + network_security_config
    /// anchors only. On iOS every fetch and wss challenge is anchored
    /// through the Swift shim.
    pub fn configure_trust(&self, roots: Vec<String>) -> anyhow::Result<()> {
        let decoded = roots
            .iter()
            .map(|root| der_from_pem_or_base64(root))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut trust = self.trust.read().unwrap().clone();
        trust.roots = decoded;
        self.rebuild(trust)
    }

    /// Clears the custom trust roots: connections configured after this
    /// trust the platform store only.
    pub fn clear_trust(&self) -> anyhow::Result<()> {
        let mut trust = self.trust.read().unwrap().clone();
        trust.roots = Vec::new();
        self.rebuild(trust)
    }

    // ---- hooks & observers (M5; the design record is
    // docs/plans/2026-11-hooks.md) ----

    /// Registers a read-write request hook: it may rewrite the request
    /// (url/method/headers) or abort the pipeline, and the pipeline waits
    /// for its resolution. The callback receives
    /// `{hookCallId, attempt, url, method, headers}` and JS must call
    /// `resolveRequestHook` or `rejectRequestHook` with that id.
    pub fn add_request_hook(&self, on_hook: jsi_turbo::TurboCallback) -> anyhow::Result<()> {
        self.hooks
            .write()
            .unwrap()
            .add_request_hook(crate::hooks::JsRequestHook::new(
                on_hook,
                self.pending.clone(),
            ));
        Ok(())
    }

    /// Registers a read-write response hook: it runs when the head
    /// arrives, before the head is delivered, and may rewrite
    /// status/headers, abort, or ask for a retry. The callback receives
    /// `{hookCallId, attempt, status, headers}`.
    pub fn add_response_hook(&self, on_hook: jsi_turbo::TurboCallback) -> anyhow::Result<()> {
        self.hooks
            .write()
            .unwrap()
            .add_response_hook(crate::hooks::JsResponseHook::new(
                on_hook,
                self.pending.clone(),
            ));
        Ok(())
    }

    /// Registers a read-only request observer: it sees the final request
    /// (after every hook, before the wire) and cannot affect the
    /// pipeline — the callback is fire-and-forget, nothing waits.
    pub fn add_request_observer(&self, on_observe: jsi_turbo::TurboCallback) -> anyhow::Result<()> {
        self.hooks
            .write()
            .unwrap()
            .add_request_observer(crate::hooks::JsRequestObserver::new(on_observe));
        Ok(())
    }

    /// Registers a read-only response observer: it sees the final
    /// response head (after every hook, before delivery).
    pub fn add_response_observer(
        &self,
        on_observe: jsi_turbo::TurboCallback,
    ) -> anyhow::Result<()> {
        self.hooks
            .write()
            .unwrap()
            .add_response_observer(crate::hooks::JsResponseObserver::new(on_observe));
        Ok(())
    }

    /// Drops every hook and observer registration (the `clear_pins`
    /// symmetry), and every outstanding hook call.
    pub fn clear_hooks(&self) -> anyhow::Result<()> {
        self.hooks.write().unwrap().clear();
        self.pending.clear();
        Ok(())
    }

    /// Delivers a request hook's resolution: the rewritten
    /// url/method/headers (body passes through untouched).
    pub fn resolve_request_hook(
        &self,
        hook_call_id: f64,
        request: crate::hooks::HookRequestResolution,
    ) -> anyhow::Result<bool> {
        Ok(crate::hooks::protocol::resolve_request(
            &self.pending,
            hook_call_id,
            request,
        ))
    }

    /// Delivers a request hook's rejection. A `Cancelled` kind maps to a
    /// clean cancellation; anything else fails the fetch as a hook error.
    pub fn reject_request_hook(
        &self,
        hook_call_id: f64,
        error: crate::hooks::HookErrorEvent,
    ) -> anyhow::Result<bool> {
        Ok(crate::hooks::protocol::reject_request(
            &self.pending,
            hook_call_id,
            error,
        ))
    }

    /// Delivers a response hook's resolution: the rewritten
    /// status/headers (the body passes through untouched).
    pub fn resolve_response_hook(
        &self,
        hook_call_id: f64,
        response: crate::hooks::HookResponseResolution,
    ) -> anyhow::Result<bool> {
        Ok(crate::hooks::protocol::resolve_response(
            &self.pending,
            hook_call_id,
            response,
        ))
    }

    /// Delivers a response hook's rejection.
    pub fn reject_response_hook(
        &self,
        hook_call_id: f64,
        error: crate::hooks::HookErrorEvent,
    ) -> anyhow::Result<bool> {
        Ok(crate::hooks::protocol::reject_response(
            &self.pending,
            hook_call_id,
            error,
        ))
    }

    /// Delivers a response hook's retry request: the pipeline re-runs
    /// from the original request (request hooks included, so they can
    /// inject a fresh token), up to the module's hard cap.
    pub fn retry_response_hook(&self, hook_call_id: f64) -> anyhow::Result<bool> {
        Ok(crate::hooks::protocol::retry_response(
            &self.pending,
            hook_call_id,
        ))
    }

    // ---- fetch: head callback first, body pulled in chunks ----

    /// Starts a fetch. The four callbacks fire from the pump task
    /// (marshalled back to the JS thread by the TurboCallback
    /// dispatcher): `on_head` once with status + headers, `on_chunk` as
    /// a signal that a chunk is queued (JS then calls `take_chunk`),
    /// then exactly one of `on_done` / `on_error`.
    ///
    /// The pump runs the full hook pipeline per attempt (request hooks
    /// raced with the cancel token → transport → response hooks), then
    /// delivers the head: `on_head` fires only after the response hooks
    /// have had their chance to rewrite it. A response hook's retry
    /// re-runs the pipeline from the original request (up to
    /// [`MAX_HOOK_RETRIES`]) — the token survives across attempts.
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
        // The upload is fully buffered in v1 (nitro parity): the JS
        // ArrayBuffer is copied here — the borrow ends with the call. The
        // bytes stay with the pump so a hook-driven retry can rebuild the
        // request (a Body is a stream and cannot be cloned).
        let body_bytes = body.map(|buffer| buffer.data(rt).to_vec());

        // Fail fast on malformed inputs exactly as before the hook
        // pipeline (the pump's build_request re-validates per attempt —
        // hook rewrites land there — but the original JS inputs should
        // throw synchronously, not surface through on_error).
        let _ = options
            .url
            .parse::<monet_core::Url>()
            .map_err(|e| anyhow::anyhow!("invalid URL: {e}"))?;

        let (fetch_id, cancel) = self.fetches.allocate();
        let on_head = Arc::new(on_head);
        let on_chunk = Arc::new(on_chunk);
        let on_done = Arc::new(on_done);
        let on_error = Arc::new(on_error);

        // The transport is the module's shared one: a per-fetch engine
        // would leak a Cronet broadcast receiver each time (the process
        // dies at the 1000-receiver cap) and pool no connections across
        // fetches. The Arc clone keeps it alive until this pump ends.
        let transport = self.transport.read().unwrap().clone();
        let hooks = self.hook_snapshot();
        spawn_fetch_pump(
            self.fetches.clone(),
            transport,
            hooks,
            fetch_id,
            cancel,
            options,
            PumpBody::Buffered(body_bytes),
            on_head,
            on_chunk,
            on_done,
            on_error,
        );

        Ok(fetch_id)
    }

    /// Starts a fetch whose request body is STREAMED from JS: the body
    /// rides a core channel whose sending half the JS producer feeds
    /// through [`Monet::fetch_stream_chunk`] and closes with
    /// [`Monet::fetch_stream_complete`] (or
    /// [`Monet::fetch_stream_error`]). The transport streams it to the
    /// wire as chunks arrive (on the rustls stack; the buffered stacks
    /// collect it — see the v2 plan's W2 carve-outs). A response-hook
    /// retry cannot replay a consumed stream: the fetch fails with an
    /// explicit error instead of silently re-sending an empty body.
    pub fn fetch_stream(
        &self,
        options: FetchOptions,
        on_head: jsi_turbo::TurboCallback,
        on_chunk: jsi_turbo::TurboCallback,
        on_done: jsi_turbo::TurboCallback,
        on_error: jsi_turbo::TurboCallback,
    ) -> anyhow::Result<f64> {
        // Same fail-fast contract as fetch: malformed inputs throw
        // synchronously instead of surfacing through on_error.
        let _ = options
            .url
            .parse::<monet_core::Url>()
            .map_err(|e| anyhow::anyhow!("invalid URL: {e}"))?;

        let (fetch_id, cancel) = self.fetches.allocate();
        // The receiving half rides the request; the JS producer holds
        // the sending half through the module methods.
        let (body, feed) = monet_core::body::channel();
        self.fetches.attach_stream_feed(fetch_id, feed);
        let on_head = Arc::new(on_head);
        let on_chunk = Arc::new(on_chunk);
        let on_done = Arc::new(on_done);
        let on_error = Arc::new(on_error);
        let transport = self.transport.read().unwrap().clone();
        let hooks = self.hook_snapshot();
        spawn_fetch_pump(
            self.fetches.clone(),
            transport,
            hooks,
            fetch_id,
            cancel,
            options,
            PumpBody::Stream(Some(body)),
            on_head,
            on_chunk,
            on_done,
            on_error,
        );

        Ok(fetch_id)
    }

    /// Pushes one producer chunk into a streaming body.
    pub fn fetch_stream_chunk<'rt>(
        &self,
        rt: &mut RuntimeHandle<'rt>,
        id: f64,
        chunk: jsi::JsiArrayBuffer<'rt>,
    ) -> anyhow::Result<()> {
        let bytes = chunk.data(rt).to_vec();
        if !self.fetches.stream_chunk(id, bytes) {
            anyhow::bail!("no streaming body for fetch {id} (completed, errored, or cleaned up)");
        }
        Ok(())
    }

    /// Ends a streaming body successfully: the request's upload
    /// completes with everything pushed so far.
    pub fn fetch_stream_complete(&self, id: f64) -> anyhow::Result<()> {
        let _ = self.fetches.stream_complete(id);
        Ok(())
    }

    /// Ends a streaming body with an error (the JS producer threw):
    /// the upload aborts and the fetch fails.
    pub fn fetch_stream_error(&self, id: f64, message: String) -> anyhow::Result<()> {
        let _ = self
            .fetches
            .stream_error(id, monet_core::Error::transport(message));
        Ok(())
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

    /// Opens a WebSocket and returns the connection object. `on_open`
    /// fires when the handshake completes; then `on_message` signals
    /// queued data (JS pulls with the object's `take_message`, the event
    /// carries `{isBinary}`), and exactly one of `on_close` / `on_error`
    /// terminates. The close event carries `{code, reason, wasClean}` —
    /// the fidelity the URLSession-based iOS transport cannot provide.
    /// The connection's lifetime is the JS object's: GC dropping the
    /// wrapper drops the native state, whose `Drop` cancels the
    /// transport.
    ///
    /// The request hook chain runs on the upgrade request (raced with
    /// the cancel token) before the connect — auth-header injection for
    /// the handshake shares the fetch-side registration. No response
    /// hooks and no retry: the upgrade has no head the JS side sees.
    pub fn ws_connect(
        &self,
        url: String,
        protocols: Vec<String>,
        headers: Vec<HeaderKV>,
        on_open: jsi_turbo::TurboCallback,
        on_message: jsi_turbo::TurboCallback,
        on_close: jsi_turbo::TurboCallback,
        on_error: jsi_turbo::TurboCallback,
    ) -> anyhow::Result<MonetWsConnection> {
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

        let (conn, cancel, command_rx, queue) = MonetWsConnection::channel();
        let on_open = Arc::new(on_open);
        let on_message = Arc::new(on_message);
        let on_close = Arc::new(on_close);
        let on_error = Arc::new(on_error);

        // The live trust config: pins and custom roots configured through
        // configurePins/configureTrust must reach the WS handshake too
        // (the wss verify block consults them), not just the rebuilt
        // HTTP transport.
        let transport = platform::ws_transport(&self.trust.read().unwrap().clone())?;
        let hooks = self.hook_snapshot();
        async_global_executor::spawn(async move {
            // Hold the transport for the connection's lifetime: its
            // private runtime drives both the connect future and the
            // socket pump — dropping it would tear them down.
            // Hold one Arc for the connection's lifetime (the connect
            // below borrows the original).
            let _keep_alive = transport.clone();

            // Request hooks on the upgrade request (attempt is always 0
            // here — no retry), raced with the cancel token: dropping
            // the JS object mid-hook tears the connect down with it.
            // A fresh correlation id tags this connection's events.
            request.extensions.insert(crate::hooks::HookAttempt(0));
            request.extensions.insert(crate::hooks::CorrelationId(
                crate::hooks::next_correlation_id(),
            ));
            let request = match race_cancel(hooks.run_request_hooks(request), &cancel).await {
                RaceOutcome::Done(Ok(request)) => request,
                RaceOutcome::Done(Err(error)) => {
                    on_error.call(ErrorEvent::from(error));
                    return;
                }
                RaceOutcome::Cancelled => {
                    on_error.call(ErrorEvent::from(monet_core::Error::Cancelled));
                    return;
                }
            };

            match transport.connect(request, cancel).await {
                Ok(mut stream) => {
                    on_open.call_with_no_args();
                    ws_pump(
                        &mut stream,
                        command_rx,
                        &queue,
                        &on_message,
                        &on_close,
                        &on_error,
                    )
                    .await;
                }
                Err(error) => {
                    // The connect itself failed: the object JS holds is
                    // inert (its queue is empty, its commands go
                    // nowhere) and `on_error` says why. Nothing to clean
                    // up — the connection carries its own state.
                    on_error.call(ErrorEvent::from(error));
                }
            }
        })
        .detach();

        Ok(conn)
    }
}

/// One custom trust root as the app supplies it: a PEM block or bare
/// base64 DER. The armor lines are dropped when present, and all
/// whitespace goes (PEM wraps its body) — what remains must decode as
/// standard base64 DER.
fn der_from_pem_or_base64(input: &str) -> anyhow::Result<Vec<u8>> {
    let body: String = input
        .lines()
        .filter(|line| !line.trim_start().starts_with("-----"))
        .collect::<Vec<_>>()
        .join("");
    if body.is_empty() {
        anyhow::bail!("empty root certificate");
    }
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(body.as_bytes())
        .map_err(|e| anyhow::anyhow!("root certificate is not base64 DER: {e}"))
}

/// Builds the request from the JS inputs. The pump calls this once per
/// attempt — a retry must re-run from the original request, and a `Body`
/// is a stream that cannot be cloned, so the buffered upload bytes stay
/// with the pump.
/// The fetch pump shared by both entry points ([`Monet::fetch`] for
/// buffered bodies, [`Monet::fetch_stream`] for streamed ones): the full
/// hook pipeline per attempt, then head + chunked body delivery over the
/// registry.
#[allow(clippy::too_many_arguments)]
fn spawn_fetch_pump(
    fetches: FetchRegistry,
    transport: Arc<dyn monet_core::Transport>,
    hooks: HookRegistry,
    fetch_id: f64,
    cancel: monet_core::CancelToken,
    options: FetchOptions,
    mut body: PumpBody,
    on_head: Arc<jsi_turbo::TurboCallback>,
    on_chunk: Arc<jsi_turbo::TurboCallback>,
    on_done: Arc<jsi_turbo::TurboCallback>,
    on_error: Arc<jsi_turbo::TurboCallback>,
) {
    async_global_executor::spawn(async move {
            // Hold one Arc for the pump's lifetime: a pin-config rebuild
            // swaps the module's transport, and the old engine's
            // shutdown() must wait until this pump is done with it. The
            // original stays usable for the per-attempt executes.
            let _keep_engine = transport.clone();

            let mut attempt: u32 = 0;
            // The fetch's pairing key: stable across retries, carried in
            // the request extensions and threaded to the response, so
            // the request-phase and response-phase events of this fetch
            // report the same correlationId (and Rust-native hooks can
            // stash their own state the same way).
            let correlation_id = crate::hooks::next_correlation_id();
            let response = loop {
                let mut request = match &mut body {
                    PumpBody::Buffered(bytes) => build_request(&options, bytes),
                    // The stream was consumed by the first attempt; a
                    // hook-driven retry cannot replay it.
                    PumpBody::Stream(slot) => match slot.take() {
                        Some(stream) => build_request_streaming(&options, stream),
                        None => Err(monet_core::Error::transport(
                            "monet-rn: a streamed body cannot be retried — the stream was consumed by the first attempt",
                        )),
                    },
                };
                let mut request = match request {
                    Ok(request) => request,
                    Err(error) => {
                        fetches.finish(fetch_id);
                        on_error.call(ErrorEvent::from(error));
                        return;
                    }
                };
                request.extensions.insert(hooks::HookAttempt(attempt));
                request
                    .extensions
                    .insert(hooks::CorrelationId(correlation_id));
                request
                    .extensions
                    .insert(crate::hooks::metadata_bag(&options.metadata));

                // Request hooks, raced with the cancel token — a stuck
                // JS hook must not outlive the fetch's abort.
                match race_cancel(hooks.run_request_hooks(request), &cancel).await {
                    RaceOutcome::Done(Ok(mut request)) => {
                        let transport = transport.clone();
                        let hooks = hooks.clone();
                        let cancel_for_execute = cancel.clone();
                        match race_cancel(
                            // Response hooks wrap the transport result:
                            // they see the head before on_head delivers
                            // it. The request's extensions travel with it
                            // (no transport reads them — nothing on the
                            // wire changes).
                            async move {
                                let extensions = std::mem::take(&mut request.extensions);
                                let mut response =
                                    transport.execute(request, cancel_for_execute).await?;
                                response.extensions = extensions;
                                hooks.run_response_hooks(response).await
                            },
                            &cancel,
                        )
                        .await
                        {
                            RaceOutcome::Done(Ok(response)) => break response,
                            RaceOutcome::Done(Err(error)) => {
                                if crate::hooks::is_hook_retry(&error) && attempt < MAX_HOOK_RETRIES
                                {
                                    // The response hook asked for a retry:
                                    // rebuild from the original inputs and
                                    // run the whole pipeline again. The
                                    // aborted attempt's body is dropped
                                    // with it.
                                    attempt += 1;
                                    continue;
                                }
                                fetches.finish(fetch_id);
                                on_error.call(ErrorEvent::from(error));
                                return;
                            }
                            RaceOutcome::Cancelled => {
                                fetches.finish(fetch_id);
                                on_error.call(ErrorEvent::from(monet_core::Error::Cancelled));
                                return;
                            }
                        }
                    }
                    RaceOutcome::Done(Err(error)) => {
                        fetches.finish(fetch_id);
                        on_error.call(ErrorEvent::from(error));
                        return;
                    }
                    RaceOutcome::Cancelled => {
                        fetches.finish(fetch_id);
                        on_error.call(ErrorEvent::from(monet_core::Error::Cancelled));
                        return;
                    }
                }
            };

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
        })
        .detach();
}

/// The pump's request body source.
enum PumpBody {
    /// Buffered bytes: cloned per attempt, so a hook-driven retry
    /// rebuilds the request from them.
    Buffered(Option<Vec<u8>>),
    /// A streamed upload ([`Monet::fetch_stream`]): the channel's
    /// receiving half, consumed by the first attempt — a retry cannot
    /// replay it and fails the fetch with an explicit error.
    Stream(Option<monet_core::body::Body>),
}

/// [`build_request`] with a streaming body: the channel rides the
/// request as-is; the transport polls it chunk by chunk.
fn build_request_streaming(
    options: &FetchOptions,
    body: monet_core::body::Body,
) -> monet_core::Result<Request> {
    let mut request = build_request(options, &None)?;
    request.body = body;
    Ok(request)
}

fn build_request(options: &FetchOptions, body: &Option<Vec<u8>>) -> monet_core::Result<Request> {
    // `FromStr::Err` is `Infallible`; unknown verbs land in `Method::Other`.
    let method = options
        .method
        .parse::<Method>()
        .unwrap_or_else(|_| unreachable!());
    let url = options
        .url
        .parse::<monet_core::Url>()
        .map_err(|e| monet_core::Error::transport(format!("invalid URL: {e}")))?;
    let mut request = Request::new(method, url);
    for header in &options.headers {
        request
            .headers
            .append(&header.name, &header.value)
            .map_err(|e| monet_core::Error::transport(format!("invalid header: {e}")))?;
    }
    if let Some(bytes) = body {
        // A clone per attempt: from_bytes consumes, and a retry rebuilds
        // the request from these bytes.
        request.body = monet_core::body::Body::from_bytes(bytes.clone());
    }
    // The per-request knobs (0 / true = keep the transport default —
    // the fields ride the core Request's Option overrides).
    if options.timeout_ms > 0.0 {
        request.timeout_ms = Some(options.timeout_ms as u64);
    }
    if !options.follow_redirects {
        request.follow_redirects = Some(false);
    }
    Ok(request)
}

/// What a cancel-raced pipeline step resolves to.
enum RaceOutcome<T> {
    Done(T),
    Cancelled,
}

/// Races a pipeline future against the fetch's cancel token — a stuck JS
/// hook must not outlive the abort. Same shape as the WS pump's select.
async fn race_cancel<F: std::future::Future>(
    fut: F,
    cancel: &monet_core::CancelToken,
) -> RaceOutcome<F::Output> {
    futures_lite::future::race(async { RaceOutcome::Done(fut.await) }, async {
        cancel.cancelled().await;
        RaceOutcome::Cancelled
    })
    .await
}

/// The WS pump: delivers received messages into the queue with signals,
/// applies user commands to the stream, and terminates exactly once.
async fn ws_pump(
    stream: &mut monet_core::ws::WebSocketStream,
    mut command_rx: futures_channel::mpsc::UnboundedReceiver<WsCommand>,
    queue: &WsQueue,
    on_message: &jsi_turbo::TurboCallback,
    on_close: &jsi_turbo::TurboCallback,
    on_error: &jsi_turbo::TurboCallback,
) {
    use futures_lite::StreamExt as _;
    use monet_core::ws::WsMessage;
    loop {
        // Two-way select: a received message or a user command. The
        // cancel token lives with the connection object (JS side) and
        // the transport (handed over in `connect`): firing it drops the
        // transport pump, which ends the stream — `Ok(None)` below
        // carries the abnormal close.
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
                queue.lock().unwrap().push(text.into_bytes());
                on_message.call(WsMessageEvent { is_binary: false });
            }
            Step::Message(Ok(Some(WsMessage::Binary(bytes)))) => {
                queue.lock().unwrap().push(bytes.to_vec());
                on_message.call(WsMessageEvent { is_binary: true });
            }
            Step::Message(Ok(Some(WsMessage::Ping(bytes)))) => {
                // Surfaced with its payload for tests/custom protocols;
                // the transport already answered it.
                queue.lock().unwrap().push(bytes.to_vec());
                on_message.call(WsMessageEvent { is_binary: true });
            }
            Step::Message(Ok(Some(WsMessage::Pong(bytes)))) => {
                // Fire-and-forget: surfaced like a message for symmetry.
                queue.lock().unwrap().push(bytes.to_vec());
                on_message.call(WsMessageEvent { is_binary: true });
            }
            Step::Message(Ok(Some(WsMessage::Close(frame)))) => {
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
                on_close.call(WsCloseEvent {
                    code: 1006.0,
                    reason: String::new(),
                    was_clean: false,
                });
                return;
            }
            Step::Message(Err(error)) => {
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
                    on_error.call(ErrorEvent::from(monet_core::Error::Body(
                        "websocket driver dropped".into(),
                    )));
                    return;
                }
            }
            Step::Command(Some(WsCommand::Close { code, reason })) => {
                if stream.close(code, reason).is_err() {
                    on_error.call(ErrorEvent::from(monet_core::Error::Body(
                        "websocket driver dropped".into(),
                    )));
                    return;
                }
                // The close frame is on the wire; the peer's answer
                // arrives as a Close message above.
            }
            Step::Command(None) => {
                // The command channel closed (the connection object is
                // gone): the cancel token has fired with it, so the
                // stream race above is about to end this pump anyway.
                return;
            }
        }
    }
}
