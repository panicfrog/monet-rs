//! WebSocket registry and pump: the Rust half of the NWConnection shim.
//!
//! Same shape as the HTTP registry: `connect` allocates an id and parks
//! on a oneshot for the handshake; Swift data-plane events look the id up
//! here. There is no async runtime in this crate, so after the handshake
//! a parked thread per connection pumps the user's outgoing channel into
//! the control plane (mirroring monet-ws's pump task) and exits when the
//! terminal event — or cancellation — drops its shutdown sender.

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_channel::oneshot;
use futures_lite::StreamExt;
use futures_lite::future::race;

use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::request::Request;
use monet_core::transport::BoxFuture;
use monet_core::ws::{WebSocketStream, WsMessage, channel as ws_channel};

use crate::bridge;

struct WsState {
    /// Resolves the connect future; taken by the first terminal-style
    /// event (`on_ws_open` or a pre-open `on_ws_close`).
    open_tx: Option<oneshot::Sender<Result<WebSocketStream>>>,
    /// The stream handed to the caller once open; created up front so
    /// either event can resolve without extra wiring.
    stream: Option<WebSocketStream>,
    /// Pushes received messages to the user.
    to_user: Option<UnboundedSender<WsMessage>>,
    /// The user's outgoing half, drained by the pump thread after open.
    from_user: Option<UnboundedReceiver<WsMessage>>,
    /// The request's cancel token, observed by the pump.
    cancel: CancelToken,
    /// Dropping this shuts the pump thread down; it lives exactly as
    /// long as the state.
    pump_done: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct WsRegistry {
    next_id: AtomicU64,
    states: Mutex<HashMap<u64, WsState>>,
}

static REGISTRY: LazyLock<WsRegistry> = LazyLock::new(WsRegistry::default);

/// Starts a connection: assembles the request under a fresh id, opens
/// the shim, and resolves once the handshake completes or the token
/// cancels.
pub(crate) fn connect(req: Request, cancel: CancelToken) -> BoxFuture<Result<WebSocketStream>> {
    Box::pin(async move {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let (id, open_rx) = allocate(cancel.clone());
        for (name, value) in req.headers.iter_flattened() {
            bridge::ws_add_header(id, name.as_bytes(), value.as_bytes());
        }
        bridge::ws_open(id, req.url.as_str().as_bytes());

        enum Step<T> {
            Open(T),
            Cancelled,
        }
        match race(async { Step::Open(open_rx.await) }, async {
            cancel.cancelled().await;
            Step::Cancelled
        })
        .await
        {
            Step::Open(Ok(stream)) => stream,
            Step::Open(Err(_)) => Err(Error::transport(
                "urlsession: websocket state dropped before the handshake completed",
            )),
            Step::Cancelled => {
                bridge::ws_cancel(id);
                REGISTRY.states.lock().unwrap().remove(&id);
                Err(Error::Cancelled)
            }
        }
    })
}

fn allocate(cancel: CancelToken) -> (u64, oneshot::Receiver<Result<WebSocketStream>>) {
    let (open_tx, open_rx) = oneshot::channel();
    let (stream, driver) = ws_channel();
    let id = REGISTRY.next_id.fetch_add(1, Ordering::Relaxed) + 1;
    REGISTRY.states.lock().unwrap().insert(
        id,
        WsState {
            open_tx: Some(open_tx),
            stream: Some(stream),
            to_user: Some(driver.network_to_user),
            from_user: Some(driver.user_to_network),
            cancel,
            pump_done: None,
        },
    );
    (id, open_rx)
}

pub(crate) fn on_ws_open(id: u64) {
    enum Opened {
        Started {
            open_tx: oneshot::Sender<Result<WebSocketStream>>,
            stream: WebSocketStream,
            from_user: UnboundedReceiver<WsMessage>,
            cancel: CancelToken,
        },
        None,
    }
    let opened = {
        let mut states = REGISTRY.states.lock().unwrap();
        let Some(state) = states.get_mut(&id) else {
            return;
        };
        match (
            state.open_tx.take(),
            state.stream.take(),
            state.from_user.take(),
        ) {
            (Some(open_tx), Some(stream), Some(from_user)) => Opened::Started {
                open_tx,
                stream,
                from_user,
                cancel: state.cancel.clone(),
            },
            _ => Opened::None, // already open; duplicate event
        }
    };
    if let Opened::Started {
        open_tx,
        stream,
        from_user,
        cancel,
    } = opened
    {
        let (done_tx, done_rx) = oneshot::channel();
        if let Some(state) = REGISTRY.states.lock().unwrap().get_mut(&id) {
            state.pump_done = Some(done_tx);
        }
        if open_tx.send(Ok(stream)).is_ok() {
            spawn_pump(id, from_user, cancel, done_rx);
        } else {
            // The connect future is gone (its cancel path won the race).
            bridge::ws_cancel(id);
            REGISTRY.states.lock().unwrap().remove(&id);
        }
    }
}

pub(crate) fn on_ws_message(id: u64, is_binary: bool, data: &[u8]) {
    let message = if is_binary {
        WsMessage::Binary(Bytes::copy_from_slice(data))
    } else {
        // The shim validates UTF-8 before delivering (protocol error
        // 1007 otherwise), so this never fails in practice.
        WsMessage::Text(String::from_utf8_lossy(data).into_owned())
    };
    deliver(id, message);
}

pub(crate) fn on_ws_ping(id: u64, data: &[u8]) {
    deliver(id, WsMessage::Ping(Bytes::copy_from_slice(data)));
}

pub(crate) fn on_ws_pong(id: u64, data: &[u8]) {
    deliver(id, WsMessage::Pong(Bytes::copy_from_slice(data)));
}

pub(crate) fn on_ws_close(id: u64, code: u16, reason: &[u8], _was_clean: bool, error: &[u8]) {
    let Some(state) = REGISTRY.states.lock().unwrap().remove(&id) else {
        return;
    };
    match state.open_tx {
        // Pre-open failure: surface the error on the connect future.
        Some(open_tx) => {
            let message = if error.is_empty() {
                "urlsession: websocket handshake failed".to_string()
            } else {
                format!("urlsession: {}", String::from_utf8_lossy(error))
            };
            let _ = open_tx.send(Err(Error::transport(message)));
        }
        // Open: deliver the close frame (code 0 = abnormal → None), then
        // dropping to_user ends the stream. Dropping the state also drops
        // pump_done, which exits the pump thread.
        None => {
            if let Some(to_user) = state.to_user {
                let frame = (code != 0).then(|| monet_core::ws::CloseFrame {
                    code,
                    reason: String::from_utf8_lossy(reason).into_owned(),
                });
                let _ = to_user.unbounded_send(WsMessage::Close(frame));
            }
        }
    }
}

/// Pushes a received message, or starts a polite close when the user is
/// gone (monet-ws closes politely on stream drop).
fn deliver(id: u64, message: WsMessage) {
    let delivered = {
        let states = REGISTRY.states.lock().unwrap();
        match states.get(&id) {
            None => true, // already closed; late events are harmless
            Some(state) => state
                .to_user
                .as_ref()
                .is_none_or(|to_user| to_user.unbounded_send(message).is_ok()),
        }
    };
    if !delivered {
        bridge::ws_close(id, 1000, b"");
    }
}

/// Drains the user's outgoing channel into the control plane until the
/// connection terminates.
fn spawn_pump(
    id: u64,
    mut from_user: UnboundedReceiver<WsMessage>,
    cancel: CancelToken,
    done: oneshot::Receiver<()>,
) {
    let spawned = std::thread::Builder::new()
        .name("monet-urlsession-ws-pump".into())
        .spawn(move || {
            use std::pin::pin;
            use std::task::Poll;
            enum Step {
                User(Option<WsMessage>),
                Cancelled,
                Done,
            }
            futures_lite::future::block_on(async {
                let mut done = pin!(done);
                // One waiter for the pump's whole life: a token waiter
                // releases its waker slot on drop, so a future rebuilt
                // per poll would never stay registered.
                let mut cancelled = pin!(cancel.cancelled());
                loop {
                    // Hand-rolled three-way select: the oneshot can only
                    // be awaited once, so every iteration re-polls it (and
                    // the token) through poll_fn, registering wakers.
                    let step = futures_lite::future::poll_fn(|cx| {
                        if done.as_mut().poll(cx).is_ready() {
                            return Poll::Ready(Step::Done);
                        }
                        if cancelled.as_mut().poll(cx).is_ready() {
                            return Poll::Ready(Step::Cancelled);
                        }
                        match pin!(&mut from_user).poll_next(cx) {
                            Poll::Ready(message) => Poll::Ready(Step::User(message)),
                            Poll::Pending => Poll::Pending,
                        }
                    })
                    .await;
                    match step {
                        Step::Done => return, // terminal event removed the state
                        // Cancellation after open closes politely, like
                        // monet-ws.
                        Step::Cancelled => {
                            bridge::ws_close(id, 0, b"");
                            return;
                        }
                        Step::User(None) => {
                            // The user dropped the stream: polite close.
                            bridge::ws_close(id, 1000, b"");
                            return;
                        }
                        Step::User(Some(WsMessage::Text(text))) => {
                            bridge::ws_send(id, false, text.as_bytes())
                        }
                        Step::User(Some(WsMessage::Binary(bytes))) => {
                            bridge::ws_send(id, true, &bytes)
                        }
                        Step::User(Some(WsMessage::Ping(bytes))) => {
                            bridge::ws_send_ping(id, &bytes)
                        }
                        Step::User(Some(WsMessage::Pong(bytes))) => {
                            bridge::ws_send_pong(id, &bytes)
                        }
                        Step::User(Some(WsMessage::Close(frame))) => {
                            match frame {
                                Some(frame) => {
                                    bridge::ws_close(id, frame.code, frame.reason.as_bytes())
                                }
                                None => bridge::ws_close(id, 0, b""),
                            }
                            return; // the sending side is finished
                        }
                    }
                }
            });
        });
    if spawned.is_err() {
        // Thread-starved: degrade to the terminal event's cleanup.
        bridge::ws_cancel(id);
        REGISTRY.states.lock().unwrap().remove(&id);
    }
}
