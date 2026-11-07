//! The WebSocket connection object: the native half of MonetWebSocket.
//!
//! `ws_connect` returns a `MonetWsConnection` to JS; the JS wrapper's
//! NativeState holds the sole strong reference, so when the JS object is
//! collected (or the app tears down) the Rust instance drops — and its
//! [`Drop`] impl fires the cancel token, which ends the pump. No registry,
//! no connection ids: the object IS the handle.
//!
//! The pump shares only the clonable pieces (the receive queue, the
//! command channel's receiving end) — never the connection itself — so the
//! reference count reaching zero is driven by JS alone. Same protocol
//! as the fetch channel underneath: the pump queues received messages and
//! signals `on_message`; JS pulls with `take_message` (zero-copy). Sending
//! is command-driven — `send`/`close` push a [`WsCommand`] into a channel
//! that the pump drains and applies to the [`WebSocketStream`] it owns
//! (the stream's send half is `&mut self`, so only the pump may touch it).

use std::sync::{Arc, Mutex};

use jsi::{JsiArrayBuffer, RuntimeHandle};
use monet_core::CancelToken;

/// One user-issued command for the pump to apply to the stream.
pub(crate) enum WsCommand {
    /// Send a text (`false`) or binary (`true`) message.
    Send { data: Vec<u8>, is_binary: bool },
    /// Start the close handshake with this code and reason.
    Close { code: u16, reason: String },
}

/// The receive queue: received messages waiting for JS to pull them
/// (zero-copy). The isBinary flag is NOT queued — it rides the
/// `on_message` signal that precedes each pull, and the TS layer pairs
/// them FIFO (same shape as the fetch channel's on_chunk/take_chunk).
pub(crate) type WsQueue = Arc<Mutex<Vec<Vec<u8>>>>;

/// A live WebSocket connection as handed to JS. Every operation goes
/// through the object; dropping it disconnects.
pub struct MonetWsConnection {
    cancel: CancelToken,
    commands: futures_channel::mpsc::UnboundedSender<WsCommand>,
    queue: WsQueue,
}

impl MonetWsConnection {
    /// Creates the connection halves: the object for JS, the cancel token
    /// for the transport's connect call, the command stream for the pump
    /// to drain, and the shared receive queue. (Same shape as
    /// `monet_core::ws::channel`, plus the cancel token the transport
    /// needs — it stays live in the object so dropping the object can
    /// fire it.)
    pub(crate) fn channel() -> (
        Self,
        CancelToken,
        futures_channel::mpsc::UnboundedReceiver<WsCommand>,
        WsQueue,
    ) {
        let cancel = CancelToken::new();
        let (command_tx, command_rx) = futures_channel::mpsc::unbounded();
        let queue: WsQueue = Arc::new(Mutex::new(Vec::new()));
        (
            MonetWsConnection {
                cancel: cancel.clone(),
                commands: command_tx,
                queue: queue.clone(),
            },
            cancel,
            command_rx,
            queue,
        )
    }
}

impl Drop for MonetWsConnection {
    fn drop(&mut self) {
        // The JS object went away: disconnect. The pump sees the stream
        // end, fires its terminal event (to whoever is still listening),
        // and exits.
        //
        // This is the release path for a *dropped* socket: the app's own
        // allocation drives the collector to the wrapper's NativeState,
        // no explicit disposal required. The timing is the collector's
        // schedule (young-gen scavenges clear WeakRefs but run cell
        // finalizers only on the old-generation sweep), so `close()` /
        // `abort()` — which fire this same token directly — remain the
        // paths to use when immediacy matters.
        self.cancel.cancel();
    }
}

impl Default for MonetWsConnection {
    /// An inert connection: a token nothing watches, a command channel
    /// nobody drains, an empty queue. Only the object machinery's
    /// default-construction bound wants this — `ws_connect` is the only
    /// constructor that reaches JS, and it always wires a real pump.
    fn default() -> Self {
        MonetWsConnection {
            cancel: CancelToken::new(),
            commands: futures_channel::mpsc::unbounded().0,
            queue: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[jsi_turbo::TurboObject]
impl MonetWsConnection {
    /// Sends a text (`is_binary == false`) or binary message.
    pub fn send<'rt>(
        &self,
        rt: &mut RuntimeHandle<'rt>,
        message: JsiArrayBuffer<'rt>,
        is_binary: bool,
    ) -> anyhow::Result<bool> {
        // The upload is copied here (v1 parity with fetch): the borrow
        // ends with the call.
        let data = message.data(rt).to_vec();
        Ok(self
            .commands
            .unbounded_send(WsCommand::Send { data, is_binary })
            .is_ok())
    }

    /// Sends a ping (the transport answers received pings by itself).
    pub fn send_ping(&self) -> anyhow::Result<bool> {
        Ok(self
            .commands
            .unbounded_send(WsCommand::Send {
                data: Vec::new(),
                is_binary: true,
            })
            .is_ok())
    }

    /// Starts the close handshake with this code and reason; the peer's
    /// answering close frame fires `on_close`.
    pub fn close(&self, code: f64, reason: String) -> anyhow::Result<bool> {
        let code = u16::try_from(code as u64).unwrap_or(1000);
        Ok(self
            .commands
            .unbounded_send(WsCommand::Close { code, reason })
            .is_ok())
    }

    /// Force-aborts the connection (no close handshake): fires the
    /// transport-level token. The pump surfaces the abrupt end as a
    /// `1006` close event.
    pub fn abort(&self) -> anyhow::Result<bool> {
        self.cancel.cancel();
        Ok(true)
    }

    /// Takes one queued message as a zero-copy ArrayBuffer. The isBinary
    /// flag arrived on the `on_message` signal that preceded this pull —
    /// the signal/data pairs are strictly FIFO, so the TS layer pairs
    /// them by order (same shape as the fetch channel's
    /// on_chunk/take_chunk).
    pub fn take_message<'rt>(
        &self,
        rt: &mut RuntimeHandle<'rt>,
    ) -> anyhow::Result<Option<JsiArrayBuffer<'rt>>> {
        let data = self.queue.lock().unwrap().pop();
        Ok(data.map(|data| JsiArrayBuffer::from_vec(data, rt)))
    }
}
