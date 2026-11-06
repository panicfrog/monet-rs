//! The WebSocket registry: connId → open socket, shared between the
//! module methods (JS thread) and the pump tasks (executor threads).
//!
//! Same protocol as the fetch channel: the pump queues received messages
//! and signals `on_message`; JS pulls with `ws_take_message`
//! (zero-copy). Sending is command-driven — `ws_send`/`ws_close` push a
//! [`WsCommand`] into a per-connection channel that the pump drains and
//! applies to the [`WebSocketStream`] it owns (the stream's send half is
//! `&mut self`, so only the pump may touch it).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jsi::JsiArrayBuffer;
use jsi::RuntimeHandle;
use monet_core::CancelToken;

/// One user-issued command for the pump to apply to the stream.
pub(crate) enum WsCommand {
    /// Send a text (`false`) or binary (`true`) message.
    Send { data: Vec<u8>, is_binary: bool },
    /// Start the close handshake with this code and reason.
    Close { code: u16, reason: String },
}

struct SocketState {
    cancel: CancelToken,
    queue: Mutex<Vec<QueuedMessage>>,
    /// Commands waiting for the pump (send/close arrive from the JS
    /// thread; only the pump touches the stream).
    commands: futures_channel::mpsc::UnboundedSender<WsCommand>,
    finished: bool,
}

struct QueuedMessage {
    data: Vec<u8>,
    is_binary: bool,
}

#[derive(Clone)]
pub struct WsRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    states: Mutex<HashMap<u64, SocketState>>,
}

impl WsRegistry {
    pub fn new() -> Self {
        WsRegistry {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(0),
                states: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Allocates a connection: the id (surfaced to JS as f64), the
    /// cancel token for the transport connect call, and the receiving
    /// end of the command channel for the pump to drain.
    pub fn allocate(
        &self,
    ) -> (
        f64,
        CancelToken,
        futures_channel::mpsc::UnboundedReceiver<WsCommand>,
    ) {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let cancel = CancelToken::new();
        let (command_tx, command_rx) = futures_channel::mpsc::unbounded();
        self.inner.states.lock().unwrap().insert(
            id,
            SocketState {
                cancel: cancel.clone(),
                queue: Mutex::new(Vec::new()),
                commands: command_tx,
                finished: false,
            },
        );
        (f64::from(u32::try_from(id).unwrap_or(u32::MAX)), cancel, command_rx)
    }

    /// Queues one received message from the pump; `false` when the state
    /// is gone (post-cleanup — the pump stops pulling).
    pub fn push_message(&self, id: f64, data: Vec<u8>, is_binary: bool) -> bool {
        let mut states = self.inner.states.lock().unwrap();
        match states.get_mut(&(id as u64)) {
            Some(state) => {
                state.queue.lock().unwrap().push(QueuedMessage { data, is_binary });
                true
            }
            None => false,
        }
    }

    /// Takes one queued message as a zero-copy ArrayBuffer; `None` when
    /// the queue is empty (check `is_done`).
    pub fn take_message<'rt>(
        &self,
        id: f64,
        rt: &mut RuntimeHandle<'rt>,
    ) -> Option<(JsiArrayBuffer<'rt>, bool)> {
        let states = self.inner.states.lock().unwrap();
        let state = states.get(&(id as u64))?;
        let mut queue = state.queue.lock().unwrap();
        queue
            .pop()
            .map(|message| (JsiArrayBuffer::from_vec(message.data, rt), message.is_binary))
    }

    /// Enqueues a send/close command for the pump. `false` when the
    /// connection is unknown or its pump is already gone.
    pub fn send_command(&self, id: f64, command: WsCommand) -> bool {
        let states = self.inner.states.lock().unwrap();
        match states.get(&(id as u64)) {
            Some(state) => state.commands.unbounded_send(command).is_ok(),
            None => false,
        }
    }

    /// Marks the connection terminated.
    pub fn finish(&self, id: f64) {
        let mut states = self.inner.states.lock().unwrap();
        if let Some(state) = states.get_mut(&(id as u64)) {
            state.finished = true;
        }
    }

    /// `true` when terminated and drained.
    pub fn is_done(&self, id: f64) -> bool {
        let states = self.inner.states.lock().unwrap();
        match states.get(&(id as u64)) {
            Some(state) => state.finished && state.queue.lock().unwrap().is_empty(),
            None => true,
        }
    }

    /// Fires the cancel token (abort / connect-phase cancellation).
    pub fn abort(&self, id: f64) -> bool {
        let states = self.inner.states.lock().unwrap();
        match states.get(&(id as u64)) {
            Some(state) => {
                state.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Drops a finished connection's state.
    pub fn cleanup(&self, id: f64) {
        self.inner.states.lock().unwrap().remove(&(id as u64));
    }
}
