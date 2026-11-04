//! The WebSocket registry: connId → open socket, shared between the
//! module methods (JS thread) and the pump tasks (executor threads).
//! Same protocol as the fetch channel: messages are queued by the pump
//! with an `on_message` signal; JS pulls with `ws_take_message`
//! (zero-copy). M4 wires this; the registry lands with the fetch
//! channel so the module's state story is complete.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jsi::JsiArrayBuffer;
use jsi::RuntimeHandle;
use monet_core::CancelToken;

/// One message: binary payload + the flag (text messages arrive as
/// UTF-8 bytes; JS decodes).
struct QueuedMessage {
    data: Vec<u8>,
    is_binary: bool,
}

struct SocketState {
    cancel: CancelToken,
    queue: Mutex<Vec<QueuedMessage>>,
    finished: bool,
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

    /// Allocates a connection: the id (surfaced to JS as f64) and the
    /// cancel token for the transport connect call.
    pub fn allocate(&self) -> (f64, CancelToken) {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let cancel = CancelToken::new();
        self.inner
            .states
            .lock()
            .unwrap()
            .insert(id, SocketState {
                cancel: cancel.clone(),
                queue: Mutex::new(Vec::new()),
                finished: false,
            });
        (id as f64, cancel)
    }

    /// Queues one received message from the pump.
    pub fn push_message(&self, id: f64, data: Vec<u8>, is_binary: bool) -> bool {
        let mut states = self.inner.states.lock().unwrap();
        match states.get_mut(&(id as u64)) {
            Some(state) => {
                state
                    .queue
                    .lock()
                    .unwrap()
                    .push(QueuedMessage { data, is_binary });
                true
            }
            None => false,
        }
    }

    /// Takes one queued message; `None` when the queue is empty.
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

    /// Fires the cancel token.
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
