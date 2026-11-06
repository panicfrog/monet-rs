//! The fetch registry: fetchId → in-flight state, shared between the
//! module methods (JS thread) and the pump tasks (executor threads).
//!
//! The protocol (see the plan): the pump awaits the transport future,
//! fires `on_head`, then pushes body chunks into a queue, signalling
//! `on_chunk` each time; JS pulls with `take_chunk` (zero-copy
//! ArrayBuffer). `abort` cancels the transport token — the pump observes
//! the error and reports `on_error(Cancelled)`. A terminal event marks
//! the state finished; `body_done` = finished && queue drained.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jsi::JsiArrayBuffer;
use jsi::RuntimeHandle;
use monet_core::CancelToken;

struct FetchState {
    /// The fetch's cancel token — cloned into the transport call; abort
    /// fires it.
    cancel: CancelToken,
    /// Queued chunks waiting for `take_chunk` (FIFO — body order is
    /// meaningful).
    queue: Mutex<VecDeque<Vec<u8>>>,
    /// Set at termination (on_done / on_error fired).
    finished: bool,
}

#[derive(Clone)]
pub struct FetchRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    states: Mutex<HashMap<u64, FetchState>>,
}

impl FetchRegistry {
    pub fn new() -> Self {
        FetchRegistry {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(0),
                states: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Allocates a fetch: the id (surfaced to JS as f64) and the cancel
    /// token to wire into the transport call.
    pub fn allocate(&self) -> (f64, CancelToken) {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let cancel = CancelToken::new();
        self.inner.states.lock().unwrap().insert(
            id,
            FetchState {
                cancel: cancel.clone(),
                queue: Mutex::new(VecDeque::new()),
                finished: false,
            },
        );
        (id as f64, cancel)
    }

    /// Queues one chunk from the pump; `false` when the fetch state is
    /// gone (a very late chunk after cleanup — the pump stops pulling).
    pub fn push_chunk(&self, id: f64, chunk: Vec<u8>) -> bool {
        let mut states = self.inner.states.lock().unwrap();
        match states.get_mut(&f64_to_u64(id)) {
            Some(state) => {
                state.queue.lock().unwrap().push_back(chunk);
                true
            }
            None => false,
        }
    }

    /// Takes one queued chunk as a zero-copy ArrayBuffer; `None` when
    /// the queue is empty (check `is_done` to tell "no data yet" from
    /// "finished").
    pub fn take_chunk<'rt>(
        &self,
        id: f64,
        rt: &mut RuntimeHandle<'rt>,
    ) -> Option<JsiArrayBuffer<'rt>> {
        let states = self.inner.states.lock().unwrap();
        let state = states.get(&f64_to_u64(id))?;
        let mut queue = state.queue.lock().unwrap();
        queue
            .pop_front()
            .map(|chunk| JsiArrayBuffer::from_vec(chunk, rt))
    }

    /// Marks the fetch terminated (the terminal callback has fired).
    pub fn finish(&self, id: f64) {
        let mut states = self.inner.states.lock().unwrap();
        if let Some(state) = states.get_mut(&f64_to_u64(id)) {
            state.finished = true;
        }
    }

    /// `true` when the fetch terminated and the queue is drained — the
    /// end-of-stream signal for JS's pull loop.
    pub fn is_done(&self, id: f64) -> bool {
        let states = self.inner.states.lock().unwrap();
        match states.get(&f64_to_u64(id)) {
            Some(state) => state.finished && state.queue.lock().unwrap().is_empty(),
            None => true,
        }
    }

    /// Fires the cancel token; the pump reports on_error(Cancelled).
    /// `false` when the id is unknown (already finished).
    pub fn abort(&self, id: f64) -> bool {
        let states = self.inner.states.lock().unwrap();
        match states.get(&f64_to_u64(id)) {
            Some(state) => {
                state.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Drops a finished fetch's state (JS signals it's done pulling).
    /// Idempotent; late events on the id are ignored afterwards.
    pub fn cleanup(&self, id: f64) {
        self.inner.states.lock().unwrap().remove(&f64_to_u64(id));
    }
}

/// JS numbers are f64; ids are small integers so the mapping is exact.
fn f64_to_u64(id: f64) -> u64 {
    id as u64
}
