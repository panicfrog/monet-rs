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
use std::task::{Poll, Waker};

use jsi::JsiArrayBuffer;
use jsi::RuntimeHandle;
use monet_core::CancelToken;

/// The JS-facing queue's high watermark (v2/W2b): at this many queued
/// chunks the pump stops pulling the transport body. With pump-side
/// chunks of 16–64KB the bound is ~256KB in flight per fetch — a slow
/// consumer bounds memory, it does not buffer the whole response.
const QUEUE_HIGH: usize = 4;

/// The resume watermark: takes bring the queue down to this before the
/// pump pulls again (hysteresis — resume at 2, pause at 4, no
/// thrash around one boundary).
const QUEUE_LOW: usize = 2;

struct FetchState {
    /// The fetch's cancel token — cloned into the transport call; abort
    /// fires it.
    cancel: CancelToken,
    /// Queued chunks waiting for `take_chunk` (FIFO — body order is
    /// meaningful), plus the pump's waker while it waits for capacity.
    queue: Mutex<ChunkQueue>,
    /// Set at termination (on_done / on_error fired).
    finished: bool,
}

struct ChunkQueue {
    chunks: VecDeque<Vec<u8>>,
    /// The pump's waker while it waits out a full queue; `take_chunk`
    /// wakes it at the low watermark.
    capacity_waker: Option<Waker>,
}

#[derive(Clone)]
pub struct FetchRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    next_id: AtomicU64,
    states: Mutex<HashMap<u64, FetchState>>,
    /// The upload feeds of streaming-body fetches (fetch_stream): JS
    /// pushes chunks through the module methods; the pump's request
    /// carries the receiving half. Dropped by completion, error, and
    /// the fetch's terminal cleanup alike.
    stream_feeds: Mutex<HashMap<u64, monet_core::body::BodySender>>,
}

impl FetchRegistry {
    pub fn new() -> Self {
        FetchRegistry {
            inner: Arc::new(Inner {
                next_id: AtomicU64::new(0),
                states: Mutex::new(HashMap::new()),
                stream_feeds: Mutex::new(HashMap::new()),
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
                queue: Mutex::new(ChunkQueue {
                    chunks: VecDeque::new(),
                    capacity_waker: None,
                }),
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
                state.queue.lock().unwrap().chunks.push_back(chunk);
                true
            }
            None => false,
        }
    }

    /// Waits until the fetch's queue is below the high watermark — the
    /// pump's pull gate (v2/W2b). `false` when the state is gone (the
    /// fetch was cleaned up — stop pulling). The pump races this with
    /// its cancel token: an aborted fetch with a full queue must not
    /// wait for takes that will never come.
    pub async fn wait_for_capacity(&self, id: f64) -> bool {
        std::future::poll_fn(|cx| {
            let states = self.inner.states.lock().unwrap();
            let Some(state) = states.get(&f64_to_u64(id)) else {
                return Poll::Ready(false);
            };
            let mut queue = state.queue.lock().unwrap();
            if queue.chunks.len() < QUEUE_HIGH {
                return Poll::Ready(true);
            }
            queue.capacity_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Takes one queued chunk as a zero-copy ArrayBuffer; `None` when
    /// the queue is empty (check `is_done` to tell "no data yet" from
    /// "finished"). A take that brings the queue to the low watermark
    /// wakes the pump — the gate's resume edge.
    pub fn take_chunk<'rt>(
        &self,
        id: f64,
        rt: &mut RuntimeHandle<'rt>,
    ) -> Option<JsiArrayBuffer<'rt>> {
        let woken = {
            let states = self.inner.states.lock().unwrap();
            let state = states.get(&f64_to_u64(id))?;
            let mut queue = state.queue.lock().unwrap();
            let taken = queue.chunks.pop_front();
            let waker = if queue.chunks.len() <= QUEUE_LOW {
                queue.capacity_waker.take()
            } else {
                None
            };
            (taken, waker)
        };
        // Wake outside the locks: the pump's re-poll takes them itself.
        if let Some(waker) = woken.1 {
            waker.wake();
        }
        woken.0.map(|chunk| JsiArrayBuffer::from_vec(chunk, rt))
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
            Some(state) => state.finished && state.queue.lock().unwrap().chunks.is_empty(),
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
        // Wakes a pump parked on capacity so it can observe the removal
        // — it would otherwise wait for a take that can no longer come.
        let waker = {
            let mut states = self.inner.states.lock().unwrap();
            states
                .remove(&f64_to_u64(id))
                .and_then(|state| state.queue.lock().unwrap().capacity_waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// ---- streaming uploads (fetch_stream) ----

    /// Stores the sending half of a streaming body; the pump carries the
    /// receiving half in its request.
    pub(crate) fn attach_stream_feed(
        &self,
        id: f64,
        feed: monet_core::body::BodySender,
    ) {
        self.inner
            .stream_feeds
            .lock()
            .unwrap()
            .insert(f64_to_u64(id), feed);
    }

    /// Pushes one JS-fed chunk into the upload stream; `false` when the
    /// feed is gone (already completed, errored, or the fetch cleaned
    /// up — a late producer chunk is dropped, not fatal).
    pub(crate) fn stream_chunk(&self, id: f64, chunk: Vec<u8>) -> bool {
        let feeds = self.inner.stream_feeds.lock().unwrap();
        match feeds.get(&f64_to_u64(id)) {
            Some(feed) => feed
                .send_chunk(chunk)
                .map_err(|_| ())
                .is_ok(),
            None => false,
        }
    }

    /// Ends the upload stream successfully; `false` when the feed is
    /// gone (idempotent-ish: a second complete is a no-op).
    pub(crate) fn stream_complete(&self, id: f64) -> bool {
        self.inner
            .stream_feeds
            .lock()
            .unwrap()
            .remove(&f64_to_u64(id))
            .is_some()
    }

    /// Ends the upload stream with an error (the JS producer threw).
    pub(crate) fn stream_error(&self, id: f64, error: monet_core::Error) -> bool {
        let mut feeds = self.inner.stream_feeds.lock().unwrap();
        match feeds.remove(&f64_to_u64(id)) {
            Some(feed) => {
                feed.abort(error);
                true
            }
            None => false,
        }
    }
}

/// JS numbers are f64; ids are small integers so the mapping is exact.
fn f64_to_u64(id: f64) -> u64 {
    id as u64
}
