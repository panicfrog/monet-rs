//! Streaming-upload feed (v2/W2c): the Rust half of a streamed request
//! body. The Swift shim owns a bound stream pair per streamed request —
//! URLSession reads the input half as the request body (the task's
//! `needNewBodyStream` callback hands it over), and the feeder (a
//! parked thread, the registry watcher's approach) pushes the request
//! [`Body`] into the output half through the control plane. The pair's
//! buffer is the upload's natural bound: a full buffer parks the write
//! until URLSession drains it.
//!
//! The feeder ends when the body ends (a clean finish), the body errors
//! (the task is cancelled and the cancellation remapped — the pinning
//! rejection's pattern), the request is cancelled, or the task finished
//! early (the data-plane `on_upload_done` wake — a server that answers
//! mid-upload must not leave the feeder parked in the body stream
//! forever).

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};

use monet_core::body::Body;
use monet_core::cancel::CancelToken;
use monet_core::error::Result;

use crate::bridge;

static DONE_TXS: LazyLock<Mutex<HashMap<u64, UnboundedSender<()>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Registers the feeder's done wake (the data plane fires it when the
/// task holding the upload finishes).
fn arm(id: u64) -> UnboundedReceiver<()> {
    let (tx, rx) = unbounded();
    DONE_TXS.lock().unwrap().insert(id, tx);
    rx
}

/// Drops the done wake (the feeder's exit — a later task event finds
/// nothing and no-ops).
fn disarm(id: u64) {
    DONE_TXS.lock().unwrap().remove(&id);
}

/// The task finished: wake the feeder. It may be parked in the body
/// stream with nothing left to write — the write-failure path alone
/// would never reach it.
pub(crate) fn task_finished(id: u64) {
    let tx = {
        let mut done = DONE_TXS.lock().unwrap();
        done.remove(&id)
    };
    if let Some(tx) = tx {
        let _ = tx.unbounded_send(());
    }
}

/// The feeder: pushes the request [`Body`] into the upload pipe chunk
/// by chunk until the body ends or errors, the request is cancelled, or
/// the task finished early. `drained` is what [`Body::take_complete`]
/// already pulled — it leads the stream.
pub(crate) fn spawn_feeder(id: u64, mut body: Body, cancel: CancelToken, drained: Vec<u8>) {
    let mut done_rx = arm(id);
    let spawned = std::thread::Builder::new()
        .name("monet-urlsession-upload".into())
        .spawn(move || {
            if !drained.is_empty() && !bridge::transport_upload_write(id, &drained) {
                // The pipe is already gone (the task ended before the
                // first write): nothing to feed.
                disarm(id);
                return;
            }
            futures_lite::future::block_on(async {
                use futures_lite::StreamExt;
                loop {
                    enum Step {
                        Body(Option<Result<Bytes>>),
                        Cancelled,
                        Done,
                    }
                    let step = futures_lite::future::race(
                        async { Step::Body(body.next().await) },
                        futures_lite::future::race(
                            async {
                                cancel.cancelled().await;
                                Step::Cancelled
                            },
                            async {
                                done_rx.next().await;
                                Step::Done
                            },
                        ),
                    )
                    .await;
                    match step {
                        Step::Body(Some(Ok(bytes))) => {
                            if !bridge::transport_upload_write(id, &bytes) {
                                // The pipe ended (the task is gone): stop
                                // feeding — the rest of the body stays
                                // with the producer.
                                return;
                            }
                        }
                        Step::Body(Some(Err(_))) => {
                            bridge::transport_upload_fail(id);
                            return;
                        }
                        Step::Body(None) => {
                            bridge::transport_upload_finish(id);
                            return;
                        }
                        // Cancelled: execute's cancel path already fired
                        // transport_cancel; Done: the task's terminal
                        // arrived. Either way the feeding is over.
                        Step::Cancelled | Step::Done => return,
                    }
                }
            });
            disarm(id);
        });
    if spawned.is_err() {
        // Thread-starved: fail the upload rather than leave the task
        // waiting on a body stream that never comes.
        bridge::transport_upload_fail(id);
        disarm(id);
    }
}
