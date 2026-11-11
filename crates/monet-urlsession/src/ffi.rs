//! The cxx bridge between Rust and the Swift shim.
//!
//! Two planes:
//! - **Data plane** (`extern "Rust"`): implemented in Rust, called from
//!   Swift — response head, headers, chunks, terminal events. High
//!   frequency.
//! - **Control plane** (`extern "C++"`): declared here, implemented in
//!   `cpp/trampoline.cpp` (which forwards into `@_expose(Cxx)` Swift
//!   functions) — request assembly and lifecycle. Low frequency.
//!
//! Event ordering contract (per request, all events from the session's
//! serial delegate queue):
//! 1. zero or more `on_redirect` (only for redirects being followed),
//! 2. zero or more `on_header`,
//! 3. exactly one `on_response_head` — the head is complete; this is what
//!    resolves the execute future,
//! 4. zero or more `on_chunk`,
//! 5. exactly one terminal event: `on_done` or `on_error`.
//!
//! Control-plane ordering: `transport_add_header` / `transport_set_body`
//! assemble the request under an id; `transport_start` sends it (Swift
//! builds the URLRequest and resumes the task). A request cancelled
//! between assembly and start is discarded silently by Swift — no events
//! fire — which is why Rust's cancel path also drops the registry entry.
//! `transport_cancel` on a running task surfaces as `on_error` with code
//! -999 (`NSURLErrorCancelled`), which maps to `Error::Cancelled`.

use crate::pin;
use crate::registry;
use crate::roots;
use crate::upload;
use crate::ws;

// ---- data plane: Swift calls these through the generated header ----

/// Appends one response header. Invalid names or values are dropped,
/// matching monet-cronet's tolerance for odd server headers.
fn on_header(id: u64, name: &[u8], value: &[u8]) {
    registry::on_header(id, name, value);
}

/// Completes the response head: assembles the accumulated headers into a
/// [`monet_core::Response`] and resolves the execute future.
fn on_response_head(id: u64, status: u16) {
    registry::on_response_head(id, status);
}

/// Delivers one body chunk. `true` means paused (v2/W2b): the bounded
/// channel is full and Swift must suspend the task; the body watcher
/// resumes it (`transport_resume`) once the consumer has drained.
fn on_chunk(id: u64, data: &[u8]) -> bool {
    registry::on_chunk(id, data)
}

/// Informational: fired for redirects that are being followed. v1 records
/// nothing; the event keeps the boundary complete for a future
/// redirect-hook surface.
fn on_redirect(_id: u64, _location: &[u8]) {}

/// Terminal success: completes the body channel and drops the state.
fn on_done(id: u64) {
    registry::on_done(id);
}

/// Terminal failure. `code` is the NSError code; -999
/// (`NSURLErrorCancelled`) maps to `Error::Cancelled`.
fn on_error(id: u64, code: i32, message: &[u8]) {
    registry::on_error(id, code, message);
}

/// The task holding a streamed upload finished — ends the feeder even
/// if the producer has nothing more to write (it would otherwise park
/// in the body stream forever; the write-failure path alone never
/// reaches a parked producer).
fn on_upload_done(id: u64) {
    upload::task_finished(id);
}

/// Pinning query for the server-trust challenge path: writes up to
/// `out.len() / 32` SPKI hashes into `out` and returns the count. Zero
/// means the host is not pinned.
fn pins_for_host(host: &[u8], out: &mut [u8]) -> usize {
    pin::hashes_for_host(host, out)
}

/// How many custom trust roots are registered (configureTrust) — the
/// challenge path's loop bound, and its "intercept at all" signal
/// alongside the pin query.
fn roots_count() -> usize {
    roots::count()
}

/// Copies root `index`'s DER into `out`, returning the bytes written;
/// an `out` too small writes nothing and returns the needed length
/// (Swift sizes its buffer with a first empty call).
fn root_at(index: usize, out: &mut [u8]) -> usize {
    roots::root_at(index, out)
}

// ---- data plane: WebSocket events ----

/// The handshake completed; resolves the connect future with the stream.
fn on_ws_open(id: u64) {
    ws::on_ws_open(id);
}

/// Delivers one text (`is_binary == false`) or binary message.
fn on_ws_message(id: u64, is_binary: bool, data: &[u8]) {
    ws::on_ws_message(id, is_binary, data);
}

/// Surfaces a received ping (the shim already echoed a pong).
fn on_ws_ping(id: u64, data: &[u8]) {
    ws::on_ws_ping(id, data);
}

/// Surfaces a received pong.
fn on_ws_pong(id: u64, data: &[u8]) {
    ws::on_ws_pong(id, data);
}

/// Terminal event. Before open, `error` carries the failure and the
/// connect future resolves with it; after open, `code != 0` means a clean
/// close (code + reason round-trip exactly), `code == 0` an abnormal
/// shutdown surfaced as `WsMessage::Close(None)`.
fn on_ws_close(id: u64, code: u16, reason: &[u8], was_clean: bool, error: &[u8]) {
    ws::on_ws_close(id, code, reason, was_clean, error);
}

#[allow(unsafe_code)]
#[cxx::bridge]
pub mod bridge {
    // ---- data plane: Rust implementations, Swift callers ----
    extern "Rust" {
        fn on_header(id: u64, name: &[u8], value: &[u8]);
        fn on_response_head(id: u64, status: u16);
        fn on_chunk(id: u64, data: &[u8]) -> bool;
        fn on_redirect(id: u64, location: &[u8]);
        fn on_done(id: u64);
        fn on_error(id: u64, code: i32, message: &[u8]);
        fn on_upload_done(id: u64);
        fn pins_for_host(host: &[u8], out: &mut [u8]) -> usize;
        fn roots_count() -> usize;
        fn root_at(index: usize, out: &mut [u8]) -> usize;
        fn on_ws_open(id: u64);
        fn on_ws_message(id: u64, is_binary: bool, data: &[u8]);
        fn on_ws_ping(id: u64, data: &[u8]);
        fn on_ws_pong(id: u64, data: &[u8]);
        fn on_ws_close(id: u64, code: u16, reason: &[u8], was_clean: bool, error: &[u8]);
    }

    // ---- control plane: C++ (trampoline) implementations, Rust callers ----
    unsafe extern "C++" {
        include!("monet-urlsession/shim.h");
        fn transport_add_header(id: u64, name: &[u8], value: &[u8]);
        fn transport_set_body(id: u64, data: &[u8]);
        fn transport_start(
            id: u64,
            url: &[u8],
            method: &[u8],
            follow_redirects: bool,
            timeout_ms: u64,
        );
        fn transport_cancel(id: u64);
        fn transport_resume(id: u64);
        // Streaming uploads (v2/W2c): mark the request streamed before
        // start (the pipe exists from start on), then feed/finish/fail
        // the pipe's output half.
        fn transport_set_streaming(id: u64);
        fn transport_upload_write(id: u64, data: &[u8]) -> bool;
        fn transport_upload_finish(id: u64);
        fn transport_upload_fail(id: u64);
        fn ws_add_header(id: u64, name: &[u8], value: &[u8]);
        fn ws_open(id: u64, url: &[u8]);
        fn ws_send(id: u64, is_binary: bool, data: &[u8]);
        fn ws_send_ping(id: u64, data: &[u8]);
        fn ws_send_pong(id: u64, data: &[u8]);
        fn ws_close(id: u64, code: u16, reason: &[u8]);
        fn ws_cancel(id: u64);
    }
}
