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

/// Delivers one body chunk.
fn on_chunk(id: u64, data: &[u8]) {
    registry::on_chunk(id, data);
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

/// Pinning query for the server-trust challenge path: writes up to
/// `out.len() / 32` SPKI hashes into `out` and returns the count. Zero
/// means the host is not pinned.
fn pins_for_host(host: &[u8], out: &mut [u8]) -> usize {
    pin::hashes_for_host(host, out)
}

#[allow(unsafe_code)]
#[cxx::bridge]
pub mod bridge {
    // ---- data plane: Rust implementations, Swift callers ----
    extern "Rust" {
        fn on_header(id: u64, name: &[u8], value: &[u8]);
        fn on_response_head(id: u64, status: u16);
        fn on_chunk(id: u64, data: &[u8]);
        fn on_redirect(id: u64, location: &[u8]);
        fn on_done(id: u64);
        fn on_error(id: u64, code: i32, message: &[u8]);
        fn pins_for_host(host: &[u8], out: &mut [u8]) -> usize;
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
    }
}
