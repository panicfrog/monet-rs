#pragma once

#include <cstdint>

#include "rust/cxx.h"

// Control plane: implemented in cpp/trampoline.cpp (forwarding into the
// Swift shim), declared here for the cxx-generated glue to link against.
// Ordering contract in src/ffi.rs: add_header/set_body assemble, start
// sends, cancel aborts (and a never-started request is discarded
// silently).

void transport_add_header(
    std::uint64_t id,
    rust::Slice<const std::uint8_t> name,
    rust::Slice<const std::uint8_t> value);

void transport_set_body(std::uint64_t id, rust::Slice<const std::uint8_t> data);

void transport_start(
    std::uint64_t id,
    rust::Slice<const std::uint8_t> url,
    rust::Slice<const std::uint8_t> method,
    bool follow_redirects,
    std::uint64_t timeout_ms);

void transport_cancel(std::uint64_t id);

// Resumes a task suspended by the body backpressure valve (v2/W2b):
// the Rust body watcher calls it after the consumer has drained the
// overflow. A no-op on finished tasks.
void transport_resume(std::uint64_t id);

// Control plane, WebSocket: same assemble-then-start ordering as HTTP
// (add_header first, open sends). ws_close with code 0 sends a close
// frame without a payload; ws_cancel force-cancels the connection.

void ws_add_header(std::uint64_t id, rust::Slice<const std::uint8_t> name, rust::Slice<const std::uint8_t> value);

void ws_open(std::uint64_t id, rust::Slice<const std::uint8_t> url);

void ws_send(std::uint64_t id, bool is_binary, rust::Slice<const std::uint8_t> data);

void ws_send_ping(std::uint64_t id, rust::Slice<const std::uint8_t> data);

void ws_send_pong(std::uint64_t id, rust::Slice<const std::uint8_t> data);

void ws_close(std::uint64_t id, std::uint16_t code, rust::Slice<const std::uint8_t> reason);

void ws_cancel(std::uint64_t id);
