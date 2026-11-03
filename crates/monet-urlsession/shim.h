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
