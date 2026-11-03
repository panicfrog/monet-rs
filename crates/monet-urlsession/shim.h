#pragma once

#include <cstdint>

// Control plane: implemented in cpp/trampoline.cpp (forwarding into the
// Swift shim), declared here for the cxx-generated glue to link against.
std::uint64_t monet_swift_ping(std::uint64_t id);
