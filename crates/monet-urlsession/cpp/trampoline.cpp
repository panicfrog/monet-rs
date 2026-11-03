// Control-plane trampoline: cxx declares the functions below as
// extern "C++"; each forwards into the Swift shim through the clang
// header swiftc emits (-emit-clang-header-path). Pointers and lengths
// cross as plain values; nothing here touches the data plane.

#include <cstdint>

#include "rust/cxx.h"
#include "MonetUrlsessionBridge-Swift.h"

void transport_add_header(
    std::uint64_t id,
    rust::Slice<const std::uint8_t> name,
    rust::Slice<const std::uint8_t> value) {
  MonetUrlsessionBridge::monetTransportAddHeader(
      id, name.data(), name.size(), value.data(), value.size());
}

void transport_set_body(std::uint64_t id, rust::Slice<const std::uint8_t> data) {
  MonetUrlsessionBridge::monetTransportSetBody(id, data.data(), data.size());
}

void transport_start(
    std::uint64_t id,
    rust::Slice<const std::uint8_t> url,
    rust::Slice<const std::uint8_t> method,
    bool follow_redirects,
    std::uint64_t timeout_ms) {
  MonetUrlsessionBridge::monetTransportStart(
      id, url.data(), url.size(), method.data(), method.size(),
      follow_redirects, timeout_ms);
}

void transport_cancel(std::uint64_t id) {
  MonetUrlsessionBridge::monetTransportCancel(id);
}
