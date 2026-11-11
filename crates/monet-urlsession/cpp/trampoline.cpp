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

void transport_resume(std::uint64_t id) {
  MonetUrlsessionBridge::monetTransportResume(id);
}

void transport_set_streaming(std::uint64_t id) {
  MonetUrlsessionBridge::monetTransportSetStreaming(id);
}

bool transport_upload_write(
    std::uint64_t id, rust::Slice<const std::uint8_t> data) {
  return MonetUrlsessionBridge::monetTransportUploadWrite(
      id, data.data(), data.size());
}

void transport_upload_finish(std::uint64_t id) {
  MonetUrlsessionBridge::monetTransportUploadFinish(id);
}

void transport_upload_fail(std::uint64_t id) {
  MonetUrlsessionBridge::monetTransportUploadFail(id);
}

void ws_add_header(
    std::uint64_t id,
    rust::Slice<const std::uint8_t> name,
    rust::Slice<const std::uint8_t> value) {
  MonetUrlsessionBridge::monetWsAddHeader(
      id, name.data(), name.size(), value.data(), value.size());
}

void ws_open(std::uint64_t id, rust::Slice<const std::uint8_t> url) {
  MonetUrlsessionBridge::monetWsOpen(id, url.data(), url.size());
}

void ws_send(std::uint64_t id, bool is_binary, rust::Slice<const std::uint8_t> data) {
  MonetUrlsessionBridge::monetWsSend(id, is_binary, data.data(), data.size());
}

void ws_send_ping(std::uint64_t id, rust::Slice<const std::uint8_t> data) {
  MonetUrlsessionBridge::monetWsSendPing(id, data.data(), data.size());
}

void ws_send_pong(std::uint64_t id, rust::Slice<const std::uint8_t> data) {
  MonetUrlsessionBridge::monetWsSendPong(id, data.data(), data.size());
}

void ws_close(std::uint64_t id, std::uint16_t code, rust::Slice<const std::uint8_t> reason) {
  MonetUrlsessionBridge::monetWsClose(id, code, reason.data(), reason.size());
}

void ws_cancel(std::uint64_t id) {
  MonetUrlsessionBridge::monetWsCancel(id);
}
