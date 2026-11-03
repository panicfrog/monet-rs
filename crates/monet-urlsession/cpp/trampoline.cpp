// M1 spike: the control-plane trampoline. cxx declares `monet_swift_ping`
// as extern "C++"; we implement it by forwarding to the Swift function
// exposed through the generated clang header.

#include <cstdint>

#include "MonetUrlsessionBridge-Swift.h"

uint64_t monet_swift_ping(uint64_t id) {
    return MonetUrlsessionBridge::monetSwiftPing(id: id);
}
