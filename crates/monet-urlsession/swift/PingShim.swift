// M1 spike: the minimal Swift side of the round trip.
//
// `monetSwiftPing` is exposed to C++ (via -emit-clang-header-path) so the
// trampoline can call it; its body calls back into Rust through the cxx
// module (`monet_pong`), proving both directions.

import Foundation
import monet_urlsession_bridge

@_expose(Cxx)
public func monetSwiftPing(id: UInt64) -> UInt64 {
    let pong = monet_pong(id)
    return pong &+ 1
}
