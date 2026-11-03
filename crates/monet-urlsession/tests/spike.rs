//! M1 spike: prove both directions of the Rust↔Swift bridge.
//!
//! `monet_swift_ping(41)` goes Rust → trampoline (C++) → Swift, and the
//! Swift body calls back into Rust (`monet_pong`) before returning — so
//! a result of 42 means the full round trip works, including the
//! build orchestration that stitches the three compilers together.

#[test]
fn swift_round_trip() {
    let result = monet_urlsession::bridge::monet_swift_ping(41);
    assert_eq!(result, 42);
}
