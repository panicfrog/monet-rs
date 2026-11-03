//! The cxx bridge between Rust and the Swift shim.
//!
//! Two planes:
//! - **Data plane** (`extern "Rust"`): implemented in Rust, called from
//!   Swift — response heads, headers, chunks, terminal events. High
//!   frequency.
//! - **Control plane** (`extern "C++"`): declared here, implemented in
//!   `cpp/trampoline.cpp` (which forwards into `@_expose(Cxx)` Swift
//!   functions) — start/cancel/WS operations. Low frequency.

#[cxx::bridge]
pub mod ffi {
    // ---- data plane: Rust implementations, Swift callers ----
    extern "Rust" {
        /// M1 spike: Swift calls this through the generated header and
        /// returns the result, proving the Swift -> Rust direction.
        fn monet_pong(id: u64) -> u64;
    }

    // ---- control plane: C++ (trampoline) implementations, Rust callers ----
    unsafe extern "C++" {
        /// M1 spike: Rust calls through the trampoline into Swift, which
        /// calls back into Rust — proving the full round trip.
        fn monet_swift_ping(id: u64) -> u64;
    }
}
