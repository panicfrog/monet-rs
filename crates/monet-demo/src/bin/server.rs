//! The battery target: the conformance HTTP server on a fixed, externally
//! reachable address, so the JS battery in the rn-demo app (and any device
//! on the LAN) hits the exact routes the Rust battery tests against.
//!
//! Run: cargo run -p monet-demo --bin server [--addr 0.0.0.0:8964]

use std::time::Duration;

use monet_conformance::http::HttpServer;

fn main() {
    // `--addr <a>` or a bare positional; default binds every interface so
    // emulators (10.0.2.2) and LAN devices reach the same routes.
    let mut args = std::env::args().skip(1);
    let mut bind = String::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                bind = args.next().unwrap_or_else(|| usage_exit("--addr needs a value"))
            }
            other => bind = other.to_string(),
        }
    }
    if bind.is_empty() {
        bind = "0.0.0.0:8964".to_string();
    }

    let server = HttpServer::spawn_on(&bind);
    let addr = server.local_addr();
    println!("monet-demo server — the battery target (same routes as the Rust battery)");
    println!("  host / iOS simulator : http://127.0.0.1:{}", addr.port());
    println!("  Android emulator     : http://10.0.2.2:{}", addr.port());
    if let Some(lan) = lan_ip() {
        println!("  LAN (real devices)   : http://{lan}:{}", addr.port());
    }
    println!("routes: /plain /chunked /echo /redirect /slow-head /stalled /held /fast");

    // The server thread keeps itself alive; main must not return or the
    // process exits. Ctrl-C terminates.
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// The host's outbound-interface IP — the address LAN devices should dial.
/// (A UDP connect picks a route without sending anything.)
fn lan_ip() -> Option<std::net::IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    Some(socket.local_addr().ok()?.ip())
}

fn usage_exit(message: &str) -> ! {
    eprintln!("server: {message}");
    eprintln!("usage: server [--addr <ip:port>]  (default 0.0.0.0:8964)");
    std::process::exit(2);
}
