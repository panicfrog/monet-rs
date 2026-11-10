//! The battery target: the conformance HTTP + WebSocket servers on fixed,
//! externally reachable addresses, so the JS battery in the rn-demo app
//! (and any device on the LAN) hits the exact routes the Rust battery
//! tests against. A TLS pair (HTTPS + WSS, the fixture CA) rides along by
//! default — the on-device TLS/pins battery targets it.
//!
//! Run: cargo run -p monet-demo --bin server [--addr <ip:port>]

use std::time::Duration;

use monet_conformance::http::HttpServer;
use monet_conformance::tls::{LocalCa, spawn_https_on};
use monet_conformance::ws::PathEchoServer;

/// The fixture CA the runtime batteries also use (see
/// `crates/monet-demo/src/fixtures/`): built into the binary so the TLS
/// pair works from any working directory.
const CA_DER: &[u8] = include_bytes!("../fixtures/ca.der");
const CA_KEY: &[u8] = include_bytes!("../fixtures/ca.pkcs8.key");

fn main() {
    // `--addr <a>` or a bare positional; default binds every interface so
    // emulators (10.0.2) and LAN devices reach the same routes as the host.
    let mut args = std::env::args().skip(1);
    let mut bind = String::new();
    let mut ws_bind = String::new();
    let mut no_tls = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => {
                bind = args
                    .next()
                    .unwrap_or_else(|| usage_exit("--addr needs a value"))
            }
            "--ws-addr" => {
                ws_bind = args
                    .next()
                    .unwrap_or_else(|| usage_exit("--ws-addr needs a value"))
            }
            "--no-tls" => no_tls = true,
            other => bind = other.to_string(),
        }
    }
    if bind.is_empty() {
        bind = "0.0.0.0:8964".to_string();
    }
    if ws_bind.is_empty() {
        ws_bind = "0.0.0.0:8965".to_string();
    }

    let server = HttpServer::spawn_on(&bind);
    let addr = server.local_addr();
    let ws = PathEchoServer::spawn_on(&ws_bind);
    println!("monet-demo server — the battery target (same routes as the Rust battery)");
    println!(
        "  host / iOS simulator : http://127.0.0.1:{}  ws://127.0.0.1:{}",
        addr.port(),
        ws.port()
    );
    println!(
        "  Android emulator     : http://10.0.2.2:{}  ws://10.0.2.2:{}",
        addr.port(),
        ws.port()
    );
    if let Some(lan) = lan_ip() {
        println!(
            "  LAN (real devices)   : http://{lan}:{}  ws://{lan}:{}",
            addr.port(),
            ws.port()
        );
    }
    if no_tls {
        println!("TLS pair disabled (--no-tls)");
    } else {
        // The leaf's SANs must include a name a device can both resolve
        // AND key pins on — monet-core's pin config takes hostnames, not
        // IPs, so the Bonjour name (mDNS-resolvable by every Apple device
        // on the LAN) is the pin target; the loopback names stay in the
        // SANs (from_pkcs8_for_hosts adds them) for host and simulator use.
        let hostname = detect_hostname();
        let hosts: Vec<String> = hostname.clone().into_iter().collect();
        let ca = LocalCa::from_pkcs8_for_hosts(CA_DER.to_vec(), CA_KEY, &hosts);
        let https = spawn_https_on(&ca, "0.0.0.0:8966");
        let wss = PathEchoServer::spawn_tls_on("0.0.0.0:8967", &ca);
        println!(
            "TLS pair — the fixture CA; install crates/monet-demo/src/fixtures/ca.pem on the device, then trust it in Settings:"
        );
        match &hostname {
            Some(name) => println!(
                "  device (mDNS name)        : https://{name}:{}  wss://{name}:{}",
                https.local_addr().port(),
                wss.port()
            ),
            None => println!("  (hostname undetectable — leaf SANs cover loopback only)"),
        }
        if let Some(lan) = lan_ip() {
            println!(
                "  device (LAN IP, trust only): https://{lan}:{}  wss://{lan}:{}  (pin keys must be hostnames)",
                https.local_addr().port(),
                wss.port()
            );
        }
        // Android's TLS battery base is https://localhost (pin keys must
        // be hostnames; 10.0.2.2 is IP-shaped and rejected) — which only
        // resolves through adb reverse, on real devices AND emulators
        // (the reverse set dies with every device reboot).
        println!(
            "  adb reverse (Android, TLS battery): for p in 8964 8965 8966 8967; do adb reverse tcp:$p tcp:$p; done"
        );
        let spki: String = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(ca.leaf_spki_sha256())
        };
        println!("  leaf SPKI SHA-256 (base64; also served at /leaf-spki):");
        println!("    {spki}");
        if let Some(name) = &hostname {
            println!(
                "  pin config (hostname keys only): {{\"{name}\": {{\"publicKeyHashes\": [\"{spki}\"]}}}}"
            );
        }
    }
    println!(
        "http routes: /plain /ws-open /chunked /auth-guard /echo /redirect /slow-head /stalled /held /fast /leaf-spki"
    );
    println!(
        "ws routes: /echo (mirror) /close-1012 (fidelity) /ping (probe) /gc-probe (counted) /hook-echo (hook check)"
    );

    // The server threads keep themselves alive; main must not return or
    // the process exits. Ctrl-C terminates.
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// The host's Bonjour name (`hostname`, e.g. `machine.local`) — the name
/// LAN Apple devices resolve via mDNS, and the only device-reachable name
/// a pin config can key on.
fn detect_hostname() -> Option<String> {
    let out = std::process::Command::new("hostname").output().ok()?;
    if !out.status.success() {
        return None;
    }
    let name = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!name.is_empty()).then_some(name)
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
    eprintln!(
        "usage: server [--addr <ip:port>] [--ws-addr <ip:port>] [--no-tls]  (defaults 0.0.0.0:8964 / :8965, TLS on :8966 / :8967)"
    );
    std::process::exit(2);
}
