//! The shared conformance battery against monet-ws — the same checks
//! every WebSocket transport runs (see monet-conformance).

use monet_conformance::ws;
use monet_ws::{MonetWsTransport, WsConfig};

#[test]
fn websocket_conformance() {
    let transport = MonetWsTransport::new(WsConfig::default()).unwrap();
    ws::run_suite(&transport);
    ws::client_close_reaches_server(&transport);
    ws::ping_surfaced_and_auto_ponged(&transport);
}
