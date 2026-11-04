//! TLS scenarios: a locally generated CA, an HTTPS server (same routes as
//! [`crate::http`]), and a WSS echo server — plus the checks that prove a
//! transport honors trust roots and pin policy over a real handshake.
//!
//! Everything runs against 127.0.0.1 with certificates minted on the
//! spot, so the same code verifies trust on a host test and inside an
//! Android app process (the demo rig) with no external coordination.

use std::sync::Arc;

use futures_lite::StreamExt;
use monet_core::error::Error;
use monet_core::method::Method;
use monet_core::request::Request;
use monet_core::transport::Transport;
use monet_core::{CancelToken, Url};
use tokio_rustls::TlsAcceptor;

use crate::http::HttpServer;
use crate::tls::private::ServerCert;
use crate::wait_for;
use base64::Engine as _;

/// A CA and a server certificate for `127.0.0.1`, minted on construction.
pub struct LocalCa {
    cert: ServerCert,
}

impl LocalCa {
    /// Generates the CA plus a leaf certificate for loopback.
    pub fn generate() -> LocalCa {
        LocalCa {
            cert: ServerCert::generate(),
        }
    }

    /// Builds on a pre-existing CA (DER certificate + PKCS#8 key) — for
    /// fixture CAs that must be trusted somewhere outside this process
    /// too, like an Android `network_security_config` trust anchor.
    pub fn from_pkcs8(ca_der: Vec<u8>, ca_key_pkcs8: &[u8]) -> LocalCa {
        LocalCa {
            cert: ServerCert::from_pkcs8(ca_der, ca_key_pkcs8),
        }
    }

    /// The DER root for [`monet_tls::TlsTrust::CustomRoots`].
    pub fn der(&self) -> Vec<u8> {
        self.cert.ca_der.clone()
    }

    /// The SPKI SHA-256 of the leaf — the value a pin config would carry
    /// for this server (and the value a mismatch check perturbs).
    pub fn leaf_spki_sha256(&self) -> [u8; 32] {
        self.cert.leaf_spki_sha256
    }
}

/// Spawns the HTTPS variant of the conformance HTTP server: identical
/// routes and observations, TLS-encrypted with `ca`'s leaf certificate.
pub fn spawn_https_server(ca: &LocalCa) -> HttpServer {
    let acceptor = TlsAcceptor::from(Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("rustls protocol versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![self::private::leaf_der(&ca.cert)],
            ca.cert.leaf_key.clone_key(),
        )
        .expect("fresh leaf certificate is valid"),
    ));
    HttpServer::spawn_with(move |stream| {
        let acceptor = acceptor.clone();
        async move {
            acceptor
                .accept(stream)
                .await
                .map_err(|_| std::io::Error::other("tls"))
        }
    })
}

/// PEM ("-----BEGIN PRIVATE KEY-----…") or raw PKCS#8 DER → DER.
fn pkcs8_der(input: &[u8]) -> Vec<u8> {
    if input.starts_with(b"-----") {
        let text = String::from_utf8_lossy(input);
        let encoded: String = text
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        // A malformed fixture surfaces as a parse failure downstream
        // (the caller's expect), not a panic here.
        base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .unwrap_or_default()
    } else {
        input.to_vec()
    }
}

mod private {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use sha2::{Digest, Sha256};

    /// Everything needed to serve TLS: the CA (as DER for trust), the
    /// leaf (as DER for the handshake), its key, and the leaf's pinned
    /// SPKI hash.
    pub struct ServerCert {
        pub ca_der: Vec<u8>,
        pub leaf_spki_sha256: [u8; 32],
        pub leaf_key: PrivateKeyDer<'static>,
        leaf_der: CertificateDer<'static>,
    }

    impl ServerCert {
        pub fn generate() -> ServerCert {
            let ca_key = rcgen::KeyPair::generate().expect("generate CA key");
            let mut ca_params = rcgen::CertificateParams::new(vec![]).expect("CA params");
            // CA:TRUE, unconstrained — Android PKIX validates the chain
            // element against BasicConstraints; without it the leaf chain
            // is rejected even when the CA is a trust anchor (rustls's
            // webpki path does not check the anchor, so only the platform
            // verifier surfaces this).
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = ca_params.self_signed(&ca_key).expect("self-signed CA");
            let ca_der = ca.der().to_vec();
            Self::mint(ca, ca_key, ca_der)
        }

        pub fn from_pkcs8(ca_der: Vec<u8>, ca_key_pkcs8: &[u8]) -> ServerCert {
            // Fixture keys ship as PEM (openssl's default) or DER — accept
            // both, so a hand-generated fixture cannot panic the battery.
            let key_der = super::pkcs8_der(ca_key_pkcs8);
            let ca_key = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(
                &rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
                &rcgen::PKCS_ECDSA_P256_SHA256,
            )
            .expect("fixture CA key parses as PKCS#8 P-256");
            // Reconstruct an issuer from the certificate (same subject,
            // same key): leaves minted below chain to the original DER.
            let ca =
                rcgen::CertificateParams::from_ca_cert_der(&CertificateDer::from(ca_der.clone()))
                    .expect("fixture CA certificate parses")
                    .self_signed(&ca_key)
                    .expect("issuer reconstructs");
            Self::mint(ca, ca_key, ca_der)
        }

        fn mint(ca: rcgen::Certificate, ca_key: rcgen::KeyPair, ca_der: Vec<u8>) -> ServerCert {
            let leaf_key = rcgen::KeyPair::generate().expect("generate leaf key");
            let mut leaf_params =
                rcgen::CertificateParams::new(vec!["127.0.0.1".into(), "localhost".into()])
                    .expect("leaf params");
            // An explicit subject distinct from the issuer: PKIX treats a
            // subject==issuer leaf as self-signed and rejects the chain
            // (rustls's webpki path tolerates it, so only the platform
            // verifier surfaces the difference).
            leaf_params
                .distinguished_name
                .push(rcgen::DnType::CommonName, "monet-test-leaf");
            let leaf = leaf_params
                .signed_by(&leaf_key, &ca, &ca_key)
                .expect("leaf signed by CA");
            let leaf_der = leaf.der().clone();
            let spki = monet_tls::spki::spki_der(leaf_der.as_ref())
                .expect("leaf exposes SubjectPublicKeyInfo");
            ServerCert {
                ca_der,
                leaf_spki_sha256: Sha256::digest(spki).into(),
                leaf_key: PrivateKeyDer::Pkcs8(leaf_key.serialize_der().into()),
                leaf_der,
            }
        }
    }

    pub fn leaf_der(cert: &ServerCert) -> CertificateDer<'static> {
        cert.leaf_der.clone()
    }
}

/// HTTPS GET against a server whose CA the transport trusts: the plain
/// conformance checks, encrypted.
pub fn https_get_with_trusted_ca(t: &dyn Transport, ca: &LocalCa) {
    let server = spawn_https_server(ca);
    let url = server.url_on("https", "localhost", "/plain");
    let response = wait_for(
        "https response head",
        t.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap();
    assert_eq!(response.status, 200);
    let chunks = wait_for(
        "https body",
        response.body.map(|c| c.unwrap()).collect::<Vec<_>>(),
    );
    assert_eq!(chunks.concat(), b"plain".to_vec());
}

/// A transport trusting a *different* CA must fail the handshake —
/// trust roots are enforced, not advisory.
pub fn https_untrusted_ca_rejects(t: &dyn Transport, ca: &LocalCa) {
    let server = spawn_https_server(ca);
    let url = server.url_on("https", "localhost", "/plain");
    let err = wait_for(
        "untrusted handshake outcome",
        t.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap_err();
    assert!(matches!(err, Error::Transport { .. }), "got: {err:?}");
}

/// A pin on the loopback host with a hash that matches nothing in the
/// chain must fail closed, even though the CA itself is trusted.
pub fn https_pin_mismatch_rejects(t: &dyn Transport, ca: &LocalCa) {
    let server = spawn_https_server(ca);
    let url = server.url_on("https", "localhost", "/plain");
    let err = wait_for(
        "pin mismatch outcome",
        t.execute(Request::new(Method::Get, url), CancelToken::new()),
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Transport { message: ref m, .. } if m.contains("PINNED")),
        "got: {err:?}"
    );
}

/// The WSS counterpart of the echo round-trip, encrypted with `ca` and
/// trusted via the same custom roots.
pub fn wss_echo_with_trusted_ca(t: &dyn monet_core::transport::WebSocketTransport, ca: &LocalCa) {
    use monet_core::ws::WsMessage;
    use tokio_tungstenite::tungstenite::Message;

    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let acceptor = TlsAcceptor::from(Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("rustls protocol versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![private::leaf_der(&ca.cert)],
            ca.cert.leaf_key.clone_key(),
        )
        .expect("fresh leaf certificate is valid"),
    ));
    std::thread::Builder::new()
        .name("monet-conformance-wss".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                addr_tx.send(listener.local_addr().unwrap()).unwrap();
                let (stream, _) = listener.accept().await.unwrap();
                let tls = acceptor.accept(stream).await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(tls).await.unwrap();
                while let Some(Ok(message)) = futures_util::StreamExt::next(&mut ws).await {
                    if let Message::Text(text) = message {
                        futures_util::SinkExt::send(&mut ws, Message::Text(text))
                            .await
                            .unwrap();
                    }
                }
            });
        })
        .unwrap();
    let addr = addr_rx.recv().unwrap();

    let request = Request::new(
        Method::Get,
        Url::parse(&format!("wss://{addr}/socket")).unwrap(),
    );
    let mut stream = wait_for("wss open", t.connect(request, CancelToken::new())).unwrap();
    stream
        .send(WsMessage::Text("encrypted hello".into()))
        .unwrap();
    let echoed = wait_for("wss echo", stream.next_message()).unwrap();
    assert_eq!(echoed, Some(WsMessage::Text("encrypted hello".into())));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PEM fixture key must load exactly like a DER one: leaves minted
    /// from it chain to the CA certificate (regression: the Android
    /// platform battery fed openssl's PEM output to a DER-only parser
    /// and panicked on device).
    #[test]
    fn from_pkcs8_accepts_pem_and_der_keys() {
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = rcgen::CertificateParams::new(vec![])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let pem = pkcs8_pem_from_der(&key.serialize_der());
        let from_pem = LocalCa::from_pkcs8(ca.der().to_vec(), pem.as_bytes());
        let from_der = LocalCa::from_pkcs8(ca.der().to_vec(), &key.serialize_der());
        assert_eq!(from_pem.der(), from_der.der());
    }

    /// Wraps DER in a PEM envelope the way openssl does (64-column
    /// base64, BEGIN/END guards) — the fixture format.
    pub(crate) fn pkcs8_pem_from_der(der: &[u8]) -> String {
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(der);
        let mut pem = String::from("-----BEGIN PRIVATE KEY-----\n");
        for chunk in encoded.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(chunk).unwrap());
            pem.push('\n');
        }
        pem.push_str("-----END PRIVATE KEY-----\n");
        pem
    }
}

#[cfg(test)]
mod regenerate {
    use super::tests::pkcs8_pem_from_der;

    /// Regenerates the demo app's fixture CA with the same toolchain the
    /// runtime uses (openssl-generated PKCS#8 trips rcgen's ring parser
    /// with WrongAlgorithm). Run on demand:
    /// `cargo test -p monet-conformance --lib -- --ignored --nocapture`
    /// then copy the printed artifacts into crates/monet-demo/src/fixtures/
    /// and platform/android-demo/app/src/main/res/raw/.
    #[test]
    #[ignore]
    fn print_fixture_ca() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params =
            rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&key).unwrap();
        println!("--- ca.der (hex) ---");
        println!("{}", hex(ca.der()));
        println!("--- ca.pkcs8.key (PEM) ---");
        println!("{}", pkcs8_pem_from_der(&key.serialize_der()));
        println!("--- ca.pem ---");
        println!("{}", ca.pem());
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02X}")).collect()
    }
}

#[cfg(test)]
mod leaf_probe {
    use super::pkcs8_der;

    /// Prints a leaf (base64 DER) signed by the fixture CA for the
    /// on-device verification probe:
    /// `cargo test -p monet-conformance --lib leaf_probe -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn print_fixture_leaf() {
        let ca_der = std::fs::read("../../crates/monet-demo/src/fixtures/ca.der").unwrap();
        let ca_key = std::fs::read("../../crates/monet-demo/src/fixtures/ca.pkcs8.key").unwrap();
        let key_der = pkcs8_der(&ca_key);
        let ca_key = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(
            &rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
            &rcgen::PKCS_ECDSA_P256_SHA256,
        )
        .unwrap();
        let ca = rcgen::CertificateParams::from_ca_cert_der(
            &rustls::pki_types::CertificateDer::from(ca_der.clone()),
        )
        .unwrap()
        .self_signed(&ca_key)
        .unwrap();
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let mut leaf_params =
            rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        leaf_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "monet-test-leaf");
        let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
        let b64: String = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(leaf.der())
        };
        println!("--- leaf (base64 DER) ---");
        println!("{b64}");
    }
}
