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
            let ca = rcgen::CertificateParams::new(vec![])
                .expect("CA params")
                .self_signed(&ca_key)
                .expect("self-signed CA");
            let ca_der = ca.der().to_vec();
            Self::mint(ca, ca_key, ca_der)
        }

        pub fn from_pkcs8(ca_der: Vec<u8>, ca_key_pkcs8: &[u8]) -> ServerCert {
            let ca_key = rcgen::KeyPair::from_pkcs8_der_and_sign_algo(
                &rustls::pki_types::PrivatePkcs8KeyDer::from(ca_key_pkcs8),
                &rcgen::PKCS_ECDSA_P256_SHA256,
            )
            .expect("fixture CA key parses as PKCS#8 P-256");
            // Reconstruct an issuer from the certificate (same subject,
            // same key): leaves minted below chain to the original DER.
            let ca = rcgen::CertificateParams::from_ca_cert_der(&CertificateDer::from(
                ca_der.clone(),
            ))
            .expect("fixture CA certificate parses")
            .self_signed(&ca_key)
            .expect("issuer reconstructs");
            Self::mint(ca, ca_key, ca_der)
        }

        fn mint(
            ca: rcgen::Certificate,
            ca_key: rcgen::KeyPair,
            ca_der: Vec<u8>,
        ) -> ServerCert {
            let leaf_key = rcgen::KeyPair::generate().expect("generate leaf key");
            let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".into(), "localhost".into()])
                .expect("leaf params")
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
