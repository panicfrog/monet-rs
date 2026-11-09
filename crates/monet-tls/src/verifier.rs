//! TLS trust and pinning for every monet-rs rustls transport.
//!
//! Four trust sources: the platform verifier (Android system CAs +
//! network_security_config, feature-switched), the embedded Mozilla
//! bundle (the default), an explicit set of custom DER roots (private
//! CAs, tests — trusted *instead of* everything else), or the platform
//! verifier with custom DER roots alongside it (network_security_config
//! semantics — the app's anchors are additive). Whichever is active,
//! pinned hosts additionally require a matching SPKI hash in the
//! presented chain — the same semantics as monet-core's pin walk-up,
//! enforced in one place instead of nitro-fetch's three copies.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring as ring_crypto;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme,
};
use sha2::{Digest, Sha256};

use monet_core::error::{Error, Result};
use monet_core::pin::PinConfig;

/// Which roots to trust.
#[derive(Clone, Default)]
pub enum TlsTrust {
    /// Platform trust store via rustls-platform-verifier (feature
    /// `platform-verifier`). On Android, the embedder must have initialized
    /// it (see the crate docs) before connections start.
    PlatformVerifier,
    /// Exactly these DER-encoded root certificates — nothing else. For
    /// private/enterprise CAs and for tests that issue their own CA;
    /// callers who want the public web on top should stay on
    /// [`TlsTrust::WebpkiRoots`], which this replaces entirely.
    CustomRoots(Vec<Vec<u8>>),
    /// The platform trust store plus these DER-encoded roots alongside
    /// it — network_security_config semantics: system CAs stay trusted,
    /// the app's own anchors are additive. For apps whose traffic spans
    /// the public web and a private CA; a roots-only trust is
    /// [`TlsTrust::CustomRoots`]. Needs feature `platform-verifier`,
    /// like [`TlsTrust::PlatformVerifier`].
    PlatformAndCustomRoots(Vec<Vec<u8>>),
    /// Embedded Mozilla CA bundle (feature `webpki-roots`, the default).
    #[default]
    WebpkiRoots,
}

/// Wraps a standard verifier with monet-core's SPKI pinning.
struct PinningVerifier {
    inner: Arc<dyn ServerCertVerifier>,
    pins: PinConfig,
}

impl std::fmt::Debug for PinningVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinningVerifier")
            .field("pinned_hosts", &self.pins.entries().len())
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for PinningVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        // Standard validation first (chain, expiry, hostname)...
        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;
        // ...then the pin policy on top.
        let host = server_name.to_str();
        if let Some(entry) = self.pins.pins_for_host(host.as_ref()) {
            let mut matched = false;
            for cert in std::iter::once(end_entity).chain(intermediates) {
                if let Some(spki) = crate::spki::spki_der(cert.as_ref()) {
                    let digest: [u8; 32] = Sha256::digest(spki).into();
                    if entry.hashes.iter().any(|h| h == &digest) {
                        matched = true;
                        break;
                    }
                }
            }
            if !matched {
                return Err(RustlsError::General(
                    "ERR_SSL_PINNED_KEY_NOT_IN_CERT_CHAIN: no pinned key in the chain".into(),
                ));
            }
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// System trust with app-supplied roots on top. The platform verifier
/// runs first; a chain it rejects is retried against the custom roots
/// (Conscrypt's ordering for user-added anchors: the more specific store
/// answers last, so when both reject, the custom error surfaces — the
/// one the app can act on).
struct PlatformAndCustomRootsVerifier {
    platform: Arc<dyn ServerCertVerifier>,
    custom: Arc<dyn ServerCertVerifier>,
}

impl std::fmt::Debug for PlatformAndCustomRootsVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlatformAndCustomRootsVerifier")
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for PlatformAndCustomRootsVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        match self.platform.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(verified) => Ok(verified),
            Err(_) => self.custom.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                ocsp_response,
                now,
            ),
        }
    }

    // Both halves verify signatures through the same ring registry (the
    // platform verifier is built on ring_crypto, WebPkiServerVerifier on
    // the crate-features default, also ring), so the custom half's
    // answers stand for both.
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        self.custom.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        self.custom.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.custom.supported_verify_schemes()
    }
}

/// A WebPki verifier over exactly these DER roots — the custom half of
/// both `CustomRoots` and `PlatformAndCustomRoots`.
fn custom_roots_verifier(der_roots: &[Vec<u8>]) -> Result<Arc<dyn ServerCertVerifier>> {
    let mut roots = RootCertStore::empty();
    for der in der_roots {
        roots
            .add(rustls::pki_types::CertificateDer::from(der.clone()))
            .map_err(|e| Error::transport(format!("monet-tls: invalid custom root: {e}")))?;
    }
    Ok(
        rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| Error::transport(format!("monet-tls: custom-roots verifier: {e}")))?,
    )
}

#[cfg(feature = "platform-verifier")]
fn platform_verifier() -> Result<Arc<dyn ServerCertVerifier>> {
    Ok(Arc::new(
        rustls_platform_verifier::Verifier::new(ring_crypto::default_provider().into()).map_err(
            |e| Error::transport(format!("monet-tls: platform verifier unavailable: {e}")),
        )?,
    ))
}

/// The trust half of [`client_config`]: which roots a chain must climb,
/// as one verifier.
fn base_verifier(trust: &TlsTrust) -> Result<Arc<dyn ServerCertVerifier>> {
    match trust {
        TlsTrust::CustomRoots(der_roots) => custom_roots_verifier(der_roots),
        TlsTrust::PlatformVerifier | TlsTrust::PlatformAndCustomRoots(_) => {
            #[cfg(feature = "platform-verifier")]
            {
                match trust {
                    TlsTrust::PlatformVerifier => platform_verifier(),
                    // The roots validate at build even though the platform
                    // half is primary: garbage DER must fail here, not at
                    // the first handshake.
                    TlsTrust::PlatformAndCustomRoots(der_roots) => {
                        let custom = custom_roots_verifier(der_roots)?;
                        let platform = platform_verifier()?;
                        Ok(Arc::new(PlatformAndCustomRootsVerifier {
                            platform,
                            custom,
                        }))
                    }
                    _ => unreachable!("the outer match pinned the variant"),
                }
            }
            #[cfg(not(feature = "platform-verifier"))]
            {
                return Err(Error::transport(
                    "monet-tls: built without the `platform-verifier` feature",
                ));
            }
        }
        TlsTrust::WebpkiRoots => {
            #[cfg(feature = "webpki-roots")]
            {
                let mut roots = RootCertStore::empty();
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                let verifier: Arc<dyn ServerCertVerifier> =
                    rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
                        .build()
                        .map_err(|e| {
                            Error::transport(format!("monet-tls: webpki verifier: {e}"))
                        })?;
                Ok(verifier)
            }
            #[cfg(not(feature = "webpki-roots"))]
            {
                return Err(Error::transport(
                    "monet-tls: built without any trust source (enable `webpki-roots` or `platform-verifier`)",
                ));
            }
        }
    }
}

/// Builds a rustls client config: chosen trust source, optionally wrapped
/// with the pin policy.
pub fn client_config(trust: &TlsTrust, pins: &PinConfig) -> Result<Arc<ClientConfig>> {
    let base = base_verifier(trust)?;
    let verifier: Arc<dyn ServerCertVerifier> = if pins.is_empty() {
        base
    } else {
        Arc::new(PinningVerifier {
            inner: base,
            pins: pins.clone(),
        })
    };

    Ok(Arc::new(
        ClientConfig::builder_with_provider(ring_crypto::default_provider().into())
            .with_safe_default_protocol_versions()
            .map_err(|e| Error::transport(format!("monet-tls: rustls protocol versions: {e}")))?
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_roots_build_a_verifier() {
        let key = rcgen::KeyPair::generate().unwrap();
        let ca = rcgen::CertificateParams::new(vec![])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let config = client_config(
            &TlsTrust::CustomRoots(vec![ca.der().to_vec()]),
            &PinConfig::default(),
        );
        assert!(config.is_ok());
    }

    #[test]
    fn custom_roots_reject_garbage_der() {
        let err = client_config(
            &TlsTrust::CustomRoots(vec![vec![0xDE, 0xAD]]),
            &PinConfig::default(),
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Transport { message: ref m, .. } if m.contains("custom root")),
            "got: {err:?}"
        );
    }

    /// The additive variant needs a real platform verifier; run with
    /// `cargo test -p monet-tls --features platform-verifier`.
    #[cfg(feature = "platform-verifier")]
    mod platform_and_custom_roots {
        use super::*;

        /// A CA + leaf chain, minted the way the conformance crate mints
        /// its fixtures: CA:TRUE on the anchor and a leaf subject distinct
        /// from the issuer — the platform verifier checks both where
        /// rustls's webpki path does not, so the chain must be built to
        /// pass a real verifier, not just a lenient one.
        fn minted_chain() -> (Vec<u8>, CertificateDer<'static>) {
            let ca_key = rcgen::KeyPair::generate().unwrap();
            let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = ca_params.self_signed(&ca_key).unwrap();

            let leaf_key = rcgen::KeyPair::generate().unwrap();
            let mut leaf_params =
                rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
            leaf_params
                .distinguished_name
                .push(rcgen::DnType::CommonName, "monet-test-leaf");
            let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
            (ca.der().to_vec(), leaf.der().clone())
        }

        #[test]
        fn garbage_der_fails_at_build() {
            let err = base_verifier(&TlsTrust::PlatformAndCustomRoots(vec![vec![0xDE, 0xAD]]))
                .unwrap_err();
            assert!(
                matches!(err, Error::Transport { message: ref m, .. } if m.contains("custom root")),
                "got: {err:?}"
            );
        }

        /// The additive semantics that justify the variant: a locally
        /// minted CA is exactly what the platform store rejects
        /// (UnknownIssuer) and the configured roots accept — the
        /// network_security_config shape (battery fixture, enterprise CA).
        #[test]
        fn trusts_what_platform_alone_rejects() {
            let (ca_der, leaf) = minted_chain();
            let name = ServerName::try_from("localhost").unwrap();

            let platform = base_verifier(&TlsTrust::PlatformVerifier).unwrap();
            assert!(
                platform
                    .verify_server_cert(&leaf, &[], &name, &[], UnixTime::now())
                    .is_err(),
                "a locally minted CA is not in the platform store"
            );

            let combined = base_verifier(&TlsTrust::PlatformAndCustomRoots(vec![ca_der])).unwrap();
            combined
                .verify_server_cert(&leaf, &[], &name, &[], UnixTime::now())
                .expect("the configured root anchors the chain the platform rejected");
        }

        /// Additive is not permissive: a chain from a CA that is neither
        /// in the platform store nor in the configured roots fails closed.
        #[test]
        fn still_rejects_unknown_cas() {
            let (_unconfigured_ca, leaf) = minted_chain();
            let other_key = rcgen::KeyPair::generate().unwrap();
            let other_ca = rcgen::CertificateParams::new(vec![])
                .unwrap()
                .self_signed(&other_key)
                .unwrap();

            let combined = base_verifier(&TlsTrust::PlatformAndCustomRoots(vec![
                other_ca.der().to_vec(),
            ]))
            .unwrap();
            let name = ServerName::try_from("localhost").unwrap();
            assert!(
                combined
                    .verify_server_cert(&leaf, &[], &name, &[], UnixTime::now())
                    .is_err(),
                "extend is not accept-everything"
            );
        }
    }
}
