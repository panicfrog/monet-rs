//! TLS trust and pinning for every monet-rs rustls transport.
//!
//! Three trust sources: the platform verifier (Android system CAs +
//! network_security_config, feature-switched), the embedded Mozilla
//! bundle (the default), or an explicit set of custom DER roots (private
//! CAs, tests). Whichever is active, pinned hosts additionally require a
//! matching SPKI hash in the presented chain — the same semantics as
//! monet-core's pin walk-up, enforced in one place instead of
//! nitro-fetch's three copies.

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

/// Builds a rustls client config: chosen trust source, optionally wrapped
/// with the pin policy.
pub fn client_config(trust: &TlsTrust, pins: &PinConfig) -> Result<Arc<ClientConfig>> {
    let base: Arc<dyn ServerCertVerifier> = match trust {
        TlsTrust::CustomRoots(der_roots) => {
            let mut roots = RootCertStore::empty();
            for der in der_roots {
                roots
                    .add(rustls::pki_types::CertificateDer::from(der.clone()))
                    .map_err(|e| {
                        Error::transport(format!("monet-tls: invalid custom root: {e}"))
                    })?;
            }
            rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| Error::transport(format!("monet-tls: custom-roots verifier: {e}")))?
        }
        TlsTrust::PlatformVerifier => {
            #[cfg(feature = "platform-verifier")]
            {
                Arc::new(
                    rustls_platform_verifier::Verifier::new(ring_crypto::default_provider().into())
                        .map_err(|e| {
                            Error::transport(format!(
                                "monet-tls: platform verifier unavailable: {e}"
                            ))
                        })?,
                )
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
                rustls::client::WebPkiServerVerifier::builder(Arc::new(roots))
                    .build()
                    .map_err(|e| Error::transport(format!("monet-tls: webpki verifier: {e}")))?
            }
            #[cfg(not(feature = "webpki-roots"))]
            {
                return Err(Error::transport(
                    "monet-tls: built without any trust source (enable `webpki-roots` or `platform-verifier`)",
                ));
            }
        }
    };

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
}
