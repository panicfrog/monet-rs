//! Extracting the DER-encoded SubjectPublicKeyInfo from a certificate.
//!
//! Pin hashes are SHA-256 over the SPKI in DER form (RFC 7469). Rather than
//! pulling in a full ASN.1 parser, this walks the handful of TLVs in front
//! of the SPKI inside a well-formed X.509 certificate.

/// Returns the DER-encoded SPKI (tag + length + value) of `cert`, or `None`
/// when the certificate is not shaped as expected.
pub(crate) fn spki_der(cert: &[u8]) -> Option<&[u8]> {
    // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signature }
    let (tag, tbs, rest) = tlv(cert)?;
    if tag != 0x30 || !rest.is_empty() {
        return None;
    }
    let mut cursor = tbs;
    // TBSCertificate ::= SEQUENCE {
    //   [0] version (optional), serialNumber, signature, issuer, validity,
    //   subject, subjectPublicKeyInfo, ... }
    let (tag, _, _) = tlv(cursor)?; // version (context tag) or serialNumber
    if tag == 0xA0 {
        cursor = skip(cursor)?;
    }
    cursor = skip(cursor)?; // serialNumber
    cursor = skip(cursor)?; // signature
    cursor = skip(cursor)?; // issuer
    cursor = skip(cursor)?; // validity
    cursor = skip(cursor)?; // subject
    // subjectPublicKeyInfo is next; return its full DER encoding
    // (header + value), i.e. everything up to what follows it.
    let (tag, _, after) = tlv(cursor)?;
    if tag != 0x30 {
        return None;
    }
    let spki_len = cursor.len() - after.len();
    Some(&cursor[..spki_len])
}

/// Reads one TLV header; returns (tag, content, everything after the TLV).
fn tlv(bytes: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = bytes.split_first()?;
    let (&len_byte, rest) = rest.split_first()?;
    let (len, body) = if len_byte < 0x80 {
        (usize::from(len_byte), rest)
    } else {
        let n = usize::from(len_byte & 0x7F);
        if n == 0 || n > rest.len() || n > 4 {
            return None;
        }
        let len = rest[..n]
            .iter()
            .fold(0usize, |acc, b| (acc << 8) | usize::from(*b));
        (len, &rest[n..])
    };
    let (content, after) = body.split_at_checked(len)?;
    Some((tag, content, after))
}

fn skip(bytes: &[u8]) -> Option<&[u8]> {
    tlv(bytes).map(|(_, _, after)| after)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal DER: Certificate SEQUENCE { TBS SEQUENCE { [0]{}, serial INT,
    /// sig SEQ, issuer SEQ, validity SEQ, subject SEQ, SPKI SEQ{...},
    /// sigalg SEQ, sig BITSTRING }, rest }
    fn fixture() -> (Vec<u8>, Vec<u8>) {
        fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
            let mut out = vec![tag];
            if content.len() < 0x80 {
                out.push(content.len() as u8);
            } else {
                out.push(0x81);
                out.push(content.len() as u8);
            }
            out.extend_from_slice(content);
            out
        }
        let spki = tlv(
            0x30,
            &[0x06, 0x07, 0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x02, 0x01],
        );
        let tbs = [
            tlv(0xA0, &[]),
            tlv(0x02, &[0x01]),
            tlv(0x30, &[]),
            tlv(0x30, &[]),
            tlv(0x30, &[]),
            tlv(0x30, &[]),
            spki.clone(),
        ]
        .concat();
        // Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signature }
        let content = [
            tbs,
            tlv(0x30, &[]),     // signatureAlgorithm
            tlv(0x03, &[0x00]), // signature (BIT STRING)
        ]
        .concat();
        let cert = tlv(0x30, &content);
        (cert, spki)
    }

    #[test]
    fn extracts_spki() {
        let (cert, spki) = fixture();
        assert_eq!(spki_der(&cert), Some(spki.as_slice()));
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(spki_der(&[]), None);
        assert_eq!(spki_der(&[0x30, 0x00]), None); // empty certificate
        assert_eq!(spki_der(&[0x02, 0x01, 0x00]), None); // not a SEQUENCE
    }
}
