// Pinning for server-trust challenges on the shared session.
//
// The host-matching semantics (walk-up, includeSubdomains, expiry) live
// in monet-core; Swift asks through the bridge and does only the
// mechanical parts: SecTrustEvaluate, SPKI SHA-256 extraction from the
// DER certificate, comparison. Modeled on nitro-fetch's
// CertificatePinning.swift.

import CryptoKit
import Foundation
import Security
import monet_urlsession_bridge

enum PinningShim {
  /// The outcome of evaluating a pinned host's trust.
  enum Outcome {
    /// The chain validated and carries a pinned key.
    case use
    /// Standard validation failed; let the system surface its own error.
    case defaultHandling
    /// The chain is valid but carries no pinned key: reject.
    case reject
  }

  /// Trust evaluation can block (e.g. fetching an intermediate), so it
  /// runs off any transport's serial queue.
  static let evaluationQueue = DispatchQueue(
    label: "monet.urlsession.pinning", qos: .userInitiated, attributes: .concurrent)

  /// Upper bound on hashes a challenge asks for (the bridge call fills
  /// a fixed buffer).
  static let maxHashes = 64

  /// The pinned SPKI hashes for `host`, asking monet-core for the
  /// matching entry. Empty means the host is not pinned.
  static func pinnedHashes(for host: String) -> [Data] {
    let hostBytes = Array(host.utf8)
    guard !hostBytes.isEmpty else { return [] }
    var out = [UInt8](repeating: 0, count: maxHashes * 32)
    let count = hostBytes.withUnsafeBufferPointer { hostBuf in
      out.withUnsafeMutableBufferPointer { outBuf in
        monet_pins_for_host(
          hostBuf.baseAddress, hostBuf.count, outBuf.baseAddress, outBuf.count)
      }
    }
    return (0..<count).map { i in Data(out[(i * 32)..<((i + 1) * 32)]) }
  }

  /// Evaluates `trust` and matches the chain against `hashes`,
  /// reporting on the evaluation queue. Empty `hashes` (an unpinned
  /// host, challenged only because custom roots are registered) skips
  /// the pin layer: the evaluation alone decides.
  static func evaluate(
    trust: SecTrust,
    hashes: [Data],
    completion: @escaping (Outcome) -> Void
  ) {
    evaluationQueue.async {
      SecTrustEvaluateAsyncWithError(trust, PinningShim.evaluationQueue) { _, trusted, _ in
        guard trusted else {
          completion(.defaultHandling)
          return
        }
        guard !hashes.isEmpty else {
          completion(.use)
          return
        }
        completion(chainMatches(trust, hashes) ? .use : .reject)
      }
    }
  }

  // MARK: - Custom roots (configureTrust)

  /// The registered custom trust roots as certificates, ready for
  /// `SecTrustSetAnchorCertificates`. Queried per challenge: the
  /// registration is process-global last-wins, same as pins.
  static func customRoots() -> [SecCertificate] {
    let count = roots_count()
    guard count > 0 else { return [] }
    var certificates: [SecCertificate] = []
    certificates.reserveCapacity(count)
    for index in 0..<count {
      // Two calls per root: size, then copy (the bridge writes nothing
      // into a too-small buffer and returns the needed length). A zero
      // from either call is a torn read — the registry was replaced
      // between the count and the copy — and the slot is skipped, which
      // fails the handshake closed rather than trusting less.
      let needed = monet_root_at(index, nil, 0)
      guard needed > 0 else { continue }
      var der = [UInt8](repeating: 0, count: needed)
      let written = der.withUnsafeMutableBufferPointer { buffer in
        monet_root_at(index, buffer.baseAddress, buffer.count)
      }
      guard written == needed,
        let certificate = SecCertificateCreateWithData(nil, Data(der) as CFData)
      else { continue }
      certificates.append(certificate)
    }
    return certificates
  }

  /// Adds `roots` to `trust` as additional anchors. The system's stay
  /// trusted — `SecTrustSetAnchorCertificatesOnly` is never called —
  /// the network_security_config semantics. No-op on an empty list.
  static func apply(_ roots: [SecCertificate], to trust: SecTrust) {
    guard !roots.isEmpty else { return }
    SecTrustSetAnchorCertificates(trust, roots as CFArray)
  }

  /// A chain passes when any certificate in it (leaf, intermediate, or
  /// trust-anchor root) has a pinned key — the same rule Cronet and the
  /// Rust transports apply.
  private static func chainMatches(_ trust: SecTrust, _ hashes: [Data]) -> Bool {
    let chain = (SecTrustCopyCertificateChain(trust) as? [SecCertificate]) ?? []
    return chain.contains { certificate in
      guard let spki = subjectPublicKeyInfo(of: certificate) else { return false }
      return hashes.contains(Data(SHA256.hash(data: spki)))
    }
  }

  // MARK: - SPKI extraction

  /// The DER SubjectPublicKeyInfo of a certificate: the bytes that
  /// `openssl x509 -pubkey -noout | openssl pkey -pubin -outform DER |
  /// openssl dgst -sha256` hashes. Read straight out of the certificate,
  /// so every key type (RSA, EC, Ed25519) hashes the same way.
  private static func subjectPublicKeyInfo(of certificate: SecCertificate) -> Data? {
    let der = [UInt8](SecCertificateCopyData(certificate) as Data)
    // Certificate ::= SEQUENCE { tbsCertificate SEQUENCE { [0] version
    // OPTIONAL, serialNumber, signature, issuer, validity, subject,
    // subjectPublicKeyInfo, ... }, ... }
    guard let cert = derElement(der, at: 0), cert.tag == 0x30,
      let tbs = derElement(der, at: cert.contentStart), tbs.tag == 0x30, tbs.end <= cert.end
    else { return nil }
    var cursor = tbs.contentStart
    if let version = derElement(der, at: cursor), version.tag == 0xA0 { cursor = version.end }
    for _ in 0..<5 {  // serialNumber, signature, issuer, validity, subject
      guard let field = derElement(der, at: cursor), field.end <= tbs.end else { return nil }
      cursor = field.end
    }
    guard let spki = derElement(der, at: cursor), spki.tag == 0x30, spki.end <= tbs.end
    else { return nil }
    return Data(der[cursor..<spki.end])
  }

  private static func derElement(
    _ der: [UInt8], at offset: Int
  ) -> (tag: UInt8, contentStart: Int, end: Int)? {
    guard offset >= 0, offset + 2 <= der.count else { return nil }
    var length = Int(der[offset + 1])
    var contentStart = offset + 2
    if length & 0x80 != 0 {
      // Long form. 0x80 (indefinite length) isn't valid DER; more than 4
      // length bytes never occurs in a certificate.
      let count = length & 0x7F
      guard (1...4).contains(count), contentStart + count <= der.count else { return nil }
      length = der[contentStart..<contentStart + count].reduce(0) { $0 << 8 | Int($1) }
      contentStart += count
    }
    let end = contentStart + length
    guard end <= der.count else { return nil }
    return (der[offset], contentStart, end)
  }
}
