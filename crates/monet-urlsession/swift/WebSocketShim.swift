// WebSocket over Network.framework: NWConnection as the transport, the
// RFC 6455 protocol implemented here.
//
// Why not URLSessionWebSocketTask: it maps close codes 1012-1014 to
// 1005 and drops the reason — the raw close frame is unreachable (the
// documented reason nitro-fetch hand-rolls frames too, see its
// NWWebSocketConnection.hpp). monet-core's CloseFrame contract needs
// exact code/reason round-trips, so the protocol lives on this side of
// the bridge. Ported from nitro's NWStreamWebSocketConnection.mm, v1
// without permessage-deflate, redirect following, or cookie injection
// (see docs/plans/2026-11-monet-urlsession.md).

import CryptoKit
import Foundation
import Network
import Security
import monet_urlsession_bridge

/// One WebSocket connection. All mutable state is confined to `queue`
/// (nitro's model); control-plane entry points hop onto it.
final class WebSocketConnection {
  private enum Const {
    static let opContinuation: UInt8 = 0x0
    static let opText: UInt8 = 0x1
    static let opBinary: UInt8 = 0x2
    static let opClose: UInt8 = 0x8
    static let opPing: UInt8 = 0x9
    static let opPong: UInt8 = 0xA
    static let fin: UInt8 = 0x80
    static let rsv1: UInt8 = 0x40
    static let maxMessageSize = 16 * 1024 * 1024
    static let maxHandshakeSize = 256 * 1024
    static let readSize = 4 * 1024 * 1024
    static let handshakeTimeoutNs: UInt64 = 60 * 1_000_000_000
    static let closeTimeoutNs: UInt64 = 5 * 1_000_000_000
    static let wsGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  }

  private let id: UInt64
  private let queue: DispatchQueue

  // Connection state (queue-confined).
  private var connection: NWConnection?
  private var url: URL?
  private var extraHeaders: [(String, String)] = []
  private var handshakeKey = ""
  private var offeredProtocols = ""
  private var handshake = [UInt8]()
  private var tlsFailure: String?

  // Frame parsing state.
  private var buffer = [UInt8]()
  private var message = [UInt8]()
  private var inMessage = false
  private var messageBinary = false

  // Lifecycle.
  private var open = false
  private var closeSent = false
  private var closeWritten = false
  private var stopped = false
  private var finished = false
  private var closeCode: UInt16 = 0
  private var closeReason = ""
  private var failure: String?
  private var timerGeneration: UInt64 = 0
  /// Keeps the connection alive across the close handshake.
  private var retainSelf: WebSocketConnection?

  init(id: UInt64, headers: [(String, String)]) {
    self.id = id
    self.extraHeaders = headers
    self.queue = DispatchQueue(
      label: "monet.urlsession.ws.connection", qos: .userInitiated)
  }

  // MARK: - Control plane entry points

  func openConnection(urlString: String) {
    queue.async { [self] in
      guard !finished, let target = URL(string: urlString), WebSocketConnection.isValid(target)
      else {
        finish(code: 0, reason: "", wasClean: false,
               error: "invalid websocket URL: \(urlString)")
        return
      }
      start(target: target)
    }
  }

  func send(data: [UInt8], isBinary: Bool) {
    queue.async { [self] in
      guard open, !closeSent, !finished else { return }
      sendFrame(
        opcode: isBinary ? Const.opBinary : Const.opText,
        payload: data)
    }
  }

  func sendPing(_ data: [UInt8]) {
    queue.async { [self] in
      guard open, !closeSent, !finished else { return }
      sendFrame(opcode: Const.opPing, payload: data)
    }
  }

  func sendPong(_ data: [UInt8]) {
    queue.async { [self] in
      guard open, !closeSent, !finished else { return }
      sendFrame(opcode: Const.opPong, payload: data)
    }
  }

  func closeConnection(code: UInt16, reason: String) {
    queue.async { [self] in
      guard !finished, open, !closeSent else { return }
      closeSent = true
      closeCode = code
      closeReason = reason
      retainSelf = self
      let payload = WebSocketConnection.isValidCloseCode(code)
        ? WebSocketConnection.closePayload(code: code, reason: reason) : []
      sendFrame(
        opcode: Const.opClose, payload: payload,
        completion: { [weak self] error in
          guard let self, error == nil else { return }
          self.queue.async { self.closeWritten = true }
        })
      armTimer(nanos: Const.closeTimeoutNs)
    }
  }

  /// Force cancel (the Rust cancel path): no close handshake.
  func cancelConnection() {
    queue.async { [self] in
      guard !finished else { return }
      finish(code: 0, reason: "", wasClean: false, error: "cancelled")
    }
  }

  // MARK: - Connection

  private func start(target: URL) {
    url = target
    handshake.removeAll()
    tlsFailure = nil
    let secure = WebSocketConnection.isSecure(target)
    let port = target.port ?? (secure ? 443 : 80)
    let endpoint = NWEndpoint.hostPort(
      host: NWEndpoint.Host(target.host ?? ""),
      port: NWEndpoint.Port(rawValue: UInt16(clamping: port)) ?? 80)
    let tcpOptions = NWProtocolTCP.Options()
    tcpOptions.noDelay = true
    let tlsOptions = secure ? NWProtocolTLS.Options() : nil
    if let tlsOptions, let host = target.host {
      configureTlsVerification(tlsOptions: tlsOptions, host: host)
    }
    let parameters = NWParameters(tls: tlsOptions, tcp: tcpOptions)

    let created = NWConnection(to: endpoint, using: parameters)
    connection = created
    created.stateUpdateHandler = { [weak self] state in
      guard let self else { return }
      self.queue.async {
        guard self.connection === created, !self.finished else { return }
        switch state {
        case .ready:
          self.sendHandshake()
          self.receiveHandshake()
        case .waiting(let error), .failed(let error):
          self.transportFailed(error)
        default:
          break
        }
      }
    }
    armTimer(nanos: Const.handshakeTimeoutNs, error: "the connection timed out")
    created.start(queue: queue)
  }

  /// Pinned hosts and roots-configured apps get a verify block that
  /// replaces Network.framework's trust evaluation (nitro's
  /// NitroWSPinningBridge pattern); the pins and roots resolve once so
  /// the block checks the configuration this decision made with. The
  /// block evaluates with the custom roots anchored (configureTrust
  /// applies to every wss host, not just pinned ones), then the pin
  /// layer on top.
  private func configureTlsVerification(tlsOptions: NWProtocolTLS.Options, host: String) {
    let hashes = PinningShim.pinnedHashes(for: host)
    let roots = PinningShim.customRoots()
    guard !hashes.isEmpty || !roots.isEmpty else { return }
    let failureSlot = PinningFailureSlot()
    tlsFailureSlot = failureSlot
    let securityOptions = tlsOptions.securityProtocolOptions
    sec_protocol_options_set_verify_block(
      securityOptions,
      { _, secTrust, complete in
        let trust = sec_trust_copy_ref(secTrust).takeRetainedValue()
        PinningShim.apply(roots, to: trust)
        PinningShim.evaluate(trust: trust, hashes: hashes) { outcome in
          switch outcome {
          case .use:
            complete(true)
          case .reject:
            failureSlot.message =
              "certificate pinning failed for \(host): no pinned key in the chain"
            complete(false)
          case .defaultHandling:
            // Standard evaluation already rejected it; keep Network's own
            // failure rather than a pinning one.
            complete(false)
          }
        }
      },
      PinningShim.evaluationQueue)
  }

  private var tlsFailureSlot: PinningFailureSlot?

  // MARK: - Handshake

  private func sendHandshake() {
    var nonce = [UInt8](repeating: 0, count: 16)
    let nonceCount = nonce.count
    _ = nonce.withUnsafeMutableBytes {
      SecRandomCopyBytes(kSecRandomDefault, nonceCount, $0.baseAddress!)
    }
    guard let url else { return }
    let secure = WebSocketConnection.isSecure(url)
    var host = url.host ?? ""
    if host.contains(":") { host = "[\(host)]" }
    if let port = url.port, port != (secure ? 443 : 80) { host += ":\(port)" }

    var fields: [(String, String)] = [
      ("Host", host),
      ("Upgrade", "websocket"),
      ("Connection", "Upgrade"),
      ("Sec-WebSocket-Key", Data(nonce).base64EncodedString()),
      ("Sec-WebSocket-Version", "13"),
    ]
    for (name, value) in extraHeaders {
      // Reject header injection and reserved-field shadowing.
      guard !name.isEmpty, !name.contains(where: { ":\r\n".contains($0) }),
        !value.contains(where: { "\r\n".contains($0) }),
        WebSocketConnection.findField(fields, name) == nil
      else { continue }
      fields.append((name, value))
    }
    // Whatever actually goes out is what the response is validated
    // against.
    handshakeKey = WebSocketConnection.findField(fields, "Sec-WebSocket-Key")?.1 ?? ""
    offeredProtocols = WebSocketConnection.findField(fields, "Sec-WebSocket-Protocol")?.1 ?? ""

    // The request target keeps the URL's percent encoding (nitro uses
    // percentEncodedPath/percentEncodedQuery for the same reason).
    let components = URLComponents(url: url, resolvingAgainstBaseURL: true)
    var target = components?.percentEncodedPath ?? ""
    if target.isEmpty { target = "/" }
    if let query = components?.percentEncodedQuery { target += "?\(query)" }
    var request = "GET \(target) HTTP/1.1\r\n"
    for (name, value) in fields { request += "\(name): \(value)\r\n" }
    request += "\r\n"
    connection?.send(
      content: Data(request.utf8), contentContext: .defaultMessage, isComplete: true,
      completion: .contentProcessed { _ in })
  }

  private func receiveHandshake() {
    connection?.receive(minimumIncompleteLength: 1, maximumLength: Const.readSize) {
      [weak self] content, context, isComplete, error in
      guard let self else { return }
      self.queue.async {
        guard !self.finished else { return }
        if let content {
          self.handshake.append(contentsOf: content)
          if self.handshake.count > Const.maxHandshakeSize {
            self.finish(
              code: 0, reason: "", wasClean: false,
              error: "there was a bad response from the server")
            return
          }
          if let end = WebSocketConnection.firstRange(
            of: [0x0D, 0x0A, 0x0D, 0x0A], in: self.handshake)
          {
            let headerBytes = Array(self.handshake[..<end.upperBound])
            let leftover = Array(self.handshake[end.upperBound...])
            self.handshakeReceived(header: headerBytes, leftover: leftover)
            return
          }
        }
        if let error {
          self.transportFailed(error)
        } else if isComplete && (context == nil || context!.isFinal) {
          self.transportFailed(nil)
        } else {
          self.receiveHandshake()
        }
      }
    }
  }

  private func handshakeReceived(header: [UInt8], leftover: [UInt8]) {
    guard let parsed = WebSocketConnection.parseHandshakeResponse(header) else {
      finish(
        code: 0, reason: "", wasClean: false,
        error: "there was a bad response from the server")
      return
    }
    if parsed.status != 101
      || parsed.upgrade.lowercased() != "websocket"
      || !parsed.connectionTokens.contains(where: { $0.lowercased() == "upgrade" })
      || parsed.accept != WebSocketConnection.acceptKey(for: handshakeKey)
    {
      finish(
        code: 0, reason: "", wasClean: false,
        error: "there was a bad response from the server")
      return
    }
    // A protocol the server picked must be one we offered; extensions
    // were not offered (no deflate in v1), so any negotiation is bad.
    if !parsed.negotiatedProtocol.isEmpty {
      let offered = offeredProtocols.split(separator: ",").map {
        $0.trimmingCharacters(in: .whitespaces)
      }
      if !offered.contains(parsed.negotiatedProtocol) {
        finish(
          code: 0, reason: "", wasClean: false,
          error: "there was a bad response from the server")
        return
      }
    }
    // Sec-WebSocket-Extensions in the response is deliberately NOT
    // rejected here even though we never offer any (RFC 6455 §4.1 step
    // 5 says fail): tungstenite — the other WS implementation this
    // transport mirrors — implements that step as an upstream TODO and
    // accepts the handshake. Consistency wins (the conformance check
    // `unrequested_deflate_offer_round_trips` pins this on both
    // engines); the framing stays plain either way, and the day
    // tungstenite lands step 5, that check trips and both sides
    // tighten together.

    open = true
    cancelTimer()
    on_ws_open(id)
    if !leftover.isEmpty { consume(leftover) }
    if !stopped { receiveFrames() }
  }

  // MARK: - Frame reader

  private func receiveFrames() {
    connection?.receive(minimumIncompleteLength: 1, maximumLength: Const.readSize) {
      [weak self] content, context, isComplete, error in
      guard let self else { return }
      self.queue.async {
        guard !self.stopped else { return }
        if let content { self.consume(Array(content)) }
        if self.stopped { return }
        if let error {
          self.transportFailed(error)
        } else if isComplete && (context == nil || context!.isFinal) {
          self.transportClosed()
        } else {
          self.receiveFrames()
        }
      }
    }
  }

  /// Feeds bytes into the frame parser; ports nitro's consume/parseFrames
  /// state machine (single accumulating buffer, fragmentation state kept
  /// in `message`/`inMessage`).
  private func consume(_ data: [UInt8]) {
    buffer.append(contentsOf: data)
    parseFrames()
  }

  private func parseFrames() {
    while !stopped {
      guard buffer.count >= 2 else { return }
      let first = buffer[0]
      let second = buffer[1]
      let opcode = first & 0x0F
      let isFin = first & Const.fin != 0
      let isCompressed = first & Const.rsv1 != 0
      var headerLength = 2
      var length = UInt64(second & 0x7F)
      if length == 126 {
        guard buffer.count >= 4 else { return }
        length = (UInt64(buffer[2]) << 8) | UInt64(buffer[3])
        headerLength = 4
      } else if length == 127 {
        guard buffer.count >= 10 else { return }
        length = 0
        for i in 0..<8 { length = (length << 8) | UInt64(buffer[2 + i]) }
        headerLength = 10
      }
      // Server frames must be unmasked; RSV2/3 are always bad here.
      if second & 0x80 != 0 || first & 0x30 != 0 {
        fail(code: 1002, posixError: "protocol error")
        return
      }
      if opcode & 0x08 != 0 {
        // Control frames: final, uncompressed, <= 125 bytes.
        guard isFin, !isCompressed, length <= 125, opcode >= Const.opClose, opcode <= Const.opPong
        else {
          fail(code: 1002, posixError: "protocol error")
          return
        }
        guard buffer.count >= headerLength + Int(length) else { return }
        let payload = Array(buffer[headerLength..<(headerLength + Int(length))])
        buffer.removeFirst(headerLength + Int(length))
        controlFrame(opcode: opcode, payload: payload)
        continue
      }
      // Data frames: text/binary/continuation, fragmentation rules,
      // RSV1 impossible (deflate not negotiated).
      guard opcode <= Const.opBinary,
        (opcode == Const.opContinuation ? (inMessage && !isCompressed) : !inMessage),
        !isCompressed
      else {
        fail(code: 1002, posixError: "protocol error")
        return
      }
      let base = opcode == Const.opContinuation ? message.count : 0
      if length > Const.maxMessageSize - base {
        fail(code: 1009, posixError: "message too big")
        return
      }
      guard buffer.count >= headerLength + Int(length) else { return }
      let payload = Array(buffer[headerLength..<(headerLength + Int(length))])
      buffer.removeFirst(headerLength + Int(length))
      dataFrame(opcode: opcode, isFin: isFin, payload: payload)
    }
  }

  private func dataFrame(opcode: UInt8, isFin: Bool, payload: [UInt8]) {
    if opcode != Const.opContinuation && isFin {
      deliverMessage(payload: payload, isBinary: opcode == Const.opBinary)
      return
    }
    if opcode != Const.opContinuation {
      message = payload
      inMessage = true
      messageBinary = opcode == Const.opBinary
    } else {
      message.append(contentsOf: payload)
    }
    if isFin {
      inMessage = false
      let complete = message
      message = []
      deliverMessage(payload: complete, isBinary: messageBinary)
    }
  }

  private func deliverMessage(payload: [UInt8], isBinary: Bool) {
    if !isBinary, String(bytes: payload, encoding: .utf8) == nil {
      fail(code: 1007, posixError: "invalid UTF-8 in text message")
      return
    }
    if closeSent { return }
    notifyWsMessage(id, isBinary: isBinary, data: payload)
  }

  private func controlFrame(opcode: UInt8, payload: [UInt8]) {
    if opcode == Const.opPing {
      notifyWsPing(id, data: payload)
      if !closeSent {
        sendFrame(opcode: Const.opPong, payload: payload)
      }
      return
    }
    if opcode == Const.opPong {
      notifyWsPong(id, data: payload)
      return
    }
    // Close.
    var code: UInt16 = 1005
    var reason = ""
    if payload.count == 1 {
      fail(code: 1002, posixError: "protocol error")
      return
    }
    if payload.count >= 2 {
      code = (UInt16(payload[0]) << 8) | UInt16(payload[1])
      guard WebSocketConnection.isValidCloseCode(code),
        String(bytes: payload[2...], encoding: .utf8) != nil
      else {
        fail(code: 1002, posixError: "protocol error")
        return
      }
      reason = String(bytes: payload[2...], encoding: .utf8) ?? ""
    }
    stopped = true
    if closeSent {
      finish(code: closeCode, reason: closeReason, wasClean: true, error: nil)
      return
    }
    closeSent = true
    closeCode = code
    closeReason = reason
    retainSelf = self
    // Echo the close (code only, no reason), then finish with the peer's
    // code and reason — this is the fidelity URLSession can't give us.
    let echo = payload.count >= 2
      ? WebSocketConnection.closePayload(code: code, reason: "") : []
    sendFrame(opcode: Const.opClose, payload: echo, completion: { [weak self] _ in
      guard let self else { return }
      self.queue.async {
        self.finish(
          code: self.closeCode, reason: self.closeReason, wasClean: true, error: nil)
      }
    })
    armTimer(nanos: Const.closeTimeoutNs)
  }

  // MARK: - Failure paths

  /// A protocol violation: send a close frame carrying `code`, then
  /// finish uncleanly (1006 semantics), like nitro's fail().
  private func fail(code: UInt16, posixError: String) {
    guard !stopped else { return }
    stopped = true
    failure = posixError
    if closeSent || !open {
      finish(code: 0, reason: "", wasClean: false, error: failure)
      return
    }
    closeSent = true
    retainSelf = self
    let payload = WebSocketConnection.isValidCloseCode(code)
      ? WebSocketConnection.closePayload(code: code, reason: "") : []
    sendFrame(opcode: Const.opClose, payload: payload, completion: { [weak self] _ in
      guard let self else { return }
      self.queue.async {
        self.finish(code: 0, reason: "", wasClean: false, error: self.failure)
      }
    })
    armTimer(nanos: Const.closeTimeoutNs)
  }

  private func transportFailed(_ error: NWError?) {
    if let tlsFailureSlot, let message = tlsFailureSlot.message {
      finish(code: 0, reason: "", wasClean: false, error: message)
      return
    }
    if closeSent {
      closedAfterCloseSent()
      return
    }
    let description = error.map { $0.localizedDescription }
      ?? "the network connection was lost"
    finish(code: 0, reason: "", wasClean: false, error: description)
  }

  private func transportClosed() {
    if closeSent {
      closedAfterCloseSent()
      return
    }
    finish(code: 0, reason: "", wasClean: false, error: "connection closed")
  }

  private func closedAfterCloseSent() {
    if closeWritten {
      finish(code: closeCode, reason: closeReason, wasClean: true, error: nil)
    } else {
      finish(code: 0, reason: "", wasClean: false, error: nil)
    }
  }

  /// The one terminal exit: reports through the data plane exactly once.
  private func finish(code: UInt16, reason: String, wasClean: Bool, error: String?) {
    guard !finished else { return }
    finished = true
    stopped = true
    cancelTimer()
    connection?.cancel()
    connection = nil
    buffer = []
    message = []
    WebSocketShim.shared.completed(id: id)
    notifyWsClose(
      id, code: wasClean ? code : 0, reason: reason, wasClean: wasClean, error: error ?? "")
    retainSelf = nil
  }

  // MARK: - Frame writer

  /// A masked client frame (RFC 6455 5.3): clients must mask.
  private func sendFrame(opcode: UInt8, payload: [UInt8], completion: ((NWError?) -> Void)? = nil) {
    var frame = [UInt8]()
    frame.reserveCapacity(payload.count + 14)
    frame.append(Const.fin | opcode)
    let len = payload.count
    var key = [UInt8](repeating: 0, count: 4)
    let keyCount = key.count
    _ = key.withUnsafeMutableBytes {
      SecRandomCopyBytes(kSecRandomDefault, keyCount, $0.baseAddress!)
    }
    if len < 126 {
      frame.append(0x80 | UInt8(len))
    } else if len <= 0xFFFF {
      frame.append(0x80 | 126)
      frame.append(UInt8((len >> 8) & 0xFF))
      frame.append(UInt8(len & 0xFF))
    } else {
      frame.append(0x80 | 127)
      for shift in stride(from: 56, through: 0, by: -8) {
        frame.append(UInt8((UInt64(len) >> UInt64(shift)) & 0xFF))
      }
    }
    frame.append(contentsOf: key)
    let masked = (0..<len).map { payload[$0] ^ key[$0 & 3] }
    frame.append(contentsOf: masked)
    connection?.send(
      content: Data(frame), contentContext: .defaultMessage, isComplete: true,
      completion: .contentProcessed { error in completion?(error) })
  }

  // MARK: - Timers

  private func armTimer(nanos: UInt64, error: String? = nil) {
    timerGeneration += 1
    let generation = timerGeneration
    queue.asyncAfter(deadline: .now() + Double(nanos) / 1_000_000_000) { [weak self] in
      guard let self, self.timerGeneration == generation, !self.finished else { return }
      self.finish(code: 0, reason: "", wasClean: false, error: error)
    }
  }

  private func cancelTimer() {
    timerGeneration += 1
  }

  // MARK: - Handshake response parsing

  private struct HandshakeResponse {
    var status: Int
    var upgrade: String
    var connectionTokens: [String]
    var accept: String
    var negotiatedProtocol: String
    var extensions: String
  }

  private static func parseHandshakeResponse(_ bytes: [UInt8]) -> HandshakeResponse? {
    guard let text = String(bytes: bytes, encoding: .utf8) else { return nil }
    var lines = text.components(separatedBy: "\r\n")
    guard !lines.isEmpty else { return nil }
    let statusLine = lines.removeFirst()
    let parts = statusLine.split(separator: " ", maxSplits: 2)
    guard parts.count >= 2, parts[0].hasPrefix("HTTP/1."), let status = Int(parts[1]) else {
      return nil
    }
    var headers: [String: String] = [:]
    for line in lines {
      guard let colon = line.firstIndex(of: ":") else { continue }
      let name = String(line[..<colon]).trimmingCharacters(in: .whitespaces).lowercased()
      let value = String(line[line.index(after: colon)...]).trimmingCharacters(in: .whitespaces)
      headers[name] = value
    }
    return HandshakeResponse(
      status: status,
      upgrade: headers["upgrade"] ?? "",
      connectionTokens: (headers["connection"] ?? "").split(separator: ",").map {
        $0.trimmingCharacters(in: .whitespaces)
      },
      accept: headers["sec-websocket-accept"] ?? "",
      negotiatedProtocol: headers["sec-websocket-protocol"] ?? "",
      extensions: headers["sec-websocket-extensions"] ?? "")
  }

  private static func acceptKey(for key: String) -> String {
    let digest = Insecure.SHA1.hash(data: Data((key + Const.wsGuid).utf8))
    return Data(digest).base64EncodedString()
  }

  private static func findField(_ fields: [(String, String)], _ name: String) -> (String, String)? {
    fields.first { $0.0.lowercased() == name.lowercased() }
  }

  /// Finds the first occurrence of `needle` in `haystack` (Array has no
  /// range(of:) — that's a String API).
  private static func firstRange(of needle: [UInt8], in haystack: [UInt8]) -> Range<Int>? {
    guard !needle.isEmpty, haystack.count >= needle.count else { return nil }
    let limit = haystack.count - needle.count
    var start = 0
    while start <= limit {
      if haystack[start..<(start + needle.count)].elementsEqual(needle) {
        return start..<(start + needle.count)
      }
      start += 1
    }
    return nil
  }

  private static func isValidCloseCode(_ code: UInt16) -> Bool {
    (1000...1003).contains(code) || (1007...1014).contains(code) || (3000...4999).contains(code)
  }

  /// Close payload: code + reason trimmed to 123 bytes on a UTF-8
  /// boundary (port of nitro's closePayload).
  private static func closePayload(code: UInt16, reason: String) -> [UInt8] {
    let bytes = Array(reason.utf8)
    var len = min(bytes.count, 123)
    while len > 0 && len < bytes.count && (bytes[len] & 0xC0) == 0x80 { len -= 1 }
    return [UInt8((code >> 8) & 0xFF), UInt8(code & 0xFF)] + bytes[..<len]
  }

  private static func isSecure(_ url: URL) -> Bool {
    let scheme = url.scheme?.lowercased() ?? ""
    return scheme == "wss" || scheme == "https"
  }

  private static func isValid(_ url: URL) -> Bool {
    let scheme = url.scheme?.lowercased() ?? ""
    guard ["ws", "wss", "http", "https"].contains(scheme), url.host != nil else { return false }
    guard let port = url.port else { return true }
    return port > 0 && port <= 65535
  }
}

/// Thread-safe slot for the pinning verify block to record its failure
/// message ahead of the TLS error it triggers.
final class PinningFailureSlot {
  private let lock = NSLock()
  var message: String? {
    get {
      lock.lock()
      defer { lock.unlock() }
      return stored
    }
    set {
      lock.lock()
      stored = newValue
      lock.unlock()
    }
  }
  private var stored: String?
}

/// The WebSocket half of the shim: a registry of live connections.
final class WebSocketShim {
  static let shared = WebSocketShim()

  private let lock = NSLock()
  private var connections: [UInt64: WebSocketConnection] = [:]
  private var pendingHeaders: [UInt64: [(String, String)]] = [:]

  func addHeader(id: UInt64, name: String, value: String) {
    lock.lock()
    pendingHeaders[id, default: []].append((name, value))
    lock.unlock()
  }

  func open(id: UInt64, url urlString: String) {
    lock.lock()
    let headers = pendingHeaders.removeValue(forKey: id) ?? []
    let connection = WebSocketConnection(id: id, headers: headers)
    connections[id] = connection
    lock.unlock()
    connection.openConnection(urlString: urlString)
  }

  func send(id: UInt64, data: [UInt8], isBinary: Bool) {
    withConnection(id) { $0.send(data: data, isBinary: isBinary) }
  }

  func sendPing(id: UInt64, data: [UInt8]) {
    withConnection(id) { $0.sendPing(data) }
  }

  func sendPong(id: UInt64, data: [UInt8]) {
    withConnection(id) { $0.sendPong(data) }
  }

  func close(id: UInt64, code: UInt16, reason: String) {
    withConnection(id) { $0.closeConnection(code: code, reason: reason) }
  }

  /// Force cancel; the Rust cancel path removes its state first, so the
  /// terminal event this triggers is a no-op there.
  func cancel(id: UInt64) {
    withConnection(id) { $0.cancelConnection() }
  }

  /// Connections remove themselves when they finish.
  func completed(id: UInt64) {
    lock.lock()
    connections.removeValue(forKey: id)
    pendingHeaders.removeValue(forKey: id)
    lock.unlock()
  }

  private func withConnection(_ id: UInt64, _ body: (WebSocketConnection) -> Void) {
    lock.lock()
    let connection = connections[id]
    lock.unlock()
    connection.map(body)
  }
}

// ---- data-plane glue (pointer conversions live in BridgeGlue.swift) ----

@inline(__always)
func notifyWsMessage(_ id: UInt64, isBinary: Bool, data: [UInt8]) {
  data.withUnsafeBufferPointer { buf in
    monet_on_ws_message(id, isBinary, buf.baseAddress, buf.count)
  }
}

@inline(__always)
func notifyWsPing(_ id: UInt64, data: [UInt8]) {
  data.withUnsafeBufferPointer { buf in
    monet_on_ws_ping(id, buf.baseAddress, buf.count)
  }
}

@inline(__always)
func notifyWsPong(_ id: UInt64, data: [UInt8]) {
  data.withUnsafeBufferPointer { buf in
    monet_on_ws_pong(id, buf.baseAddress, buf.count)
  }
}

@inline(__always)
func notifyWsClose(_ id: UInt64, code: UInt16, reason: String, wasClean: Bool, error: String) {
  let reasonBytes = Array(reason.utf8)
  let errorBytes = Array(error.utf8)
  reasonBytes.withUnsafeBufferPointer { reasonBuf in
    errorBytes.withUnsafeBufferPointer { errorBuf in
      monet_on_ws_close(
        id, code, reasonBuf.baseAddress, reasonBuf.count, wasClean,
        errorBuf.baseAddress, errorBuf.count)
    }
  }
}

// ---- control plane: Rust (via cpp/trampoline.cpp) calls these ----

@_expose(Cxx)
public func monetWsAddHeader(
  _ id: UInt64,
  _ namePtr: UnsafePointer<UInt8>,
  _ nameLen: Int,
  _ valuePtr: UnsafePointer<UInt8>,
  _ valueLen: Int
) {
  let name = bridgeString(namePtr, nameLen)
  let value = bridgeString(valuePtr, valueLen)
  WebSocketShim.shared.addHeader(id: id, name: name, value: value)
}

@_expose(Cxx)
public func monetWsOpen(_ id: UInt64, _ urlPtr: UnsafePointer<UInt8>, _ urlLen: Int) {
  let url = bridgeString(urlPtr, urlLen)
  WebSocketShim.shared.open(id: id, url: url)
}

@_expose(Cxx)
public func monetWsSend(
  _ id: UInt64, _ isBinary: Bool, _ dataPtr: UnsafePointer<UInt8>, _ dataLen: Int
) {
  let data = [UInt8](UnsafeBufferPointer(start: dataPtr, count: dataLen))
  WebSocketShim.shared.send(id: id, data: data, isBinary: isBinary)
}

@_expose(Cxx)
public func monetWsSendPing(_ id: UInt64, _ dataPtr: UnsafePointer<UInt8>, _ dataLen: Int) {
  let data = [UInt8](UnsafeBufferPointer(start: dataPtr, count: dataLen))
  WebSocketShim.shared.sendPing(id: id, data: data)
}

@_expose(Cxx)
public func monetWsSendPong(_ id: UInt64, _ dataPtr: UnsafePointer<UInt8>, _ dataLen: Int) {
  let data = [UInt8](UnsafeBufferPointer(start: dataPtr, count: dataLen))
  WebSocketShim.shared.sendPong(id: id, data: data)
}

@_expose(Cxx)
public func monetWsClose(
  _ id: UInt64, _ code: UInt16, _ reasonPtr: UnsafePointer<UInt8>, _ reasonLen: Int
) {
  let reason = bridgeString(reasonPtr, reasonLen)
  WebSocketShim.shared.close(id: id, code: code, reason: reason)
}

@_expose(Cxx)
public func monetWsCancel(_ id: UInt64) {
  WebSocketShim.shared.cancel(id: id)
}
