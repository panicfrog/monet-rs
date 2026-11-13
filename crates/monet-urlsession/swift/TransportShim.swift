// The URLSession delegate state machine and the control-plane entry
// points exposed to C++ (`@_expose(Cxx)`). Modeled on nitro-fetch's
// NitroBufferedTaskDelegate: one shared session, a delegate per task,
// events pushed straight into the Rust data plane — no buffering
// heuristics (v1 streams everything, as the plan records).

import Foundation
import monet_urlsession_bridge

/// Per-request assembly state, guarded by `TransportShim.lock`.
final class RequestBox {
  /// Headers gathered through transport_add_header, applied when the
  /// URLRequest is built.
  var headers: [(String, String)] = []
  var body = Data()
  var hasBody = false
  /// v2/W2c: the body streams through an upload pipe instead of
  /// httpBody.
  var streamed = false
  var uploadPipe: UploadPipe?
  /// Set once the task exists; nil while still assembling.
  var task: URLSessionDataTask?
}

/// One streamed upload's plumbing (v2/W2c): a bound stream pair whose
/// input half URLSession reads as the request body (the task's
/// needNewBodyStream callback hands it over) and whose output half the
/// Rust feeder writes through the control plane. The pair's buffer
/// bounds the in-flight bytes — a full buffer parks the write until
/// URLSession drains it, which is the upload's natural backpressure.
final class UploadPipe: NSObject {
  let input: InputStream
  let output: OutputStream
  private let lock = NSLock()
  private var ended = false
  /// Set once the input half was handed to the session — a second ask
  /// (a body-preserving redirect) cannot be served: a stream cannot
  /// replay.
  private var handedOver = false

  override init() {
    var readStream: Unmanaged<CFReadStream>?
    var writeStream: Unmanaged<CFWriteStream>?
    // 64KB of in-flight upload bytes — the same order as the response
    // channel's bound. The pair creation only allocates; a nil unwrap
    // here is malloc-failure territory.
    CFStreamCreateBoundPair(nil, &readStream, &writeStream, 64 * 1024)
    input = readStream!.takeRetainedValue()
    output = writeStream!.takeRetainedValue()
    super.init()
    // The output half opens now (the write loop feeds it); the input
    // half stays closed — the session opens and schedules it itself
    // when needNewBodyStream hands it over (a pre-opened stream breaks
    // the session's management: it errors and re-asks in a loop, as the
    // host battery traced).
    output.open()
  }

  private func isEnded() -> Bool {
    lock.lock()
    defer { lock.unlock() }
    return ended
  }

  private func markEnded() {
    lock.lock()
    ended = true
    lock.unlock()
  }

  /// The input half for needNewBodyStream. Nil when this pipe already
  /// served one ask (a replay — a redirect that preserves the body) or
  /// has ended: the task then fails, the same refusal the Android
  /// provider's rewind makes.
  func takeInput() -> InputStream? {
    lock.lock()
    defer { lock.unlock() }
    guard !handedOver, !ended else { return nil }
    handedOver = true
    return input
  }

  /// Writes one chunk from the Rust feeder's thread. False means the
  /// pipe ended (the task is gone) — the feeder stops.
  func write(_ data: Data) -> Bool {
    guard !data.isEmpty else { return !isEnded() }
    var offset = 0
    let count = data.count
    return data.withUnsafeBytes { (raw: UnsafeRawBufferPointer) -> Bool in
      guard let base = raw.baseAddress?.assumingMemoryBound(to: UInt8.self) else { return true }
      while offset < count {
        if isEnded() { return false }
        let written = output.write(base + offset, maxLength: count - offset)
        if written > 0 {
          offset += written
        } else if written == 0 {
          // The pair's buffer is full: URLSession hasn't drained it.
          // Poll — the session reads at the network's pace, and a
          // bounded poll keeps an ended pipe from parking this thread.
          Thread.sleep(forTimeInterval: 0.05)
        } else {
          markEnded()
          return false
        }
      }
      return true
    }
  }

  /// Ends the stream cleanly: the input half sees EOF, which completes
  /// the request body. Deliberately NOT `markEnded()`: a fast producer
  /// routinely reaches EOF before the session's first needNewBodyStream
  /// ask (the bytes wait in the pair's buffer), and that ask must still
  /// be served — refusing it failed the upload as a cancellation.
  /// `ended` means the task is gone, nothing else.
  func finish() {
    output.close()
  }
}

/// The shared transport: one URLSession, one registry of live requests.
final class TransportShim {
  static let shared = TransportShim()

  /// nitro-fetch parity: one shared session over the default
  /// configuration (system URLCache policy included). delegateQueue is
  /// nil, so all delegate events for a request arrive on one serial
  /// queue — the ordering contract in src/ffi.rs relies on that.
  private let session = URLSession(configuration: .default)

  private let lock = NSLock()
  private var boxes: [UInt64: RequestBox] = [:]

  /// Returns (creating if needed) the box for `id`. Callers must hold
  /// `lock`.
  private func lockedBox(for id: UInt64) -> RequestBox {
    if let box = boxes[id] { return box }
    let box = RequestBox()
    boxes[id] = box
    return box
  }

  func addHeader(id: UInt64, name: String, value: String) {
    lock.lock()
    lockedBox(for: id).headers.append((name, value))
    lock.unlock()
  }

  func setBody(id: UInt64, data: Data) {
    lock.lock()
    let box = lockedBox(for: id)
    box.body = data
    box.hasBody = true
    lock.unlock()
  }

  /// Marks the request's body as streamed (v2/W2c): start then builds
  /// an uploadTask(withStreamedRequest:) whose body the pipe feeds.
  func setStreaming(id: UInt64) {
    lock.lock()
    lockedBox(for: id).streamed = true
    lock.unlock()
  }

  func start(id: UInt64, url urlString: String, method: String, followRedirects: Bool, timeoutMs: UInt64) {
    lock.lock()
    // Get-or-create: a headerless, bodyless request never touched the box
    // through addHeader/setBody, and it still has to start.
    let box = lockedBox(for: id)
    let pending = (
      headers: box.headers, body: box.body, hasBody: box.hasBody, streamed: box.streamed
    )
    lock.unlock()

    guard let url = URL(string: urlString) else {
      lock.lock()
      boxes.removeValue(forKey: id)
      lock.unlock()
      notifyError(id, code: NSURLErrorBadURL, message: "invalid URL: \(urlString)")
      return
    }
    var request = URLRequest(url: url)
    request.httpMethod = method
    if timeoutMs > 0 {
      request.timeoutInterval = Double(timeoutMs) / 1000
    }
    for (name, value) in pending.headers {
      request.addValue(value, forHTTPHeaderField: name)
    }
    if pending.hasBody {
      request.httpBody = pending.body
    }

    // v2/W2c: a streamed body has no httpBody — the session asks for
    // it through needNewBodyStream and the pipe feeds it. The pipe is
    // created here so control-plane writes (which start once
    // transport_start returns) always find it.
    let pipe = pending.streamed ? UploadPipe() : nil

    let task: URLSessionTask
    if pending.streamed {
      task = session.uploadTask(withStreamedRequest: request)
    } else {
      task = session.dataTask(with: request)
    }
    task.delegate = TaskDelegate(id: id, followRedirects: followRedirects)

    lock.lock()
    // A cancel may have arrived while the request was being built; the
    // box would then be gone — don't start an orphaned task.
    guard boxes[id] != nil else {
      lock.unlock()
      return
    }
    box.task = task as? URLSessionDataTask
    box.uploadPipe = pipe
    lock.unlock()
    task.resume()
  }

  func cancel(id: UInt64) {
    lock.lock()
    let task = boxes[id]?.task
    if task == nil {
      // Never started: discard silently; Rust dropped its state on its
      // cancel path, so no events may follow.
      boxes.removeValue(forKey: id)
    }
    lock.unlock()
    // Cancelling a running task fires didCompleteWithError with
    // NSURLErrorCancelled, which reports through the data plane.
    task?.cancel()
  }

  /// The input half for the task's needNewBodyStream ask (v2/W2c). Nil
  /// refuses the ask — see UploadPipe.takeInput.
  func uploadStream(for id: UInt64) -> InputStream? {
    lock.lock()
    let pipe = boxes[id]?.uploadPipe
    lock.unlock()
    return pipe?.takeInput()
  }

  /// One chunk into the upload pipe (v2/W2c). False ends the feeder:
  /// the pipe is gone or the task finished.
  func uploadWrite(id: UInt64, data: Data) -> Bool {
    lock.lock()
    let pipe = boxes[id]?.uploadPipe
    lock.unlock()
    guard let pipe else { return false }
    return pipe.write(data)
  }

  /// The body ended cleanly: EOF.
  func uploadFinish(id: UInt64) {
    lock.lock()
    let pipe = boxes[id]?.uploadPipe
    lock.unlock()
    pipe?.finish()
  }

  /// The body failed: cancel the task, remapping the cancellation into
  /// an upload error (the pinning rejection's pattern).
  func uploadFail(id: UInt64) {
    lock.lock()
    let box = boxes[id]
    lock.unlock()
    box?.uploadPipe?.finish()
    if let delegate = box?.task?.delegate as? TaskDelegate {
      delegate.uploadFailure = "the upload body failed (the producer's stream errored)"
    }
    box?.task?.cancel()
  }

  /// Called by a task delegate on completion so the box doesn't leak.
  func completed(id: UInt64) {
    lock.lock()
    let box = boxes.removeValue(forKey: id)
    lock.unlock()
    // Release the pipe (a parked feeder write exits) and end the feeder
    // itself (it may be parked in the body stream with nothing left to
    // write — the data-plane wake reaches it).
    box?.uploadPipe?.finish()
    if box?.streamed == true {
      on_upload_done(id)
    }
  }
}

/// The per-task delegate: pushes URLSession events into the Rust data
/// plane. All methods run on the session's serial delegate queue, except
/// the pinning evaluation's async tail.
final class TaskDelegate: NSObject, URLSessionDataDelegate {
  let id: UInt64
  let followRedirects: Bool
  /// Set (before the challenge handler returns) when pinning rejected
  /// the chain; the rejection completes the task as a cancellation, so
  /// didCompleteWithError remaps it to a certificate error.
  var pinRejection: String?
  /// Set when a streamed upload's producer failed (v2/W2c); the
  /// task.cancel() it triggers remaps the same way.
  var uploadFailure: String?
  /// Set when the session asked for a streamed body a second time (it
  /// restarts a request in some connection-loss cases) and the ask was
  /// refused; the task then ends as a cancellation, remapped so the
  /// failure names its real cause instead of reading as an abort.
  var bodyReplayRefused = false

  init(id: UInt64, followRedirects: Bool) {
    self.id = id
    self.followRedirects = followRedirects
  }

  /// Server-trust challenges: pinned hosts get evaluated + matched
  /// against monet-core's pins; hosts challenged while custom roots are
  /// registered get them anchored before evaluation (configureTrust —
  /// the roots must apply to every host, not just pinned ones, or the
  /// added trust would be unreachable); everything else falls through
  /// to the system's default handling (which already performs standard
  /// chain validation — pinning is the extra layer on top).
  func urlSession(
    _ session: URLSession,
    task: URLSessionTask,
    didReceive challenge: URLAuthenticationChallenge,
    completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
  ) {
    let host = challenge.protectionSpace.host
    let hashes = PinningShim.pinnedHashes(for: host)
    let roots = PinningShim.customRoots()
    guard !hashes.isEmpty || !roots.isEmpty,
      challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust,
      let trust = challenge.protectionSpace.serverTrust
    else {
      completionHandler(.performDefaultHandling, nil)
      return
    }
    PinningShim.apply(roots, to: trust)
    PinningShim.evaluate(trust: trust, hashes: hashes) { [weak self] outcome in
      guard let self else {
        completionHandler(.performDefaultHandling, nil)
        return
      }
      switch outcome {
      case .use:
        completionHandler(.useCredential, URLCredential(trust: trust))
      case .defaultHandling:
        completionHandler(.performDefaultHandling, nil)
      case .reject:
        // Recorded before the completion handler runs, so the task's
        // didCompleteWithError (which it triggers) observes it.
        self.pinRejection = "certificate pinning failed for \(host): no pinned key in the chain"
        completionHandler(.cancelAuthenticationChallenge, nil)
      }
    }
  }

  func urlSession(
    _ session: URLSession,
    dataTask: URLSessionDataTask,
    didReceive response: URLResponse,
    completionHandler: @escaping (URLSession.ResponseDisposition) -> Void
  ) {
    // The head arrives complete: headers first, then the head event
    // (ordering contract in src/ffi.rs). allHeaderFields collapses
    // duplicate names — v1 accepts the dictionary view.
    if let http = response as? HTTPURLResponse {
      for (name, value) in http.allHeaderFields {
        notifyHeader(id, name: String(describing: name), value: String(describing: value))
      }
      notifyResponseHead(id, status: http.statusCode)
    } else {
      notifyResponseHead(id, status: 0)
    }
    completionHandler(.allow)
  }

  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
    // v2/W2b redesign: no task.suspend() valve. The instrumented
    // evidence (see the plan's W2b record) showed balanced
    // suspend/resume pairing with the task never delivering again —
    // URLSessionTask suspend/resume is not a reliable pause primitive
    // (a resumed task can simply stop delivering data and completion).
    // The consumer's pace is enforced entirely on the Rust side: the
    // bounded body channel plus the overflow the body watcher drains.
    // The boundary this leaves: the transport buffers the response at
    // network pace (the overflow), so peak memory is the response size
    // — the network pause the other two stacks get (Cronet's read
    // gate, rustls's channel capacity) has no safe equivalent here.
    _ = notifyChunk(id, data: data)
  }

  func urlSession(
    _ session: URLSession,
    task: URLSessionTask,
    willPerformHTTPRedirection response: HTTPURLResponse,
    newRequest request: URLRequest,
    completionHandler: @escaping @Sendable (URLRequest?) -> Void
  ) {
    if followRedirects {
      if let location = response.value(forHTTPHeaderField: "Location") {
        notifyRedirect(id, location: location)
      }
      completionHandler(request)
    } else {
      // nil delivers the 3xx response itself instead of following.
      completionHandler(nil)
    }
  }

  /// The session needs the request body of a streamed upload (v2/W2c):
  /// hand over the pipe's input half. This fires for the first send and
  /// again for body-preserving redirects/retries — the second kind
  /// cannot be served (a stream cannot replay; nil fails the task), the
  /// same refusal the Android provider's rewind makes.
  func urlSession(
    _ session: URLSession,
    task: URLSessionTask,
    needNewBodyStream completionHandler: @escaping (InputStream?) -> Void
  ) {
    let stream = TransportShim.shared.uploadStream(for: id)
    if stream == nil {
      bodyReplayRefused = true
    }
    completionHandler(stream)
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    TransportShim.shared.completed(id: id)
    if let error = error as NSError? {
      if error.code == NSURLErrorCancelled, let reason = pinRejection {
        // A rejected pin surfaces as cancellation; remap so Rust reports
        // a certificate failure instead of Error::Cancelled.
        notifyError(
          id, code: NSURLErrorServerCertificateUntrusted, message: reason)
      } else if error.code == NSURLErrorCancelled, bodyReplayRefused {
        notifyError(
          id, code: NSURLErrorRequestBodyStreamExhausted,
          message: "the session asked to replay a streamed request body (it restarted the request); a stream cannot replay")
      } else if error.code == NSURLErrorCancelled, let reason = uploadFailure {
        // A failed upload producer surfaces the same way.
        notifyError(
          id, code: NSURLErrorRequestBodyStreamExhausted, message: reason)
      } else {
        notifyError(id, code: error.code, message: error.localizedDescription)
      }
    } else {
      notifyDone(id)
    }
  }
}

// ---- control plane: Rust (via cpp/trampoline.cpp) calls these ----

@_expose(Cxx)
public func monetTransportAddHeader(
  _ id: UInt64,
  _ namePtr: UnsafePointer<UInt8>,
  _ nameLen: Int,
  _ valuePtr: UnsafePointer<UInt8>,
  _ valueLen: Int
) {
  let name = bridgeString(namePtr, nameLen)
  let value = bridgeString(valuePtr, valueLen)
  TransportShim.shared.addHeader(id: id, name: name, value: value)
}

@_expose(Cxx)
public func monetTransportSetBody(
  _ id: UInt64,
  _ dataPtr: UnsafePointer<UInt8>,
  _ dataLen: Int
) {
  let data = Data(bytes: dataPtr, count: dataLen)
  TransportShim.shared.setBody(id: id, data: data)
}

@_expose(Cxx)
public func monetTransportStart(
  _ id: UInt64,
  _ urlPtr: UnsafePointer<UInt8>,
  _ urlLen: Int,
  _ methodPtr: UnsafePointer<UInt8>,
  _ methodLen: Int,
  _ followRedirects: Bool,
  _ timeoutMs: UInt64
) {
  let url = bridgeString(urlPtr, urlLen)
  let method = bridgeString(methodPtr, methodLen)
  TransportShim.shared.start(
    id: id, url: url, method: method, followRedirects: followRedirects, timeoutMs: timeoutMs)
}

@_expose(Cxx)
public func monetTransportCancel(_ id: UInt64) {
  TransportShim.shared.cancel(id: id)
}

@_expose(Cxx)
public func monetTransportSetStreaming(_ id: UInt64) {
  TransportShim.shared.setStreaming(id: id)
}

@_expose(Cxx)
public func monetTransportUploadWrite(
  _ id: UInt64,
  _ dataPtr: UnsafePointer<UInt8>,
  _ dataLen: Int
) -> Bool {
  let data = Data(bytes: dataPtr, count: dataLen)
  return TransportShim.shared.uploadWrite(id: id, data: data)
}

@_expose(Cxx)
public func monetTransportUploadFinish(_ id: UInt64) {
  TransportShim.shared.uploadFinish(id: id)
}

@_expose(Cxx)
public func monetTransportUploadFail(_ id: UInt64) {
  TransportShim.shared.uploadFail(id: id)
}
