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
  /// Set once the task exists; nil while still assembling.
  var task: URLSessionDataTask?
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

  func start(id: UInt64, url urlString: String, method: String, followRedirects: Bool, timeoutMs: UInt64) {
    lock.lock()
    // Get-or-create: a headerless, bodyless request never touched the box
    // through addHeader/setBody, and it still has to start.
    let box = lockedBox(for: id)
    let pending = (headers: box.headers, body: box.body, hasBody: box.hasBody)
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

    let task = session.dataTask(with: request)
    task.delegate = TaskDelegate(id: id, followRedirects: followRedirects)

    lock.lock()
    // A cancel may have arrived while the request was being built; the
    // box would then be gone — don't start an orphaned task.
    guard boxes[id] != nil else {
      lock.unlock()
      return
    }
    box.task = task
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

  /// The body backpressure valve (v2/W2b): Rust probed the bounded
  /// channel full, so data delivery pauses until the consumer drains.
  func suspend(id: UInt64) {
    lock.lock()
    let task = boxes[id]?.task
    lock.unlock()
    task?.suspend()
  }

  /// The valve's resume edge: the Rust body watcher calls it after
  /// draining the overflow. A no-op on finished tasks.
  func resume(id: UInt64) {
    lock.lock()
    let task = boxes[id]?.task
    lock.unlock()
    task?.resume()
  }

  /// Called by a task delegate on completion so the box doesn't leak.
  func completed(id: UInt64) {
    lock.lock()
    boxes.removeValue(forKey: id)
    lock.unlock()
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
    let paused = notifyChunk(id, data: data)
    if paused {
      // The bounded body channel is full (v2/W2b): stop data delivery
      // until the consumer drains — the Rust body watcher resumes
      // through transportResume. Suspend only pauses delivery; the
      // task's lifecycle (cancel, completion) is untouched.
      TransportShim.shared.suspend(id: id)
    }
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

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    TransportShim.shared.completed(id: id)
    if let error = error as NSError? {
      if error.code == NSURLErrorCancelled, let reason = pinRejection {
        // A rejected pin surfaces as cancellation; remap so Rust reports
        // a certificate failure instead of Error::Cancelled.
        notifyError(
          id, code: NSURLErrorServerCertificateUntrusted, message: reason)
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
public func monetTransportResume(_ id: UInt64) {
  TransportShim.shared.resume(id: id)
}
