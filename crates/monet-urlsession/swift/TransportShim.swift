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

  /// Called by a task delegate on completion so the box doesn't leak.
  func completed(id: UInt64) {
    lock.lock()
    boxes.removeValue(forKey: id)
    lock.unlock()
  }
}

/// The per-task delegate: pushes URLSession events into the Rust data
/// plane. All methods run on the session's serial delegate queue.
final class TaskDelegate: NSObject, URLSessionDataDelegate {
  let id: UInt64
  let followRedirects: Bool

  init(id: UInt64, followRedirects: Bool) {
    self.id = id
    self.followRedirects = followRedirects
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
    notifyChunk(id, data: data)
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
      notifyError(id, code: error.code, message: error.localizedDescription)
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
