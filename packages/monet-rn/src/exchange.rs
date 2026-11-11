//! The exchange recorder (v2/W3b): one complete record per fetch,
//! delivered at its terminal — the final (post-hook) request head, the
//! request body when it was buffered, the response head the pump
//! delivered, a capped prefix of the response body, sizes, duration,
//! and the error when the fetch failed. The nitro-fetch
//! NetworkInspector's record shape, fed from the pump so every field is
//! the final form (nitro's JS-side capture sees pre-hook requests and
//! only bodies the consumer read).
//!
//! Delivery is a Drop guard: the pump owns one, fills it as the fetch
//! progresses, and every exit path — completion, failure, cancellation
//! — fires the record exactly once when the guard falls out of scope.
//! The existing hook/observer semantics are untouched.

use std::sync::Arc;
use std::time::Instant;

use jsi_turbo::TurboCallback;

use crate::HeaderKV;

/// The body view a record carries: a UTF-8 text prefix when the bytes
/// read as text (capped; a binary body reports `None` here — the size
/// still tells the truth), plus the full byte count.
#[derive(serde::Serialize, serde::Deserialize, jsi_turbo::TurboStruct, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeBody {
    pub text: Option<String>,
    pub size: f64,
}

/// One fetch's complete record, fired once at its terminal.
#[derive(serde::Serialize, serde::Deserialize, jsi_turbo::TurboStruct, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeRecord {
    pub correlation_id: f64,
    pub url: String,
    pub method: String,
    pub request_headers: Vec<HeaderKV>,
    /// The buffered request body; a streamed one (W2a) has no complete
    /// form at any single point — the record leaves it out.
    pub request_body: Option<ExchangeBody>,
    /// 0 when the fetch failed before a head.
    pub status: f64,
    pub response_headers: Vec<HeaderKV>,
    pub response_body: Option<ExchangeBody>,
    pub duration_ms: f64,
    /// The terminal error (`kind: message`), when the fetch failed.
    pub error: Option<String>,
}

/// The response-body capture cap: the first 64KB of text (binary
/// records report size only — the zero-copy ArrayBuffer take is the
/// recorded growth path, not in this protocol version).
pub(crate) const BODY_CAP: usize = 64 * 1024;

/// The pump-side capture state. Fills as the fetch progresses; the
/// `Drop` impl fires the record to every registered recorder (the
/// snapshot taken at spawn — the same snapshot semantics the hook
/// registry's clones carry).
pub(crate) struct ExchangeGuard {
    recorders: Vec<Arc<TurboCallback>>,
    correlation_id: u64,
    started: Instant,
    url: String,
    method: String,
    request_headers: Vec<HeaderKV>,
    request_body: Option<ExchangeBody>,
    status: f64,
    response_headers: Vec<HeaderKV>,
    response_capture: Vec<u8>,
    response_size: usize,
    error: Option<String>,
    /// Fired once: the explicit terminal calls fire before the JS
    /// callbacks (an await can resume before the guard's Drop runs);
    /// Drop backstops every other exit.
    fired: bool,
}

impl ExchangeGuard {
    /// Starts the clock, seeded with the call site's head; the guard
    /// fires on drop however the pump exits.
    pub(crate) fn new(
        recorders: Vec<Arc<TurboCallback>>,
        correlation_id: u64,
        seed_url: &str,
        seed_method: &str,
        seed_headers: &[HeaderKV],
    ) -> Self {
        ExchangeGuard {
            recorders,
            correlation_id,
            started: Instant::now(),
            url: seed_url.to_string(),
            method: seed_method.to_string(),
            request_headers: seed_headers.to_vec(),
            request_body: None,
            status: 0.0,
            response_headers: Vec::new(),
            response_capture: Vec::new(),
            response_size: 0,
            error: None,
            fired: false,
        }
    }

    /// The request's final form (the post-hook head; the pre-hook one
    /// for exits before the hooks ran — the best available).
    pub(crate) fn observe_request(
        &mut self,
        request: &monet_core::request::Request,
    ) {
        self.url = request.url.to_string();
        self.method = request.method.as_str().to_string();
        self.request_headers = request
            .headers
            .iter_flattened()
            .map(|(name, value)| HeaderKV {
                name: name.to_string(),
                value: value.to_string(),
            })
            .collect();
    }

    /// The buffered request body (the complete, final bytes — hooks do
    /// not rewrite bodies).
    pub(crate) fn observe_request_body(&mut self, bytes: &[u8]) {
        self.request_body = Some(ExchangeBody {
            text: monet_core::body::utf8_debug_prefix(&bytes[..bytes.len().min(BODY_CAP)]),
            size: bytes.len() as f64,
        });
    }

    /// The response head the pump is about to deliver (post-hook, the
    /// same form on_head reports).
    pub(crate) fn observe_response_head(
        &mut self,
        status: u16,
        headers: &[HeaderKV],
    ) {
        self.status = f64::from(status);
        self.response_headers = headers.to_vec();
    }

    /// One response chunk passing through the pump: extend the capture
    /// while under the cap, count the size always.
    pub(crate) fn observe_response_chunk(&mut self, bytes: &[u8]) {
        self.response_size += bytes.len();
        if self.response_capture.len() < BODY_CAP {
            let room = BODY_CAP - self.response_capture.len();
            let take = room.min(bytes.len());
            self.response_capture.extend_from_slice(&bytes[..take]);
        }
    }

    /// The terminal error (`kind: message`).
    pub(crate) fn fail(&mut self, kind: &str, message: &str) {
        self.error = Some(format!("{kind}: {message}"));
    }

    /// Fires the record (idempotent). The pump calls this BEFORE its
    /// terminal JS callbacks — an awaiting consumer can resume before
    /// the guard's Drop would run — and Drop backstops the rest.
    pub(crate) fn fire(&mut self) {
        if self.fired || self.recorders.is_empty() {
            return;
        }
        self.fired = true;
        let record = ExchangeRecord {
            correlation_id: self.correlation_id as f64,
            url: std::mem::take(&mut self.url),
            method: std::mem::take(&mut self.method),
            request_headers: std::mem::take(&mut self.request_headers),
            request_body: self.request_body.take(),
            status: self.status,
            response_headers: std::mem::take(&mut self.response_headers),
            response_body: if self.response_size > 0 || self.status > 0.0 {
                Some(ExchangeBody {
                    text: monet_core::body::utf8_debug_prefix(&self.response_capture),
                    size: self.response_size as f64,
                })
            } else {
                None
            },
            duration_ms: self.started.elapsed().as_millis() as f64,
            error: self.error.take(),
        };
        for recorder in &self.recorders {
            recorder.call(record.clone());
        }
    }
}

impl Drop for ExchangeGuard {
    fn drop(&mut self) {
        // The backstop: every exit that did not fire explicitly (a
        // state-gone return mid-body, a panic) still records once.
        self.fire();
    }
}
