//! Per-request callback state machine: Cronet's callback threads bridged
//! into monet-core's boxed future and streaming body.
//!
//! Ownership scheme (the subtle part):
//! - The callback object's `client_context` holds one strong `Arc`
//!   reference to [`RequestState`]; the execute future holds another.
//! - Terminal callbacks (`OnSucceeded` / `OnFailed` / `OnCanceled`) adopt
//!   that reference with `Arc::from_raw` and drop it on return — the API
//!   guarantees no callback fires after a terminal one. Non-terminal
//!   callbacks only borrow it.
//! - [`RequestState`] owns the callback object and, through
//!   [`RequestGuard`], the request; both are destroyed when the last
//!   reference goes away.

use bytes::Bytes;
use futures_channel::oneshot;
use std::ffi::CStr;
use std::sync::{Arc, Mutex};

use monet_core::body::{BodySender, channel as body_channel};
use monet_core::error::{Error, PlatformErrorCode, Result};
use monet_core::headers::HeaderMap;
use monet_core::response::Response;

use monet_cronet_sys as sys;

/// Read buffer size for response chunks.
const READ_CHUNK: usize = 64 * 1024;

pub(crate) struct RequestState {
    /// Resolves the execute future; taken exactly once.
    head_tx: Mutex<Option<oneshot::Sender<Result<Response>>>>,
    /// Body channel half; taken by the first terminal-style event.
    body_tx: Mutex<Option<BodySender>>,
    /// The Cronet request, destroyed here (terminal callbacks may also take
    /// it, which the API explicitly allows).
    request: Mutex<Option<RequestGuard>>,
    /// The callback object itself, destroyed with the state.
    callback: sys::Cronet_UrlRequestCallbackPtr,
}

impl RequestState {
    pub(crate) fn new(head_tx: oneshot::Sender<Result<Response>>) -> Self {
        RequestState {
            head_tx: Mutex::new(Some(head_tx)),
            body_tx: Mutex::new(None),
            request: Mutex::new(None),
            callback: std::ptr::null_mut(),
        }
    }

    pub(crate) fn set_callback(&mut self, callback: sys::Cronet_UrlRequestCallbackPtr) {
        self.callback = callback;
    }

    pub(crate) fn set_request(&self, request: sys::Cronet_UrlRequestPtr) {
        *self.request.lock().unwrap() = Some(RequestGuard(request));
    }

    pub(crate) fn clear_request(&self) {
        self.request.lock().unwrap().take();
    }
}

/// Owns a `Cronet_UrlRequest`; dropping cancels (no-op once finished) and
/// destroys it.
pub(crate) struct RequestGuard(pub(crate) sys::Cronet_UrlRequestPtr);

impl RequestGuard {
    fn cancel(&self) {
        unsafe { sys::Cronet_UrlRequest_Cancel(self.0) };
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.cancel();
        unsafe { sys::Cronet_UrlRequest_Destroy(self.0) };
    }
}

impl RequestState {
    /// Borrows the state behind a callback's client context.
    fn from_callback(callback: sys::Cronet_UrlRequestCallbackPtr) -> &'static RequestState {
        unsafe {
            &*(sys::Cronet_UrlRequestCallback_GetClientContext(callback) as *const RequestState)
        }
    }

    /// Adopts the client-context reference; terminal callbacks call this at
    /// entry so the state dies with them.
    fn adopt(callback: sys::Cronet_UrlRequestCallbackPtr) -> Arc<RequestState> {
        unsafe {
            Arc::from_raw(
                sys::Cronet_UrlRequestCallback_GetClientContext(callback) as *const RequestState
            )
        }
    }

    pub(crate) fn cancel_request(&self) {
        if let Some(guard) = self.request.lock().unwrap().as_ref() {
            guard.cancel();
        }
    }

    /// Delivers the response head (or an error). Returns `false` when the
    /// receiving side is gone — the caller should cancel the request.
    fn deliver_head(&self, result: Result<Response>) -> bool {
        match self.head_tx.lock().unwrap().take() {
            Some(tx) => tx.send(result).is_ok(),
            None => true, // already delivered; nothing to do
        }
    }

    fn abort_body(&self, error: Error) {
        if let Some(sender) = self.body_tx.lock().unwrap().take() {
            sender.abort(error);
        }
    }

    fn finish_request(&self) {
        self.request.lock().unwrap().take();
    }
}

impl Drop for RequestState {
    fn drop(&mut self) {
        unsafe { sys::Cronet_UrlRequestCallback_Destroy(self.callback) };
    }
}

// Safety: the raw Cronet handles it carries are opaque pointers that are
// safe to use from any thread (Cronet's request/engine APIs are
// thread-safe); all Rust-side mutation goes through Mutexes.
unsafe impl Send for RequestState {}
unsafe impl Sync for RequestState {}

pub(crate) unsafe extern "C" fn on_redirect_received(
    _self: sys::Cronet_UrlRequestCallbackPtr,
    request: sys::Cronet_UrlRequestPtr,
    _info: sys::Cronet_UrlResponseInfoPtr,
    _new_location_url: sys::Cronet_String,
) {
    // v1 policy: always follow. A configurable policy lands with monet-rn.
    unsafe { sys::Cronet_UrlRequest_FollowRedirect(request) };
}

pub(crate) unsafe extern "C" fn on_response_started(
    _self: sys::Cronet_UrlRequestCallbackPtr,
    request: sys::Cronet_UrlRequestPtr,
    info: sys::Cronet_UrlResponseInfoPtr,
) {
    let state = RequestState::from_callback(_self);
    let status = unsafe { sys::Cronet_UrlResponseInfo_http_status_code_get(info) } as u16;
    let headers = read_headers(info);
    let (body, sender) = body_channel();
    *state.body_tx.lock().unwrap() = Some(sender);
    if !state.deliver_head(Ok(Response::new(status, headers, body))) {
        state.cancel_request();
        return;
    }
    start_read(state, request);
}

pub(crate) unsafe extern "C" fn on_read_completed(
    _self: sys::Cronet_UrlRequestCallbackPtr,
    request: sys::Cronet_UrlRequestPtr,
    _info: sys::Cronet_UrlResponseInfoPtr,
    buffer: sys::Cronet_BufferPtr,
    bytes_read: u64,
) {
    let state = RequestState::from_callback(_self);
    let bytes = unsafe {
        let data = sys::Cronet_Buffer_GetData(buffer) as *const u8;
        let slice = std::slice::from_raw_parts(data, bytes_read as usize);
        Bytes::copy_from_slice(slice)
    };
    unsafe { sys::Cronet_Buffer_Destroy(buffer) };

    if !bytes.is_empty() {
        let delivered = match state.body_tx.lock().unwrap().as_ref() {
            Some(sender) => sender.send_chunk(bytes).is_ok(),
            None => false, // body already terminated (or aborted)
        };
        if !delivered {
            state.cancel_request();
            return;
        }
    }
    start_read(state, request);
}

pub(crate) unsafe extern "C" fn on_succeeded(
    _self: sys::Cronet_UrlRequestCallbackPtr,
    _request: sys::Cronet_UrlRequestPtr,
    _info: sys::Cronet_UrlResponseInfoPtr,
) {
    let state = RequestState::adopt(_self);
    if let Some(sender) = state.body_tx.lock().unwrap().take() {
        sender.complete();
    }
    state.finish_request();
}

pub(crate) unsafe extern "C" fn on_failed(
    _self: sys::Cronet_UrlRequestCallbackPtr,
    _request: sys::Cronet_UrlRequestPtr,
    _info: sys::Cronet_UrlResponseInfoPtr,
    error: sys::Cronet_ErrorPtr,
) {
    let state = RequestState::adopt(_self);
    let err = read_error(error);
    if state.head_tx.lock().unwrap().is_some() {
        state.deliver_head(Err(err));
    } else {
        state.abort_body(err);
    }
    state.finish_request();
}

pub(crate) unsafe extern "C" fn on_canceled(
    _self: sys::Cronet_UrlRequestCallbackPtr,
    _request: sys::Cronet_UrlRequestPtr,
    _info: sys::Cronet_UrlResponseInfoPtr,
) {
    let state = RequestState::adopt(_self);
    if state.head_tx.lock().unwrap().is_some() {
        state.deliver_head(Err(Error::Cancelled));
    } else {
        state.abort_body(Error::Cancelled);
    }
    state.finish_request();
}

/// Allocates a fresh buffer and pulls the next chunk. Cronet's reads are
/// pull-driven: nothing arrives until `Read` is called.
fn start_read(state: &RequestState, request: sys::Cronet_UrlRequestPtr) {
    unsafe {
        let buffer = sys::Cronet_Buffer_Create();
        sys::Cronet_Buffer_InitWithAlloc(buffer, READ_CHUNK as u64);
        let result = sys::Cronet_UrlRequest_Read(request, buffer);
        if result != sys::Cronet_RESULT_Cronet_RESULT_SUCCESS {
            sys::Cronet_Buffer_Destroy(buffer);
            state.cancel_request();
        }
    }
}

fn read_headers(info: sys::Cronet_UrlResponseInfoPtr) -> HeaderMap {
    let mut headers = HeaderMap::new();
    unsafe {
        let count = sys::Cronet_UrlResponseInfo_all_headers_list_size(info);
        for i in 0..count {
            let header = sys::Cronet_UrlResponseInfo_all_headers_list_at(info, i);
            let name = CStr::from_ptr(sys::Cronet_HttpHeader_name_get(header))
                .to_string_lossy()
                .into_owned();
            let value = CStr::from_ptr(sys::Cronet_HttpHeader_value_get(header))
                .to_string_lossy()
                .into_owned();
            // A response header that fails our validation rules is dropped,
            // not fatal.
            let _ = headers.append(&name, value);
        }
    }
    headers
}

fn read_error(error: sys::Cronet_ErrorPtr) -> Error {
    unsafe {
        let code = i64::from(sys::Cronet_Error_error_code_get(error));
        let internal = i64::from(sys::Cronet_Error_internal_error_code_get(error));
        let message = CStr::from_ptr(sys::Cronet_Error_message_get(error)).to_string_lossy();
        Error::transport_with_code(
            format!("cronet: {message} (error {code}, internal {internal})"),
            PlatformErrorCode::new("cronet", code),
        )
    }
}
