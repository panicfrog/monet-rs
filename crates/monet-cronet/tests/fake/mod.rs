//! A fake implementation of the Cronet C API symbol surface, linked into
//! test binaries in place of the real `libcronet.so`.
//!
//! The fake is driven by [`configure`] and inspects itself through
//! [`last_request`] / [`recorded_pins`] / [`engine_events`]. Requests run
//! synchronously inside `Cronet_UrlRequest_Start`: the fake delivers
//! callbacks through the app's (pass-through) executor as runnables, which
//! run inline — so by the time `Start` returns, the request has typically
//! reached its terminal callback and been destroyed, exactly like a real
//! request that completes before the start call unwinds.

#![allow(unsafe_code)]
#![allow(non_snake_case)]
#![allow(clippy::missing_safety_doc)]
// The fake implements the whole C surface; not every symbol is used by
// every test binary that links it, and unsafe-op-in-unsafe-fn is noise at
// an FFI boundary.
#![allow(dead_code)]
#![allow(unsafe_op_in_unsafe_fn)]

use monet_cronet_sys as sys;
use std::cell::Cell;
use std::ffi::{CStr, CString, c_char};
use std::os::raw::c_void;
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// World: what the tests configure and observe.
// ---------------------------------------------------------------------------

/// How the fake answers requests.
#[derive(Clone)]
pub struct FakeConfig {
    pub status: u16,
    pub status_text: String,
    pub response_headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Max bytes per `OnReadCompleted`; `usize::MAX` delivers one chunk.
    pub chunk_size: usize,
    /// When set, `OnFailed` fires (before any head).
    pub fail: Option<(i32, String)>,
    /// When set, `OnRedirectReceived` fires first; the wrapper's follow
    /// then triggers the (configured) response.
    pub redirect_to: Option<String>,
}

impl Default for FakeConfig {
    fn default() -> Self {
        FakeConfig {
            status: 200,
            status_text: "OK".into(),
            response_headers: Vec::new(),
            body: Vec::new(),
            chunk_size: usize::MAX,
            fail: None,
            redirect_to: None,
        }
    }
}

/// What the fake captured about the last started request.
#[derive(Clone)]
pub struct RecordedRequest {
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub upload: Option<Vec<u8>>,
}

/// A pin entry snapshot taken at engine start.
#[derive(Clone)]
pub struct RecordedPins {
    pub host: String,
    pub hashes: Vec<String>,
    pub include_subdomains: bool,
    pub expiration_ms: i64,
}

#[derive(Default)]
struct FakeWorld {
    config: FakeConfig,
    last_request: Option<RecordedRequest>,
    pins: Vec<RecordedPins>,
    engines_started: usize,
    engine_shutdowns: usize,
    user_agent: Option<String>,
    storage_path: Option<String>,
    upload_capture: Vec<u8>,
}

static WORLD: Mutex<Option<FakeWorld>> = Mutex::new(None);

fn with_world<R>(f: impl FnOnce(&mut FakeWorld) -> R) -> R {
    let mut guard = WORLD.lock().unwrap();
    let world = guard.get_or_insert_with(FakeWorld::default);
    f(world)
}

/// Sets how the fake answers subsequent requests.
pub fn configure(config: FakeConfig) {
    with_world(|w| w.config = config);
}

/// Clears every record and resets the config.
pub fn reset() {
    *WORLD.lock().unwrap() = Some(FakeWorld::default());
}

/// The last request the fake saw.
pub fn last_request() -> Option<RecordedRequest> {
    with_world(|w| w.last_request.clone())
}

/// Pin entries injected at engine start.
pub fn recorded_pins() -> Vec<RecordedPins> {
    with_world(|w| w.pins.clone())
}

/// (engines started, engine shutdowns) plus engine params of the last start.
pub fn engine_events() -> (usize, usize, Option<String>, Option<String>) {
    with_world(|w| {
        (
            w.engines_started,
            w.engine_shutdowns,
            w.user_agent.clone(),
            w.storage_path.clone(),
        )
    })
}

// ---------------------------------------------------------------------------
// Fake objects (one Box per Cronet handle).
// ---------------------------------------------------------------------------

struct FakeEngine;

#[derive(Default)]
struct FakeEngineParams {
    user_agent: Option<CString>,
    storage_path: Option<CString>,
    pins: Vec<RecordedPins>,
}

struct FakePublicKeyPins {
    host: Option<CString>,
    hashes: Vec<CString>,
    include_subdomains: bool,
    expiration_ms: i64,
}

struct FakeHttpHeader {
    name: CString,
    value: CString,
}

struct FakeBuffer {
    data: *mut u8,
    size: usize,
}

struct FakeResponseInfo {
    url: CString,
    status_code: i32,
    status_text: CString,
    headers: Vec<FakeHttpHeader>,
}

struct FakeError {
    code: i32,
    message: CString,
    internal: i32,
}

struct FakeRunnable {
    run: Option<Box<dyn FnOnce()>>,
}

struct FakeExecutor {
    execute: sys::Cronet_Executor_ExecuteFunc,
}

struct FakeCallback {
    on_redirect: sys::Cronet_UrlRequestCallback_OnRedirectReceivedFunc,
    on_response_started: sys::Cronet_UrlRequestCallback_OnResponseStartedFunc,
    on_read_completed: sys::Cronet_UrlRequestCallback_OnReadCompletedFunc,
    on_succeeded: sys::Cronet_UrlRequestCallback_OnSucceededFunc,
    on_failed: sys::Cronet_UrlRequestCallback_OnFailedFunc,
    on_canceled: sys::Cronet_UrlRequestCallback_OnCanceledFunc,
    client_context: *mut c_void,
}

struct FakeUploadProvider {
    get_length: sys::Cronet_UploadDataProvider_GetLengthFunc,
    read: sys::Cronet_UploadDataProvider_ReadFunc,
    rewind: sys::Cronet_UploadDataProvider_RewindFunc,
    close: sys::Cronet_UploadDataProvider_CloseFunc,
    client_context: *mut c_void,
}

struct FakeUploadSink;

struct FakeUrlRequest {
    url: CString,
    method: CString,
    headers: Vec<(CString, CString)>,
    callback: sys::Cronet_UrlRequestCallbackPtr,
    executor: sys::Cronet_ExecutorPtr,
    upload: Option<sys::Cronet_UploadDataProviderPtr>,
    upload_sink: Option<sys::Cronet_UploadDataSinkPtr>,
    response_info: Option<Box<FakeResponseInfo>>,
    body: Vec<u8>,
    body_pos: usize,
    done: bool,
}

thread_local! {
    static CURRENT_UPLOAD_BUFFER: Cell<*mut FakeBuffer> = const { Cell::new(std::ptr::null_mut()) };
}

// ---------------------------------------------------------------------------
// Engine.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Engine_Create() -> sys::Cronet_EnginePtr {
    Box::into_raw(Box::new(FakeEngine)) as sys::Cronet_EnginePtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Engine_Destroy(self_: sys::Cronet_EnginePtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeEngine) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Engine_StartWithParams(
    _self_: sys::Cronet_EnginePtr,
    params: sys::Cronet_EngineParamsPtr,
) -> sys::Cronet_RESULT {
    let p = unsafe { &*(params as *mut FakeEngineParams) };
    with_world(|w| {
        w.engines_started += 1;
        w.user_agent = p
            .user_agent
            .as_ref()
            .map(|c| c.to_string_lossy().into_owned());
        w.storage_path = p
            .storage_path
            .as_ref()
            .map(|c| c.to_string_lossy().into_owned());
        w.pins = p.pins.clone();
    });
    sys::Cronet_RESULT_Cronet_RESULT_SUCCESS
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Engine_Shutdown(
    _self_: sys::Cronet_EnginePtr,
) -> sys::Cronet_RESULT {
    with_world(|w| w.engine_shutdowns += 1);
    sys::Cronet_RESULT_Cronet_RESULT_SUCCESS
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Engine_GetVersionString(
    _self_: sys::Cronet_EnginePtr,
) -> sys::Cronet_String {
    static VERSION: &[u8] = b"cronet-fake/143.0.7445.0\0";
    VERSION.as_ptr() as sys::Cronet_String
}

// ---------------------------------------------------------------------------
// EngineParams.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_Create() -> sys::Cronet_EngineParamsPtr {
    Box::into_raw(Box::new(FakeEngineParams::default())) as sys::Cronet_EngineParamsPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_Destroy(self_: sys::Cronet_EngineParamsPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeEngineParams) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_user_agent_set(
    self_: sys::Cronet_EngineParamsPtr,
    user_agent: sys::Cronet_String,
) {
    let p = unsafe { &mut *(self_ as *mut FakeEngineParams) };
    p.user_agent = unsafe { opt_cstring(user_agent) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_storage_path_set(
    self_: sys::Cronet_EngineParamsPtr,
    storage_path: sys::Cronet_String,
) {
    let p = unsafe { &mut *(self_ as *mut FakeEngineParams) };
    p.storage_path = unsafe { opt_cstring(storage_path) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_public_key_pins_add(
    self_: sys::Cronet_EngineParamsPtr,
    element: sys::Cronet_PublicKeyPinsPtr,
) {
    let p = unsafe { &mut *(self_ as *mut FakeEngineParams) };
    let pin = unsafe { &mut *(element as *mut FakePublicKeyPins) };
    p.pins.push(RecordedPins {
        host: pin
            .host
            .as_ref()
            .map(|c| c.to_string_lossy().into_owned())
            .unwrap_or_default(),
        hashes: pin
            .hashes
            .iter()
            .map(|c| c.to_string_lossy().into_owned())
            .collect(),
        include_subdomains: pin.include_subdomains,
        expiration_ms: pin.expiration_ms,
    });
}

// Setters the wrapper calls but tests don't assert on.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_enable_http2_set(
    _self_: sys::Cronet_EngineParamsPtr,
    _enable_http2: bool,
) {
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_enable_quic_set(
    _self_: sys::Cronet_EngineParamsPtr,
    _enable_quic: bool,
) {
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_enable_brotli_set(
    _self_: sys::Cronet_EngineParamsPtr,
    _enable_brotli: bool,
) {
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_http_cache_mode_set(
    _self_: sys::Cronet_EngineParamsPtr,
    _mode: sys::Cronet_EngineParams_HTTP_CACHE_MODE,
) {
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_http_cache_max_size_set(
    _self_: sys::Cronet_EngineParamsPtr,
    _max: i64,
) {
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_EngineParams_enable_public_key_pinning_bypass_for_local_trust_anchors_set(
    _self_: sys::Cronet_EngineParamsPtr,
    _bypass: bool,
) {
}

// ---------------------------------------------------------------------------
// PublicKeyPins.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_PublicKeyPins_Create() -> sys::Cronet_PublicKeyPinsPtr {
    Box::into_raw(Box::new(FakePublicKeyPins {
        host: None,
        hashes: Vec::new(),
        include_subdomains: false,
        expiration_ms: 0,
    })) as sys::Cronet_PublicKeyPinsPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_PublicKeyPins_Destroy(self_: sys::Cronet_PublicKeyPinsPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakePublicKeyPins) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_PublicKeyPins_host_set(
    self_: sys::Cronet_PublicKeyPinsPtr,
    host: sys::Cronet_String,
) {
    let pin = unsafe { &mut *(self_ as *mut FakePublicKeyPins) };
    pin.host = unsafe { opt_cstring(host) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_PublicKeyPins_pins_sha256_add(
    self_: sys::Cronet_PublicKeyPinsPtr,
    element: sys::Cronet_String,
) {
    let pin = unsafe { &mut *(self_ as *mut FakePublicKeyPins) };
    if !element.is_null() {
        pin.hashes
            .push(unsafe { CStr::from_ptr(element) }.to_owned());
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_PublicKeyPins_include_subdomains_set(
    self_: sys::Cronet_PublicKeyPinsPtr,
    include_subdomains: bool,
) {
    unsafe { (*(self_ as *mut FakePublicKeyPins)).include_subdomains = include_subdomains };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_PublicKeyPins_expiration_date_set(
    self_: sys::Cronet_PublicKeyPinsPtr,
    expiration_date: i64,
) {
    unsafe { (*(self_ as *mut FakePublicKeyPins)).expiration_ms = expiration_date };
}

// ---------------------------------------------------------------------------
// HttpHeader.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_HttpHeader_Create() -> sys::Cronet_HttpHeaderPtr {
    Box::into_raw(Box::new(FakeHttpHeader {
        name: CString::new("").unwrap(),
        value: CString::new("").unwrap(),
    })) as sys::Cronet_HttpHeaderPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_HttpHeader_Destroy(self_: sys::Cronet_HttpHeaderPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeHttpHeader) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_HttpHeader_name_set(
    self_: sys::Cronet_HttpHeaderPtr,
    name: sys::Cronet_String,
) {
    unsafe { (*(self_ as *mut FakeHttpHeader)).name = CStr::from_ptr(name).to_owned() };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_HttpHeader_value_set(
    self_: sys::Cronet_HttpHeaderPtr,
    value: sys::Cronet_String,
) {
    unsafe { (*(self_ as *mut FakeHttpHeader)).value = CStr::from_ptr(value).to_owned() };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_HttpHeader_name_get(
    self_: sys::Cronet_HttpHeaderPtr,
) -> sys::Cronet_String {
    unsafe { (*(self_ as *mut FakeHttpHeader)).name.as_ptr() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_HttpHeader_value_get(
    self_: sys::Cronet_HttpHeaderPtr,
) -> sys::Cronet_String {
    unsafe { (*(self_ as *mut FakeHttpHeader)).value.as_ptr() }
}

// ---------------------------------------------------------------------------
// UrlRequestParams.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestParams_Create() -> sys::Cronet_UrlRequestParamsPtr {
    Box::into_raw(Box::new(FakeRequestParams::default())) as sys::Cronet_UrlRequestParamsPtr
}

#[derive(Default)]
struct FakeRequestParams {
    method: Option<CString>,
    headers: Vec<(CString, CString)>,
    upload: Option<sys::Cronet_UploadDataProviderPtr>,
    upload_executor: Option<sys::Cronet_ExecutorPtr>,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestParams_Destroy(self_: sys::Cronet_UrlRequestParamsPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeRequestParams) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestParams_http_method_set(
    self_: sys::Cronet_UrlRequestParamsPtr,
    method: sys::Cronet_String,
) {
    let p = unsafe { &mut *(self_ as *mut FakeRequestParams) };
    p.method = unsafe { opt_cstring(method) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestParams_request_headers_add(
    self_: sys::Cronet_UrlRequestParamsPtr,
    element: sys::Cronet_HttpHeaderPtr,
) {
    let p = unsafe { &mut *(self_ as *mut FakeRequestParams) };
    let h = unsafe { &*(element as *mut FakeHttpHeader) };
    p.headers.push((h.name.clone(), h.value.clone()));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestParams_upload_data_provider_set(
    self_: sys::Cronet_UrlRequestParamsPtr,
    provider: sys::Cronet_UploadDataProviderPtr,
) {
    unsafe { (*(self_ as *mut FakeRequestParams)).upload = Some(provider) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestParams_upload_data_provider_executor_set(
    self_: sys::Cronet_UrlRequestParamsPtr,
    executor: sys::Cronet_ExecutorPtr,
) {
    unsafe { (*(self_ as *mut FakeRequestParams)).upload_executor = Some(executor) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestParams_allow_direct_executor_set(
    _self_: sys::Cronet_UrlRequestParamsPtr,
    _allow: bool,
) {
}

// ---------------------------------------------------------------------------
// UrlRequestCallback.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestCallback_CreateWith(
    on_redirect: sys::Cronet_UrlRequestCallback_OnRedirectReceivedFunc,
    on_response_started: sys::Cronet_UrlRequestCallback_OnResponseStartedFunc,
    on_read_completed: sys::Cronet_UrlRequestCallback_OnReadCompletedFunc,
    on_succeeded: sys::Cronet_UrlRequestCallback_OnSucceededFunc,
    on_failed: sys::Cronet_UrlRequestCallback_OnFailedFunc,
    on_canceled: sys::Cronet_UrlRequestCallback_OnCanceledFunc,
) -> sys::Cronet_UrlRequestCallbackPtr {
    Box::into_raw(Box::new(FakeCallback {
        on_redirect,
        on_response_started,
        on_read_completed,
        on_succeeded,
        on_failed,
        on_canceled,
        client_context: std::ptr::null_mut(),
    })) as sys::Cronet_UrlRequestCallbackPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestCallback_Destroy(
    self_: sys::Cronet_UrlRequestCallbackPtr,
) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeCallback) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestCallback_SetClientContext(
    self_: sys::Cronet_UrlRequestCallbackPtr,
    client_context: sys::Cronet_ClientContext,
) {
    unsafe { (*(self_ as *mut FakeCallback)).client_context = client_context };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequestCallback_GetClientContext(
    self_: sys::Cronet_UrlRequestCallbackPtr,
) -> sys::Cronet_ClientContext {
    unsafe { (*(self_ as *mut FakeCallback)).client_context }
}

// ---------------------------------------------------------------------------
// Executor and Runnable.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Executor_CreateWith(
    execute: sys::Cronet_Executor_ExecuteFunc,
) -> sys::Cronet_ExecutorPtr {
    Box::into_raw(Box::new(FakeExecutor { execute })) as sys::Cronet_ExecutorPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Executor_Destroy(self_: sys::Cronet_ExecutorPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeExecutor) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Executor_Execute(
    self_: sys::Cronet_ExecutorPtr,
    command: sys::Cronet_RunnablePtr,
) {
    let executor = unsafe { &*(self_ as *mut FakeExecutor) };
    (executor.execute.unwrap())(self_, command);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Runnable_Run(self_: sys::Cronet_RunnablePtr) {
    let runnable = unsafe { &mut *(self_ as *mut FakeRunnable) };
    if let Some(run) = runnable.run.take() {
        run();
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Runnable_Destroy(self_: sys::Cronet_RunnablePtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeRunnable) });
}

// ---------------------------------------------------------------------------
// Buffer.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Buffer_Create() -> sys::Cronet_BufferPtr {
    Box::into_raw(Box::new(FakeBuffer {
        data: std::ptr::null_mut(),
        size: 0,
    })) as sys::Cronet_BufferPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Buffer_Destroy(self_: sys::Cronet_BufferPtr) {
    let buffer = unsafe { Box::from_raw(self_ as *mut FakeBuffer) };
    if !buffer.data.is_null() && buffer.size > 0 {
        unsafe {
            std::alloc::dealloc(
                buffer.data,
                std::alloc::Layout::from_size_align(buffer.size, 1).unwrap(),
            )
        };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Buffer_InitWithAlloc(self_: sys::Cronet_BufferPtr, size: u64) {
    let buffer = unsafe { &mut *(self_ as *mut FakeBuffer) };
    let size = size as usize;
    buffer.data =
        unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align(size, 1).unwrap()) };
    buffer.size = size;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Buffer_GetSize(self_: sys::Cronet_BufferPtr) -> u64 {
    unsafe { (*(self_ as *mut FakeBuffer)).size as u64 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Buffer_GetData(
    self_: sys::Cronet_BufferPtr,
) -> sys::Cronet_RawDataPtr {
    unsafe { (*(self_ as *mut FakeBuffer)).data as sys::Cronet_RawDataPtr }
}

// ---------------------------------------------------------------------------
// UrlResponseInfo and Error.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlResponseInfo_Create() -> sys::Cronet_UrlResponseInfoPtr {
    Box::into_raw(Box::new(FakeResponseInfo {
        url: CString::new("").unwrap(),
        status_code: 0,
        status_text: CString::new("").unwrap(),
        headers: Vec::new(),
    })) as sys::Cronet_UrlResponseInfoPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlResponseInfo_Destroy(self_: sys::Cronet_UrlResponseInfoPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeResponseInfo) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlResponseInfo_url_get(
    self_: sys::Cronet_UrlResponseInfoPtr,
) -> sys::Cronet_String {
    unsafe { (*(self_ as *mut FakeResponseInfo)).url.as_ptr() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlResponseInfo_http_status_code_get(
    self_: sys::Cronet_UrlResponseInfoPtr,
) -> i32 {
    unsafe { (*(self_ as *mut FakeResponseInfo)).status_code }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlResponseInfo_http_status_text_get(
    self_: sys::Cronet_UrlResponseInfoPtr,
) -> sys::Cronet_String {
    unsafe { (*(self_ as *mut FakeResponseInfo)).status_text.as_ptr() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlResponseInfo_all_headers_list_size(
    self_: sys::Cronet_UrlResponseInfoPtr,
) -> u32 {
    unsafe { (*(self_ as *mut FakeResponseInfo)).headers.len() as u32 }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlResponseInfo_all_headers_list_at(
    self_: sys::Cronet_UrlResponseInfoPtr,
    index: u32,
) -> sys::Cronet_HttpHeaderPtr {
    let info = unsafe { &*(self_ as *mut FakeResponseInfo) };
    &info.headers[index as usize] as *const FakeHttpHeader as sys::Cronet_HttpHeaderPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_Create() -> sys::Cronet_ErrorPtr {
    Box::into_raw(Box::new(FakeError {
        code: 0,
        message: CString::new("").unwrap(),
        internal: 0,
    })) as sys::Cronet_ErrorPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_Destroy(self_: sys::Cronet_ErrorPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeError) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_error_code_get(
    self_: sys::Cronet_ErrorPtr,
) -> sys::Cronet_Error_ERROR_CODE {
    unsafe { (*(self_ as *mut FakeError)).code as sys::Cronet_Error_ERROR_CODE }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_message_get(
    self_: sys::Cronet_ErrorPtr,
) -> sys::Cronet_String {
    unsafe { (*(self_ as *mut FakeError)).message.as_ptr() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_internal_error_code_get(self_: sys::Cronet_ErrorPtr) -> i32 {
    unsafe { (*(self_ as *mut FakeError)).internal }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_error_code_set(
    self_: sys::Cronet_ErrorPtr,
    code: sys::Cronet_Error_ERROR_CODE,
) {
    unsafe { (*(self_ as *mut FakeError)).code = code as i32 };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_message_set(
    self_: sys::Cronet_ErrorPtr,
    message: sys::Cronet_String,
) {
    unsafe { (*(self_ as *mut FakeError)).message = CStr::from_ptr(message).to_owned() };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_Error_internal_error_code_set(
    self_: sys::Cronet_ErrorPtr,
    internal: i32,
) {
    unsafe { (*(self_ as *mut FakeError)).internal = internal };
}

// ---------------------------------------------------------------------------
// Upload provider and sink.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataProvider_CreateWith(
    get_length: sys::Cronet_UploadDataProvider_GetLengthFunc,
    read: sys::Cronet_UploadDataProvider_ReadFunc,
    rewind: sys::Cronet_UploadDataProvider_RewindFunc,
    close: sys::Cronet_UploadDataProvider_CloseFunc,
) -> sys::Cronet_UploadDataProviderPtr {
    Box::into_raw(Box::new(FakeUploadProvider {
        get_length,
        read,
        rewind,
        close,
        client_context: std::ptr::null_mut(),
    })) as sys::Cronet_UploadDataProviderPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataProvider_Destroy(
    self_: sys::Cronet_UploadDataProviderPtr,
) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeUploadProvider) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataProvider_SetClientContext(
    self_: sys::Cronet_UploadDataProviderPtr,
    client_context: sys::Cronet_ClientContext,
) {
    unsafe { (*(self_ as *mut FakeUploadProvider)).client_context = client_context };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataProvider_GetClientContext(
    self_: sys::Cronet_UploadDataProviderPtr,
) -> sys::Cronet_ClientContext {
    unsafe { (*(self_ as *mut FakeUploadProvider)).client_context }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataSink_OnReadSucceeded(
    _self_: sys::Cronet_UploadDataSinkPtr,
    bytes_read: u64,
    _final_chunk: bool,
) {
    let buffer = CURRENT_UPLOAD_BUFFER.with(|c| c.get());
    if !buffer.is_null() {
        let data = unsafe { std::slice::from_raw_parts((*buffer).data, bytes_read as usize) };
        with_world(|w| w.upload_capture.extend_from_slice(data));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataSink_OnReadError(
    _self_: sys::Cronet_UploadDataSinkPtr,
    _error_message: sys::Cronet_String,
) {
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataSink_OnRewindSucceeded(
    _self_: sys::Cronet_UploadDataSinkPtr,
) {
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UploadDataSink_OnRewindError(
    _self_: sys::Cronet_UploadDataSinkPtr,
    _error_message: sys::Cronet_String,
) {
}

// ---------------------------------------------------------------------------
// UrlRequest: the state machine the tests exercise.
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequest_Create() -> sys::Cronet_UrlRequestPtr {
    Box::into_raw(Box::new(FakeUrlRequest {
        url: CString::new("").unwrap(),
        method: CString::new("GET").unwrap(),
        headers: Vec::new(),
        callback: std::ptr::null_mut(),
        executor: std::ptr::null_mut(),
        upload: None,
        upload_sink: None,
        response_info: None,
        body: Vec::new(),
        body_pos: 0,
        done: false,
    })) as sys::Cronet_UrlRequestPtr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequest_Destroy(self_: sys::Cronet_UrlRequestPtr) {
    drop(unsafe { Box::from_raw(self_ as *mut FakeUrlRequest) });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequest_InitWithParams(
    self_: sys::Cronet_UrlRequestPtr,
    _engine: sys::Cronet_EnginePtr,
    url: sys::Cronet_String,
    params: sys::Cronet_UrlRequestParamsPtr,
    callback: sys::Cronet_UrlRequestCallbackPtr,
    executor: sys::Cronet_ExecutorPtr,
) -> sys::Cronet_RESULT {
    let req = unsafe { &mut *(self_ as *mut FakeUrlRequest) };
    // Borrow, don't take: real Cronet copies from params and the app
    // destroys them afterwards (which our wrapper does).
    let p = unsafe { &*(params as *mut FakeRequestParams) };
    req.url = unsafe { CStr::from_ptr(url) }.to_owned();
    req.method = p
        .method
        .clone()
        .unwrap_or_else(|| CString::new("GET").unwrap());
    req.headers = p.headers.clone();
    req.upload = p.upload;
    req.callback = callback;
    req.executor = executor;
    sys::Cronet_RESULT_Cronet_RESULT_SUCCESS
}

/// Hands a closure to the app's executor as a runnable (which runs inline
/// with the pass-through executor, i.e. on this thread).
fn deliver<F: FnOnce() + 'static>(executor: sys::Cronet_ExecutorPtr, f: F) {
    let runnable = Box::into_raw(Box::new(FakeRunnable {
        run: Some(Box::new(f)),
    })) as sys::Cronet_RunnablePtr;
    unsafe { Cronet_Executor_Execute(executor, runnable) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequest_Start(
    self_: sys::Cronet_UrlRequestPtr,
) -> sys::Cronet_RESULT {
    let req = unsafe { &mut *(self_ as *mut FakeUrlRequest) };
    let (callback, executor) = (req.callback, req.executor);

    // Drive the buffered upload provider first, so its bytes are recorded.
    if let Some(provider) = req.upload {
        drive_upload(provider);
    }

    let recorded = RecordedRequest {
        url: req.url.to_string_lossy().into_owned(),
        method: req.method.to_string_lossy().into_owned(),
        headers: req
            .headers
            .iter()
            .map(|(n, v)| {
                (
                    n.to_string_lossy().into_owned(),
                    v.to_string_lossy().into_owned(),
                )
            })
            .collect(),
        upload: with_world(|w| (!w.upload_capture.is_empty()).then(|| w.upload_capture.clone())),
    };
    with_world(|w| w.last_request = Some(recorded));

    let (fail, redirect_to) = with_world(|w| (w.config.fail.clone(), w.config.redirect_to.clone()));

    if let Some((code, message)) = fail {
        let error = Box::into_raw(Box::new(FakeError {
            code,
            message: CString::new(message).unwrap(),
            internal: 0,
        })) as sys::Cronet_ErrorPtr;
        req.done = true;
        deliver(executor, move || {
            let cb = unsafe { &*(callback as *mut FakeCallback) };
            (cb.on_failed.unwrap())(callback, self_, std::ptr::null_mut(), error);
            // The handler may destroy the request and the error object it
            // owns; free the error only if it survived.
            drop(unsafe { Box::from_raw(error) });
        });
        return sys::Cronet_RESULT_Cronet_RESULT_SUCCESS;
    }

    if let Some(location) = redirect_to {
        let info = Box::into_raw(Box::new(make_response_info(
            req.url.to_string_lossy().into_owned(),
            302,
            "Found",
            vec![("location".to_string(), location)],
        ))) as sys::Cronet_UrlResponseInfoPtr;
        let new_url = CString::new(with_world(|w| {
            w.config.redirect_to.clone().unwrap_or_default()
        }))
        .unwrap();
        deliver(executor, move || {
            let cb = unsafe { &*(callback as *mut FakeCallback) };
            (cb.on_redirect.unwrap())(callback, self_, info, new_url.as_ptr());
            drop(unsafe { Box::from_raw(info) });
        });
        // The wrapper follows redirects synchronously from inside the
        // callback, which delivers the response; nothing else to do.
        return sys::Cronet_RESULT_Cronet_RESULT_SUCCESS;
    }

    deliver_response(self_);
    sys::Cronet_RESULT_Cronet_RESULT_SUCCESS
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequest_Read(
    self_: sys::Cronet_UrlRequestPtr,
    buffer: sys::Cronet_BufferPtr,
) -> sys::Cronet_RESULT {
    let req = unsafe { &mut *(self_ as *mut FakeUrlRequest) };
    let (callback, executor) = (req.callback, req.executor);
    let chunk_size = with_world(|w| w.config.chunk_size);

    let buffer_obj = unsafe { &mut *(buffer as *mut FakeBuffer) };
    let remaining = &req.body[req.body_pos..];
    let n = remaining.len().min(chunk_size).min(buffer_obj.size);

    if n > 0 {
        unsafe { std::ptr::copy_nonoverlapping(remaining.as_ptr(), buffer_obj.data, n) };
        req.body_pos += n;
        let info = req.response_info.as_ref().unwrap().as_ref() as *const FakeResponseInfo
            as sys::Cronet_UrlResponseInfoPtr;
        deliver(executor, move || {
            let cb = unsafe { &*(callback as *mut FakeCallback) };
            (cb.on_read_completed.unwrap())(callback, self_, info, buffer, n as u64);
        });
    } else {
        req.done = true;
        let info = req.response_info.as_ref().unwrap().as_ref() as *const FakeResponseInfo
            as sys::Cronet_UrlResponseInfoPtr;
        deliver(executor, move || {
            let cb = unsafe { &*(callback as *mut FakeCallback) };
            (cb.on_succeeded.unwrap())(callback, self_, info);
        });
    }
    sys::Cronet_RESULT_Cronet_RESULT_SUCCESS
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequest_FollowRedirect(
    self_: sys::Cronet_UrlRequestPtr,
) -> sys::Cronet_RESULT {
    deliver_response(self_);
    sys::Cronet_RESULT_Cronet_RESULT_SUCCESS
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn Cronet_UrlRequest_Cancel(self_: sys::Cronet_UrlRequestPtr) {
    let req = unsafe { &mut *(self_ as *mut FakeUrlRequest) };
    if req.done {
        return;
    }
    let (callback, executor) = (req.callback, req.executor);
    req.done = true;
    let info = req
        .response_info
        .as_ref()
        .map(|b| b.as_ref() as *const FakeResponseInfo as sys::Cronet_UrlResponseInfoPtr)
        .unwrap_or(std::ptr::null_mut());
    deliver(executor, move || {
        let cb = unsafe { &*(callback as *mut FakeCallback) };
        (cb.on_canceled.unwrap())(callback, self_, info);
    });
}

fn deliver_response(request: sys::Cronet_UrlRequestPtr) {
    let req = unsafe { &mut *(request as *mut FakeUrlRequest) };
    let (callback, executor) = (req.callback, req.executor);
    let config = with_world(|w| w.config.clone());
    let info = Box::new(make_response_info(
        req.url.to_string_lossy().into_owned(),
        config.status as i32,
        &config.status_text,
        config.response_headers,
    ));
    let info_ptr = info.as_ref() as *const FakeResponseInfo as sys::Cronet_UrlResponseInfoPtr;
    req.response_info = Some(info);
    req.body = config.body;
    req.body_pos = 0;
    deliver(executor, move || {
        let cb = unsafe { &*(callback as *mut FakeCallback) };
        unsafe { (cb.on_response_started.unwrap())(callback, request, info_ptr) };
    });
}

fn make_response_info(
    url: String,
    status: i32,
    status_text: &str,
    headers: Vec<(String, String)>,
) -> FakeResponseInfo {
    FakeResponseInfo {
        url: CString::new(url).unwrap(),
        status_code: status,
        status_text: CString::new(status_text).unwrap(),
        headers: headers
            .into_iter()
            .map(|(name, value)| FakeHttpHeader {
                name: CString::new(name).unwrap(),
                value: CString::new(value).unwrap(),
            })
            .collect(),
    }
}

/// Drives the buffered upload provider: length, one read, close.
fn drive_upload(provider: sys::Cronet_UploadDataProviderPtr) {
    unsafe {
        let sink = Box::into_raw(Box::new(FakeUploadSink)) as sys::Cronet_UploadDataSinkPtr;
        let p = &*(provider as *mut FakeUploadProvider);
        let len = (p.get_length.unwrap())(provider);
        if len > 0 {
            let buffer = Cronet_Buffer_Create();
            Cronet_Buffer_InitWithAlloc(buffer, len as u64);
            CURRENT_UPLOAD_BUFFER.with(|c| c.set(buffer as *mut FakeBuffer));
            (p.read.unwrap())(provider, sink, buffer);
            CURRENT_UPLOAD_BUFFER.with(|c| c.set(std::ptr::null_mut()));
            Cronet_Buffer_Destroy(buffer);
        }
        (p.close.unwrap())(provider);
        Cronet_UploadDataProvider_Destroy(provider);
        drop(Box::from_raw(sink));
    }
}

unsafe fn opt_cstring(s: sys::Cronet_String) -> Option<CString> {
    (!s.is_null()).then(|| unsafe { CStr::from_ptr(s) }.to_owned())
}

// Keep c_char referenced for signature parity documentation.
#[allow(dead_code)]
type _CronetString = *const c_char;
