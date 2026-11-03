//! Assembling `Cronet_UrlRequest` from monet-core's `Request`.
//!
//! v1 buffers the whole request body before starting the request (upload
//! streaming through `Cronet_UploadDataProvider` is a follow-up; the C API
//! supports it fully). The buffered provider reports the exact length, fills
//! each read in one go, and refuses rewinds — a `Body` stream cannot be
//! replayed.

use bytes::Bytes;
use std::cell::UnsafeCell;

use monet_core::error::{Error, Result};
use monet_core::request::Request;

use crate::engine::{cstr, map_result};
use monet_cronet_sys as sys;

/// The buffered upload body handed to Cronet through its client context.
struct UploadBody {
    data: Bytes,
    offset: UnsafeCell<usize>,
}

// Safety: Cronet drives a request's upload callbacks sequentially (get
// length, then reads in order), so the unsynchronized offset is only ever
// touched by one callback at a time.
unsafe impl Send for UploadBody {}

unsafe extern "C" fn upload_get_length(self_: sys::Cronet_UploadDataProviderPtr) -> i64 {
    let body = unsafe { upload_context(self_) };
    body.data.len() as i64
}

unsafe extern "C" fn upload_read(
    self_: sys::Cronet_UploadDataProviderPtr,
    sink: sys::Cronet_UploadDataSinkPtr,
    buffer: sys::Cronet_BufferPtr,
) {
    let body = unsafe { upload_context(self_) };
    unsafe {
        let capacity = sys::Cronet_Buffer_GetSize(buffer) as usize;
        let dst = sys::Cronet_Buffer_GetData(buffer) as *mut u8;
        let offset = *body.offset.get();
        let remaining = &body.data[offset..];
        let n = remaining.len().min(capacity);
        if n > 0 {
            std::ptr::copy_nonoverlapping(remaining.as_ptr(), dst, n);
            *body.offset.get() = offset + n;
        }
        let final_chunk = offset + n == body.data.len();
        sys::Cronet_UploadDataSink_OnReadSucceeded(sink, n as u64, final_chunk);
    }
}

unsafe extern "C" fn upload_rewind(
    _self_: sys::Cronet_UploadDataProviderPtr,
    sink: sys::Cronet_UploadDataSinkPtr,
) {
    static MESSAGE: &[u8] = b"monet-cronet: request body cannot be rewound\0";
    unsafe {
        sys::Cronet_UploadDataSink_OnRewindError(sink, MESSAGE.as_ptr() as sys::Cronet_String);
    }
}

unsafe extern "C" fn upload_close(self_: sys::Cronet_UploadDataProviderPtr) {
    unsafe {
        let ctx = sys::Cronet_UploadDataProvider_GetClientContext(self_) as *mut UploadBody;
        drop(Box::from_raw(ctx));
    }
}

/// Borrows the upload body behind a provider's client context. The box
/// outlives every callback (Cronet calls `Close` last, which frees it), so
/// the unbounded lifetime is sound.
unsafe fn upload_context(provider: sys::Cronet_UploadDataProviderPtr) -> &'static UploadBody {
    unsafe { &*(sys::Cronet_UploadDataProvider_GetClientContext(provider) as *const UploadBody) }
}

/// Builds and initializes a `Cronet_UrlRequest` for `req`. The caller owns
/// the returned request (and must destroy it); `callback` and `executor`
/// are borrowed for the request's lifetime.
pub(crate) fn init_url_request(
    engine: sys::Cronet_EnginePtr,
    executor: sys::Cronet_ExecutorPtr,
    req: &Request,
    upload: Option<Bytes>,
    callback: sys::Cronet_UrlRequestCallbackPtr,
) -> Result<sys::Cronet_UrlRequestPtr> {
    unsafe {
        let request = sys::Cronet_UrlRequest_Create();
        if request.is_null() {
            return Err(Error::transport("cronet: request creation failed"));
        }
        let params = sys::Cronet_UrlRequestParams_Create();

        let method = cstr(req.method.as_str().as_bytes())?;
        sys::Cronet_UrlRequestParams_http_method_set(params, method.as_ptr());

        for (name, value) in req.headers.iter_flattened() {
            let header = sys::Cronet_HttpHeader_Create();
            let name_c = cstr(name.as_bytes())?;
            let value_c = cstr(value.as_bytes())?;
            sys::Cronet_HttpHeader_name_set(header, name_c.as_ptr());
            sys::Cronet_HttpHeader_value_set(header, value_c.as_ptr());
            sys::Cronet_UrlRequestParams_request_headers_add(params, header);
            sys::Cronet_HttpHeader_Destroy(header);
        }

        if let Some(data) = upload {
            let body = Box::new(UploadBody {
                data,
                offset: UnsafeCell::new(0),
            });
            let provider = sys::Cronet_UploadDataProvider_CreateWith(
                Some(upload_get_length),
                Some(upload_read),
                Some(upload_rewind),
                Some(upload_close),
            );
            sys::Cronet_UploadDataProvider_SetClientContext(
                provider,
                Box::into_raw(body) as sys::Cronet_ClientContext,
            );
            sys::Cronet_UrlRequestParams_upload_data_provider_set(params, provider);
            sys::Cronet_UrlRequestParams_upload_data_provider_executor_set(params, executor);
            sys::Cronet_UrlRequestParams_allow_direct_executor_set(params, true);
        }

        let url = cstr(req.url.as_str().as_bytes())?;
        let result = sys::Cronet_UrlRequest_InitWithParams(
            request,
            engine,
            url.as_ptr(),
            params,
            callback,
            executor,
        );
        sys::Cronet_UrlRequestParams_Destroy(params);
        if result != sys::Cronet_RESULT_Cronet_RESULT_SUCCESS {
            sys::Cronet_UrlRequest_Destroy(request);
            return Err(map_result(result));
        }
        Ok(request)
    }
}
