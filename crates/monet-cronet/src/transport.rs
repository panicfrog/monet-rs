//! [`Transport`] implementation over a [`CronetEngine`].

use bytes::Bytes;
use futures_channel::oneshot;
use futures_lite::StreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use monet_core::body::Body;
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::request::Request;
use monet_core::response::Response;
use monet_core::transport::{BoxFuture, Transport};

use crate::callback::RequestState;
use crate::engine::{map_result, release_context};
use crate::request;
use monet_cronet_sys as sys;

/// A `Transport` executing requests on a shared [`crate::CronetEngine`].
pub struct CronetTransport {
    engine: Arc<crate::engine::CronetEngine>,
}

impl CronetTransport {
    /// Wraps a started engine.
    pub fn new(engine: Arc<crate::engine::CronetEngine>) -> Self {
        CronetTransport { engine }
    }
}

impl Transport for CronetTransport {
    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>> {
        let engine = self.engine.clone();
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            // v1: buffer the request body before starting the request.
            let mut req = req;
            let upload = collect_body(std::mem::replace(&mut req.body, Body::empty())).await?;
            let (head_tx, head_rx) = oneshot::channel();
            let mut state = Arc::new(RequestState::new(head_tx));
            let callback = unsafe {
                sys::Cronet_UrlRequestCallback_CreateWith(
                    Some(crate::callback::on_redirect_received),
                    Some(crate::callback::on_response_started),
                    Some(crate::callback::on_read_completed),
                    Some(crate::callback::on_succeeded),
                    Some(crate::callback::on_failed),
                    Some(crate::callback::on_canceled),
                )
            };
            Arc::get_mut(&mut state)
                .expect("state is not shared yet")
                .set_callback(callback);
            // One strong reference lives in the client context; terminal
            // callbacks adopt and drop it (see callback.rs).
            unsafe {
                sys::Cronet_UrlRequestCallback_SetClientContext(
                    callback,
                    Arc::into_raw(state.clone()) as sys::Cronet_ClientContext,
                )
            };

            let request_ptr = match request::init_url_request(
                engine.ptr(),
                engine.executor().ptr(),
                &req,
                upload,
                callback,
            ) {
                Ok(ptr) => ptr,
                Err(e) => {
                    unsafe { release_context::<RequestState>(callback) };
                    return Err(e);
                }
            };
            state.set_request(request_ptr);

            let result = unsafe { sys::Cronet_UrlRequest_Start(request_ptr) };
            if result != sys::Cronet_RESULT_Cronet_RESULT_SUCCESS {
                state.clear_request();
                unsafe { release_context::<RequestState>(callback) };
                return Err(map_result(result));
            }

            ExecuteFuture {
                state,
                cancel,
                rx: head_rx,
            }
            .await
        })
    }
}

/// The future returned by [`CronetTransport::execute`]: resolves when the
/// response head arrives, and keeps the cancel token wired to
/// `Cronet_UrlRequest_Cancel` while it is being polled.
struct ExecuteFuture {
    state: Arc<RequestState>,
    cancel: CancelToken,
    rx: oneshot::Receiver<Result<Response>>,
}

impl Future for ExecuteFuture {
    type Output = Result<Response>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.cancel.is_cancelled() {
            self.state.cancel_request();
        }
        Pin::new(&mut self.rx).poll(cx).map(|r| {
            r.unwrap_or_else(|_| {
                Err(Error::transport(
                    "cronet: request context dropped before the response head",
                ))
            })
        })
    }
}

/// Buffers a body stream into one `Bytes`, or `None` when empty.
async fn collect_body(mut body: Body) -> Result<Option<Bytes>> {
    let mut buf = bytes::BytesMut::new();
    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok((!buf.is_empty()).then(|| buf.freeze()))
}
