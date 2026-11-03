//! A scriptable [`Transport`] for tests and examples.
//!
//! It records every request it executes (method, URL, headers — the body
//! stream is consumed, not kept) and answers from a closure, so hook-chain
//! and core-logic tests need no network.

use bytes::Bytes;
use std::sync::{Arc, Mutex};

use crate::body::Body;
use crate::cancel::CancelToken;
use crate::error::Result;
use crate::headers::HeaderMap;
use crate::request::Request;
use crate::response::Response;
use crate::transport::{BoxFuture, Transport};

/// What a [`MockTransport`] captured when a request ran.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    /// The method.
    pub method: String,
    /// The URL.
    pub url: String,
    /// Header snapshot taken before the handler ran.
    pub headers: HeaderMap,
}

/// The response a mock handler produces.
#[derive(Debug, Clone)]
pub struct MockResponse {
    /// Status code.
    pub status: u16,
    /// Response headers.
    pub headers: HeaderMap,
    /// Body chunks, streamed in order.
    pub chunks: Vec<Bytes>,
}

type Handler = Arc<dyn Fn(Request) -> Result<MockResponse> + Send + Sync>;

/// A [`Transport`] that answers from a closure.
pub struct MockTransport {
    handler: Handler,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl MockTransport {
    /// Creates a transport answering from `handler`.
    pub fn new(handler: impl Fn(Request) -> Result<MockResponse> + Send + Sync + 'static) -> Self {
        MockTransport {
            handler: Arc::new(handler),
            recorded: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Creates a transport that always answers `status` with `chunks`.
    pub fn with_response(status: u16, headers: HeaderMap, chunks: Vec<Bytes>) -> Self {
        MockTransport::new(move |_| {
            Ok(MockResponse {
                status,
                headers: headers.clone(),
                chunks: chunks.clone(),
            })
        })
    }

    /// Creates a transport that always fails with a transport error
    /// carrying `message`.
    pub fn with_error(message: impl Into<String>) -> Self {
        let message = message.into();
        MockTransport::new(move |_| Err(crate::error::Error::transport(message.clone())))
    }

    /// A snapshot of the requests executed so far, in order.
    pub fn recorded(&self) -> Vec<RecordedRequest> {
        self.recorded.lock().unwrap().clone()
    }
}

impl Transport for MockTransport {
    fn execute(&self, req: Request, cancel: CancelToken) -> BoxFuture<Result<Response>> {
        self.recorded.lock().unwrap().push(RecordedRequest {
            method: req.method.to_string(),
            url: req.url.to_string(),
            headers: req.headers.clone(),
        });
        let handler = self.handler.clone();
        Box::pin(async move {
            if cancel.is_cancelled() {
                return Err(crate::error::Error::Cancelled);
            }
            let spec = handler(req)?;
            let body = Body::from_chunks(spec.chunks);
            Ok(Response::new(spec.status, spec.headers, body))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::method::Method;
    use futures_lite::StreamExt;

    fn http_url() -> url::Url {
        url::Url::parse("https://example.com/x").unwrap()
    }

    #[test]
    fn streams_head_then_chunks_and_records() {
        let transport = MockTransport::with_response(
            200,
            HeaderMap::new(),
            vec![Bytes::from("a"), Bytes::from("b")],
        );
        let req = Request::new(Method::Get, http_url())
            .with_header("X-Test", "1")
            .unwrap();
        let resp =
            futures_lite::future::block_on(transport.execute(req, CancelToken::new())).unwrap();
        assert_eq!(resp.status, 200);
        assert!(resp.is_success());
        let body =
            futures_lite::future::block_on(resp.body.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(body, ["a", "b"]);

        let recorded = transport.recorded();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].method, "GET");
        assert_eq!(recorded[0].url, "https://example.com/x");
        assert_eq!(recorded[0].headers.get("x-test").unwrap(), "1");
    }

    #[test]
    fn cancelled_token_yields_cancelled_error() {
        let transport = MockTransport::with_response(200, HeaderMap::new(), vec![]);
        let cancel = CancelToken::new();
        cancel.cancel();
        let err = futures_lite::future::block_on(
            transport.execute(Request::new(Method::Get, http_url()), cancel),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Cancelled));
        // still recorded: the cancellation was ours, not the transport's
        assert_eq!(transport.recorded().len(), 1);
    }

    #[test]
    fn handler_errors_propagate() {
        let transport = MockTransport::with_error("boom");
        let err = futures_lite::future::block_on(
            transport.execute(Request::new(Method::Get, http_url()), CancelToken::new()),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Transport { message, .. } if message == "boom"));
    }
}
