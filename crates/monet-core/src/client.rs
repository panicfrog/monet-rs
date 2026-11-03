//! The client orchestrator: request hooks → transport → response hooks.
//!
//! This is the single entry point every frontend (monet-rn, monet-dart) and
//! every transport conformance test drives. It owns nothing platform-specific
//! — hand it any [`Transport`] and a [`HookRegistry`], and it runs the
//! pipeline.

use std::sync::Arc;

use crate::cancel::CancelToken;
use crate::error::{Error, Result};
use crate::hook::HookRegistry;
use crate::request::Request;
use crate::response::Response;
use crate::transport::Transport;

/// Drives the request pipeline: request hooks run first (in registration
/// order), then the transport, then response hooks once the response head
/// arrives. The body keeps streaming while response hooks have already
/// finished — they see the head, not the bytes.
///
/// ```
/// use monet_core::{CancelToken, Client, HeaderMap, MockTransport, Request, Transport};
/// use monet_core::method::Method;
///
/// # fn main() -> monet_core::Result<()> {
/// let client = Client::new(std::sync::Arc::new(MockTransport::with_response(
///     200,
///     HeaderMap::new(),
///     vec![bytes::Bytes::from("hello")],
/// )));
/// let url = url::Url::parse("https://example.com").unwrap();
/// let resp = futures_lite::future::block_on(
///     client.execute(Request::new(Method::Get, url)),
/// )?;
/// assert!(resp.is_success());
/// # Ok(())
/// # }
/// ```
pub struct Client {
    transport: Arc<dyn Transport>,
    hooks: HookRegistry,
}

impl Client {
    /// Creates a client over `transport` with an empty hook registry.
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Client {
            transport,
            hooks: HookRegistry::new(),
        }
    }

    /// The hook registry, for registering hooks.
    pub fn hooks(&mut self) -> &mut HookRegistry {
        &mut self.hooks
    }

    /// Executes the pipeline without external cancellation.
    pub async fn execute(&self, req: Request) -> Result<Response> {
        self.execute_with_cancel(req, CancelToken::new()).await
    }

    /// Executes the pipeline with a cancellation token shared with the
    /// caller (e.g. a JS `AbortController` adapted by monet-rn).
    ///
    /// - An already-cancelled token fails fast with [`Error::Cancelled`]
    ///   before any hook or transport runs.
    /// - After the response head arrives, cancellation is the transport's
    ///   concern: it should stop the body stream (aborting the body channel)
    ///   while this method has already returned.
    /// - Hook failures abort the pipeline at the failing hook; the transport
    ///   is never called for a request whose hooks failed.
    pub async fn execute_with_cancel(&self, req: Request, cancel: CancelToken) -> Result<Response> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let req = self.hooks.run_request_hooks(req).await?;
        let res = self.transport.execute(req, cancel).await?;
        self.hooks.run_response_hooks(res).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::headers::HeaderMap;
    use crate::method::Method;
    use crate::mock::MockTransport;
    use bytes::Bytes;
    use futures_lite::StreamExt;
    use url::Url;

    fn url() -> Url {
        Url::parse("https://example.com/x").unwrap()
    }

    fn mock() -> Arc<MockTransport> {
        Arc::new(MockTransport::with_response(
            200,
            HeaderMap::new(),
            vec![Bytes::from("a"), Bytes::from("b")],
        ))
    }

    /// Rewrites a request header and the response status.
    struct RewriteHook;

    impl crate::hook::RequestHook for RewriteHook {
        fn name(&self) -> &str {
            "rewrite"
        }
        fn on_request(&self, mut req: Request) -> crate::transport::BoxFuture<Result<Request>> {
            Box::pin(async move {
                req.headers.set("x-rewritten", "yes")?;
                Ok(req)
            })
        }
    }

    impl crate::hook::ResponseHook for RewriteHook {
        fn name(&self) -> &str {
            "rewrite"
        }
        fn on_response(&self, mut res: Response) -> crate::transport::BoxFuture<Result<Response>> {
            Box::pin(async move {
                res.headers.set("x-seen-by", "rewrite")?;
                Ok(res)
            })
        }
    }

    #[test]
    fn pipeline_runs_hooks_then_transport_then_hooks() {
        let transport = mock();
        let mut client = Client::new(transport.clone());
        client.hooks().add_request_hook(RewriteHook);
        client.hooks().add_response_hook(RewriteHook);

        let resp = futures_lite::future::block_on(client.execute(Request::new(Method::Get, url())))
            .unwrap();
        assert!(resp.is_success());
        assert_eq!(resp.headers.get("x-seen-by").unwrap(), "rewrite");
        // body still streams after response hooks ran
        let body =
            futures_lite::future::block_on(resp.body.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(body, ["a", "b"]);
        // the transport saw the rewritten request
        let recorded = transport.recorded();
        assert_eq!(recorded[0].headers.get("x-rewritten").unwrap(), "yes");
    }

    #[test]
    fn request_hook_failure_skips_transport() {
        struct FailHook;
        impl crate::hook::RequestHook for FailHook {
            fn name(&self) -> &str {
                "fail"
            }
            fn on_request(&self, req: Request) -> crate::transport::BoxFuture<Result<Request>> {
                Box::pin(async {
                    let _ = req;
                    Err(Error::transport("hook says no"))
                })
            }
        }

        let transport = mock();
        let mut client = Client::new(transport.clone());
        client.hooks().add_request_hook(FailHook);

        let err = futures_lite::future::block_on(client.execute(Request::new(Method::Get, url())))
            .unwrap_err();
        assert!(matches!(err, Error::Hook { .. }));
        assert!(transport.recorded().is_empty());
    }

    #[test]
    fn pre_cancelled_token_fails_fast() {
        let transport = mock();
        let client = Client::new(transport.clone());
        let cancel = CancelToken::new();
        cancel.cancel();

        let err = futures_lite::future::block_on(
            client.execute_with_cancel(Request::new(Method::Get, url()), cancel),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Cancelled));
        assert!(transport.recorded().is_empty());
    }

    #[test]
    fn transport_error_propagates_unwrapped() {
        let client = Client::new(Arc::new(MockTransport::with_error("boom")));
        let err = futures_lite::future::block_on(client.execute(Request::new(Method::Get, url())))
            .unwrap_err();
        // transport failures surface as-is, not wrapped in Error::Hook
        assert!(matches!(err, Error::Transport { message, .. } if message == "boom"));
    }
}
