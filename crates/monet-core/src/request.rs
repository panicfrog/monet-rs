//! The request type passed through the hook chain into transports.

use bytes::Bytes;
use url::Url;

use crate::body::Body;
use crate::error::Result;
use crate::extensions::Extensions;
use crate::headers::HeaderMap;
use crate::method::Method;

/// An HTTP request: method, URL, headers, streaming body, and per-request
/// extensions for hooks.
#[derive(Debug)]
pub struct Request {
    /// HTTP method.
    pub method: Method,
    /// Target URL.
    pub url: Url,
    /// Request headers.
    pub headers: HeaderMap,
    /// Streaming request body (empty for bodyless requests).
    pub body: Body,
    /// Deadline for the response head, in milliseconds. `None` keeps
    /// the transport's (or client's) default — the per-request override
    /// wins over any configured value.
    pub timeout_ms: Option<u64>,
    /// Whether 3xx responses are followed. `None` keeps the transport's
    /// default (follow); `Some(false)` delivers the redirect response
    /// itself.
    pub follow_redirects: Option<bool>,
    /// Typed per-request state for hooks.
    pub extensions: Extensions,
}

impl Request {
    /// Creates a request with no headers and an empty body.
    pub fn new(method: Method, url: Url) -> Self {
        Request {
            method,
            url,
            headers: HeaderMap::new(),
            body: Body::empty(),
            timeout_ms: None,
            follow_redirects: None,
            extensions: Extensions::new(),
        }
    }

    /// Sets a per-request response-head deadline in milliseconds
    /// (builder style) — overrides any transport-level timeout.
    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = Some(ms);
        self
    }

    /// Sets the per-request redirect policy (builder style): `false`
    /// delivers 3xx responses instead of following them. `true` is the
    /// default everywhere, so only `Some(false)` changes behavior.
    pub fn with_follow_redirects(mut self, follow: bool) -> Self {
        self.follow_redirects = Some(follow);
        self
    }

    /// Replaces the body with a single complete chunk (builder style).
    pub fn with_body_bytes(mut self, bytes: impl AsRef<[u8]>) -> Self {
        self.body = Body::from_bytes(Bytes::copy_from_slice(bytes.as_ref()));
        self
    }

    /// Appends a header (builder style).
    pub fn with_header(mut self, name: &str, value: impl Into<String>) -> Result<Self> {
        self.headers.append(name, value)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::StreamExt as _;

    #[test]
    fn builder_sets_timeout_and_redirect_policy() {
        let req = Request::new(Method::Get, Url::parse("https://example.com").unwrap());
        assert_eq!(req.timeout_ms, None);
        assert_eq!(req.follow_redirects, None);
        let req = req.with_timeout_ms(1500).with_follow_redirects(false);
        assert_eq!(req.timeout_ms, Some(1500));
        assert_eq!(req.follow_redirects, Some(false));
    }

    #[test]
    fn builder_sets_headers_and_body() {
        let req = Request::new(Method::Get, Url::parse("https://example.com").unwrap())
            .with_header("X-A", "1")
            .unwrap()
            .with_header("x-a", "2")
            .unwrap()
            .with_body_bytes("payload");
        assert_eq!(req.headers.get("x-a").unwrap(), "1, 2");
        let body = futures_lite::future::block_on(req.body.map(|c| c.unwrap()).collect::<Vec<_>>());
        assert_eq!(body, ["payload"]);
    }
}
