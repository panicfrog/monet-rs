//! The response type transports produce and hooks mutate.

use crate::body::Body;
use crate::extensions::Extensions;
use crate::headers::HeaderMap;

/// An HTTP response: status, headers, and a streaming body.
///
/// Transports must deliver the response head (status + headers) as soon as it
/// arrives, with [`Body`] chunks following through the channel — never buffer
/// the whole body first. This mirrors the native transports' delegate
/// behavior and nitro-fetch's `onResponseStarted` contract.
#[derive(Debug)]
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Response headers.
    pub headers: HeaderMap,
    /// Streaming response body.
    pub body: Body,
    /// Typed per-response state for hooks.
    pub extensions: Extensions,
}

impl Response {
    /// Assembles a response from parts.
    pub fn new(status: u16, headers: HeaderMap, body: Body) -> Self {
        Response {
            status,
            headers,
            body,
            extensions: Extensions::new(),
        }
    }

    /// Returns `true` for 2xx statuses.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_ranges() {
        assert!(Response::new(200, HeaderMap::new(), Body::empty()).is_success());
        assert!(Response::new(299, HeaderMap::new(), Body::empty()).is_success());
        assert!(!Response::new(300, HeaderMap::new(), Body::empty()).is_success());
        assert!(!Response::new(404, HeaderMap::new(), Body::empty()).is_success());
    }
}
