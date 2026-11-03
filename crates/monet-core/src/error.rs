//! Error types shared across the monet-rs core.

/// A platform-specific error code, passed through from a native transport
/// (e.g. a Cronet net error or a URLError code) so frontends can branch on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformErrorCode {
    /// Transport identifier, e.g. `"cronet"`, `"urlsession"`, `"rustls"`.
    pub platform: &'static str,
    /// The native error code.
    pub code: i64,
}

impl PlatformErrorCode {
    /// Builds a code for a given platform.
    pub const fn new(platform: &'static str, code: i64) -> Self {
        Self { platform, code }
    }
}

/// Every failure surfaced by the monet-rs core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The request was cancelled through its [`crate::CancelToken`].
    #[error("request cancelled")]
    Cancelled,
    /// A transport failed to execute the request. `code` carries the native
    /// error code when the transport reports one.
    #[error("transport error: {message}")]
    Transport {
        /// Human-readable description from the transport.
        message: String,
        /// Native error code, when available.
        code: Option<PlatformErrorCode>,
    },
    /// A hook in the chain failed; execution stopped at that hook.
    #[error("hook `{hook}` failed: {source}")]
    Hook {
        /// Name of the failing hook, from its `name()` implementation.
        hook: String,
        /// The error the hook returned.
        source: Box<Error>,
    },
    /// A header name is not a valid HTTP token.
    #[error("invalid header name: {0:?}")]
    InvalidHeaderName(String),
    /// A header value contains forbidden bytes (NUL, CR, LF).
    #[error("invalid header value: {0:?}")]
    InvalidHeaderValue(String),
    /// The body channel is broken (e.g. the receiving side was dropped).
    #[error("body error: {0}")]
    Body(String),
}

impl Error {
    /// Builds a [`Error::Transport`] without a platform code.
    pub fn transport(message: impl Into<String>) -> Self {
        Error::Transport {
            message: message.into(),
            code: None,
        }
    }

    /// Builds a [`Error::Transport`] with a platform error code attached.
    pub fn transport_with_code(message: impl Into<String>, code: PlatformErrorCode) -> Self {
        Error::Transport {
            message: message.into(),
            code: Some(code),
        }
    }
}

/// Convenience alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;
