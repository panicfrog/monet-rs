//! Cronet engine lifecycle and configuration.

use std::ffi::{CStr, CString};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use monet_core::error::{Error, PlatformErrorCode, Result};
use monet_core::pin::PinConfig;

use crate::executor::PassThroughExecutor;
use monet_cronet_sys as sys;

/// HTTP cache configuration.
#[derive(Debug, Clone)]
pub enum HttpCache {
    /// No caching.
    Disabled,
    /// Disk cache rooted at `path` (created if missing), bounded by
    /// `max_size` bytes.
    Disk {
        /// Cache directory; must be app-private storage.
        path: PathBuf,
        /// Maximum cache size in bytes.
        max_size: u64,
    },
}

/// Everything the engine is configured with at start.
#[derive(Clone)]
pub struct EngineConfig {
    /// Enable HTTP/2. Enabled by default.
    pub enable_http2: bool,
    /// Enable QUIC/HTTP3. Enabled by default.
    pub enable_quic: bool,
    /// Enable Brotli content encoding. Enabled by default.
    pub enable_brotli: bool,
    /// HTTP cache. Disabled by default.
    pub http_cache: HttpCache,
    /// User agent override.
    pub user_agent: Option<String>,
    /// Public-key pins. Cronet's built-in BoringSSL ignores Android's
    /// `network_security_config.xml`, so pins must be injected through the
    /// engine; parsing and host matching live in monet-core (one source of
    /// truth shared with the rustls transports).
    pub pins: PinConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            enable_http2: true,
            enable_quic: true,
            enable_brotli: true,
            http_cache: HttpCache::Disabled,
            user_agent: None,
            pins: PinConfig::default(),
        }
    }
}

/// A started Cronet engine. Cheap to share through `Arc` and use from any
/// thread; Cronet's engine is thread-safe.
pub struct CronetEngine {
    ptr: sys::Cronet_EnginePtr,
    executor: Arc<PassThroughExecutor>,
    shutdown_done: AtomicBool,
}

impl CronetEngine {
    /// Creates and starts an engine with `config`.
    pub fn start(config: EngineConfig) -> Result<Self> {
        unsafe {
            let engine = sys::Cronet_Engine_Create();
            if engine.is_null() {
                return Err(Error::transport("cronet: engine creation failed"));
            }
            let params = sys::Cronet_EngineParams_Create();

            sys::Cronet_EngineParams_enable_http2_set(params, config.enable_http2);
            sys::Cronet_EngineParams_enable_quic_set(params, config.enable_quic);
            sys::Cronet_EngineParams_enable_brotli_set(params, config.enable_brotli);

            match &config.http_cache {
                HttpCache::Disabled => sys::Cronet_EngineParams_http_cache_mode_set(
                    params,
                    sys::Cronet_EngineParams_HTTP_CACHE_MODE_Cronet_EngineParams_HTTP_CACHE_MODE_DISABLED,
                ),
                HttpCache::Disk { path, max_size } => {
                    std::fs::create_dir_all(path).map_err(|e| {
                        Error::transport(format!(
                            "cronet: cannot create cache directory {}: {e}",
                            path.display()
                        ))
                    })?;
                    let dir = cstr(path.to_string_lossy().as_bytes())?;
                    sys::Cronet_EngineParams_storage_path_set(params, dir.as_ptr());
                    sys::Cronet_EngineParams_http_cache_max_size_set(
                        params,
                        i64::try_from(*max_size)
                            .map_err(|_| Error::transport("cronet: cache size too large"))?,
                    );
                    sys::Cronet_EngineParams_http_cache_mode_set(
                        params,
                        sys::Cronet_EngineParams_HTTP_CACHE_MODE_Cronet_EngineParams_HTTP_CACHE_MODE_DISK,
                    );
                }
            }

            if let Some(user_agent) = &config.user_agent {
                let ua = cstr(user_agent.as_bytes())?;
                sys::Cronet_EngineParams_user_agent_set(params, ua.as_ptr());
            }

            for entry in config.pins.entries() {
                let pin = sys::Cronet_PublicKeyPins_Create();
                let host = cstr(entry.host.as_bytes())?;
                sys::Cronet_PublicKeyPins_host_set(pin, host.as_ptr());
                for hash in &entry.hashes {
                    let value = cstr(format!("sha256/{}", BASE64.encode(hash)).as_bytes())?;
                    sys::Cronet_PublicKeyPins_pins_sha256_add(pin, value.as_ptr());
                }
                sys::Cronet_PublicKeyPins_include_subdomains_set(pin, entry.include_subdomains);
                sys::Cronet_PublicKeyPins_expiration_date_set(
                    pin,
                    entry.expiration.unix_time_millis(),
                );
                sys::Cronet_EngineParams_public_key_pins_add(params, pin);
                sys::Cronet_PublicKeyPins_Destroy(pin);
            }
            // Pins must hold even for user-installed local trust anchors
            // (proxies) — Cronet skips them by default.
            sys::Cronet_EngineParams_enable_public_key_pinning_bypass_for_local_trust_anchors_set(
                params, false,
            );

            let result = sys::Cronet_Engine_StartWithParams(engine, params);
            sys::Cronet_EngineParams_Destroy(params);
            if result != sys::Cronet_RESULT_Cronet_RESULT_SUCCESS {
                sys::Cronet_Engine_Destroy(engine);
                return Err(map_result(result));
            }

            Ok(CronetEngine {
                ptr: engine,
                executor: Arc::new(PassThroughExecutor::new()),
                shutdown_done: AtomicBool::new(false),
            })
        }
    }

    /// Shuts the engine down. Must not be called from a thread that runs
    /// Cronet callbacks (Cronet refuses with `-202`); call it from the
    /// owning thread before dropping.
    pub fn shutdown(&mut self) -> Result<()> {
        if self.shutdown_done.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = unsafe { sys::Cronet_Engine_Shutdown(self.ptr) };
        if result == sys::Cronet_RESULT_Cronet_RESULT_SUCCESS {
            self.shutdown_done.store(true, Ordering::Release);
            Ok(())
        } else {
            Err(map_result(result))
        }
    }

    /// The engine's version string, e.g. `"143.0.7445.0"`.
    pub fn version(&self) -> String {
        unsafe { CStr::from_ptr(sys::Cronet_Engine_GetVersionString(self.ptr)) }
            .to_string_lossy()
            .into_owned()
    }

    pub(crate) fn ptr(&self) -> sys::Cronet_EnginePtr {
        self.ptr
    }

    pub(crate) fn executor(&self) -> &Arc<PassThroughExecutor> {
        &self.executor
    }
}

impl Drop for CronetEngine {
    fn drop(&mut self) {
        // Best-effort cleanup; use shutdown() for a checked shutdown. An
        // in-flight -202 here is ignored because Drop cannot propagate it.
        if !self.shutdown_done.swap(true, Ordering::SeqCst) {
            unsafe { sys::Cronet_Engine_Shutdown(self.ptr) };
        }
        unsafe { sys::Cronet_Engine_Destroy(self.ptr) };
    }
}

// Safety: Cronet's engine is thread-safe (UrlRequest methods may be called
// from any thread), and our executor handle is Send+Sync.
unsafe impl Send for CronetEngine {}
unsafe impl Sync for CronetEngine {}

/// Maps a `Cronet_RESULT` into a monet-core transport error carrying the
/// raw code.
pub(crate) fn map_result(result: sys::Cronet_RESULT) -> Error {
    Error::transport_with_code(
        format!("cronet call failed with result {result}"),
        PlatformErrorCode::new("cronet", i64::from(result)),
    )
}

pub(crate) fn cstr(bytes: &[u8]) -> Result<CString> {
    CString::new(bytes).map_err(|_| Error::transport("cronet: string contains a NUL byte"))
}

/// Releases the strong reference stored in a callback's client context.
/// Used on non-terminal failure paths where no callback will ever adopt it.
pub(crate) unsafe fn release_context<T>(callback: sys::Cronet_UrlRequestCallbackPtr) {
    let ctx = unsafe { sys::Cronet_UrlRequestCallback_GetClientContext(callback) } as *const T;
    if !ctx.is_null() {
        drop(unsafe { Arc::from_raw(ctx) });
    }
}
