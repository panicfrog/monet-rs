//! The JNI plumbing: JVM/context bootstrap, engine construction (with
//! native pin installation), and request setup/launch. Callbacks arrive
//! in `bridge.rs` (already attached — Cronet calls them from its own
//! threads via our direct executor); this file never runs user logic.
//!
//! Conventions: method/class/field names are `jni_str!` literals,
//! signatures are compile-time-validated raw descriptors, every
//! attach-scope closure returns `Result<_, jni::errors::Error>` and is
//! mapped to a transport error at the boundary.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use futures_channel::oneshot;
use jni::JavaVM;
use jni::objects::{Global, JClass, JClassLoader, JObject, JString};
use monet_core::cancel::CancelToken;
use monet_core::error::{Error, Result};
use monet_core::pin::PinConfig;
use monet_core::request::Request;
use monet_core::response::Response;
use monet_core::transport::BoxFuture;

use crate::registry;

/// The host app's bridge classes (the Kotlin side stays mechanical; see
/// the plan doc). monet-rn will parameterize this via RegisterNatives.
/// The Kotlin bridge classes live with the embedder (the demo app,
/// monet-rn); their names are resolved through the app class loader at
/// setup time using this prefix, configured per embedder.
static BRIDGE_PKG: OnceLock<String> = OnceLock::new();

pub(crate) fn bridge_pkg() -> &'static str {
    BRIDGE_PKG
        .get()
        .map(String::as_str)
        .unwrap_or("com.monet.demo.httpengine")
}

/// Minimum platform level with `android.net.http.HttpEngine`.
const MIN_SDK: i32 = 34;

/// Read buffer per in-flight body, matching monet-cronet's size.
pub(crate) const READ_CHUNK: usize = 64 * 1024;

/// `JavaVM` is a raw pointer (Send + Sync but not Clone); the JVM is
/// process-global, so sharing the pointer by copy is sound.
#[derive(Clone)]
struct SharedVm(JavaVM);

impl SharedVm {
    fn scope<T>(
        &self,
        f: impl FnOnce(&mut jni::Env<'_>) -> std::result::Result<T, jni::errors::Error>,
    ) -> std::result::Result<T, jni::errors::Error> {
        self.0.attach_current_thread_for_scope(f)
    }
}

struct Globals {
    vm: SharedVm,
    context: Global<JObject<'static>>,
    loader: Global<JClassLoader<'static>>,
}

static GLOBALS: OnceLock<Globals> = OnceLock::new();

type JniResult<T> = std::result::Result<T, jni::errors::Error>;

fn jni_err(context: &str) -> impl Fn(jni::errors::Error) -> Error + '_ {
    move |error| Error::transport(format!("httpengine: {context}: {error}"))
}

/// The per-request direct ByteBuffer: the backing memory must outlive the
/// read and never move, so it is pinned and lives in the slot map until
/// the matching onReadCompleted (or a terminal event) replaces/drops it.
struct ReadBuffer {
    // The pinned backing of the direct ByteBuffer handed to Cronet;
    // alive exactly until the matching onReadCompleted replaces it.
    _backing: Pin<Box<[u8]>>,
}

static READ_BUFFERS: LazyLock<Mutex<HashMap<u64, ReadBuffer>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Bootstrap: stores the JVM, the app Context, and the app class loader
/// (bridge classes live in the app; platform classes are in the boot
/// classpath), then binds the six native callbacks onto the host's
/// bridge class via `RegisterNatives` — the class name is a parameter, so
/// each embedder (the demo app, monet-rn) ships its own Kotlin bridge
/// under its own package. Mirrors rustls-platform-verifier's Android
/// init shape.
pub fn init(
    vm: &JavaVM,
    context: Global<JObject<'static>>,
    bridge_class: &str, // e.g. "com.monet.demo.httpengine.HttpEngineBridge"
) -> Result<()> {
    if GLOBALS.get().is_some() {
        return Ok(());
    }
    let shared = SharedVm(vm.clone_handle());
    // The bridge classes share the HttpEngineBridge's package.
    let package = bridge_class
        .rsplit_once('.')
        .map(|(package, _)| package.to_string())
        .unwrap_or_else(|| bridge_class.to_string());
    let _ = BRIDGE_PKG.set(package);
    let (loader, registered) = shared
        .scope(|env| -> JniResult<(Global<JClassLoader<'static>>, ())> {
            let loader = env
                .call_method(
                    &*context,
                    jni::jni_str!("getClassLoader"),
                    jni::jni_sig!("()Ljava/lang/ClassLoader;"),
                    &[],
                )?
                .l()?;
            let loader = env.cast_local::<JClassLoader>(loader)?;
            let loader = env.new_global_ref(loader)?;

            // RegisterNatives: bind the neutral Rust symbols onto the
            // host's Kotlin bridge class. The Kotlin side declares
            // matching `external fun`s; the JVM finds them through this
            // table instead of name mangling.
            let name = env.new_string(bridge_class.replace('/', "."))?;
            let class: JClass<'_> = loader.load_class(env, name)?;
            use jni::NativeMethod;
            // SAFETY: each fn_ptr matches its descriptor — static
            // methods taking (EnvUnowned, JClass, jlong, JObject,
            // JObject[, JObject]) exactly as bridge.rs declares them.
            let methods = unsafe {
                [
                    NativeMethod::from_raw_parts(
                        jni::jni_str!("onStarted"),
                        jni::jni_str!("(JLandroid/net/http/UrlRequest;Landroid/net/http/UrlResponseInfo;)V"),
                        crate::bridge::native_on_started as *mut std::ffi::c_void,
                    ),
                    NativeMethod::from_raw_parts(
                        jni::jni_str!("onReadCompleted"),
                        jni::jni_str!("(JLandroid/net/http/UrlRequest;Landroid/net/http/UrlResponseInfo;Ljava/nio/ByteBuffer;)V"),
                        crate::bridge::native_on_read_completed as *mut std::ffi::c_void,
                    ),
                    NativeMethod::from_raw_parts(
                        jni::jni_str!("onRedirect"),
                        jni::jni_str!("(JLandroid/net/http/UrlRequest;Landroid/net/http/UrlResponseInfo;Ljava/lang/String;)V"),
                        crate::bridge::native_on_redirect as *mut std::ffi::c_void,
                    ),
                    NativeMethod::from_raw_parts(
                        jni::jni_str!("onSucceeded"),
                        jni::jni_str!("(JLandroid/net/http/UrlRequest;Landroid/net/http/UrlResponseInfo;)V"),
                        crate::bridge::native_on_succeeded as *mut std::ffi::c_void,
                    ),
                    NativeMethod::from_raw_parts(
                        jni::jni_str!("onFailed"),
                        jni::jni_str!("(JLandroid/net/http/UrlRequest;Landroid/net/http/UrlResponseInfo;Landroid/net/http/HttpException;)V"),
                        crate::bridge::native_on_failed as *mut std::ffi::c_void,
                    ),
                    NativeMethod::from_raw_parts(
                        jni::jni_str!("onCanceled"),
                        jni::jni_str!("(JLandroid/net/http/UrlRequest;Landroid/net/http/UrlResponseInfo;)V"),
                        crate::bridge::native_on_canceled as *mut std::ffi::c_void,
                    ),
                ]
            };
            unsafe { env.register_native_methods(&class, &methods) }?;
            Ok((loader, ()))
        })
        .map_err(jni_err("init"))?;
    let _ = registered;
    let _ = GLOBALS.set(Globals {
        vm: shared,
        context,
        loader,
    });
    Ok(())
}

/// A shared handle to the constructed `HttpEngine`.
pub struct EngineHandle {
    engine: Global<JObject<'static>>,
}

/// Releases the engine's native resources. The engine's Cronet context
/// holds a broadcast receiver the system keeps strongly referenced until
/// `shutdown()` — without this call every engine ever built leaks one
/// permanently, and the process dies at the 1000-receiver cap. Errors
/// are dropped (and the global ref still releases with the handle): a
/// failed shutdown must not crash the embedder.
pub fn shutdown_engine(engine: &EngineHandle) {
    let Some(globals) = GLOBALS.get() else {
        return;
    };
    let _ = globals.vm.scope(|env| -> JniResult<()> {
        env.call_method(
            &*engine.engine,
            jni::jni_str!("shutdown"),
            jni::jni_sig!("()V"),
            &[],
        )?;
        Ok(())
    });
}

/// The engine's version string (e.g. "121.0.6167.71").
pub fn engine_version(_engine: &EngineHandle) -> String {
    let Some(globals) = GLOBALS.get() else {
        return "<not initialized>".into();
    };
    globals
        .vm
        .scope(|env| -> JniResult<String> {
            let version = env.call_static_method(
                jni::jni_str!("android/net/http/HttpEngine"),
                jni::jni_str!("getVersionString"),
                jni::jni_sig!("()Ljava/lang/String;"),
                &[],
            )?;
            let version = version.l()?;
            Ok(read_string(env, &version)?)
        })
        .unwrap_or_else(|e| format!("<version unavailable: {e}>"))
}

/// Builds the platform engine: SDK gate, Brotli on, and every PinConfig
/// entry installed through the builder's native `addPublicKeyPins`.
pub fn build_engine(pins: &PinConfig) -> Result<EngineHandle> {
    let globals = GLOBALS.get().ok_or_else(|| {
        Error::transport("httpengine: init(vm, context) must run before constructing the transport")
    })?;
    let context = globals.vm.clone();
    globals
        .vm
        .scope(|env| -> JniResult<EngineHandle> {
            let sdk = env
                .get_static_field(
                    jni::jni_str!("android/os/Build$VERSION"),
                    jni::jni_str!("SDK_INT"),
                    jni::jni_sig!("I"),
                )?
                .i()?;
            if sdk < MIN_SDK {
                return Err(jni::errors::Error::JniCall(
                    jni::errors::JniError::InvalidArguments,
                ))
                .map_err(|_| jni::errors::Error::JniCall(jni::errors::JniError::InvalidArguments));
            }
            let _ = &context;

            let builder_class: JClass<'_> =
                env.find_class(jni::jni_str!("android/net/http/HttpEngine$Builder"))?;
            let builder = env.new_object(
                &builder_class,
                jni::jni_sig!("(Landroid/content/Context;)V"),
                &[(&*globals.context).into()],
            )?;

            for entry in pins.entries() {
                install_pins(env, &builder, entry)?;
            }

            // Pins only bite against publicly-trusted chains by default:
            // Cronet bypasses pin enforcement for local trust anchors
            // (user-installed / network-security-config CAs) — a mismatch
            // against a locally-anchored cert resolves 200. That is the H3
            // boundary on record (reproduced on both MIUI and AOSP API 35).
            // An app that pins wants enforcement exactly there — a locally
            // installed MITM CA is the attack pinning exists for — so the
            // bypass is disabled whenever pins are set. Unpinned engines
            // keep the platform default.
            if !pins.entries().is_empty() {
                env.call_method(
                    &builder,
                    jni::jni_str!("setEnablePublicKeyPinningBypassForLocalTrustAnchors"),
                    jni::jni_sig!("(Z)Landroid/net/http/HttpEngine$Builder;"),
                    &[false.into()],
                )?;
            }

            env.call_method(
                &builder,
                jni::jni_str!("setEnableBrotli"),
                jni::jni_sig!("(Z)Landroid/net/http/HttpEngine$Builder;"),
                &[true.into()],
            )?;

            let engine = env
                .call_method(
                    &builder,
                    jni::jni_str!("build"),
                    jni::jni_sig!("()Landroid/net/http/HttpEngine;"),
                    &[],
                )?
                .l()?;
            log::info!(
                "httpengine: engine built ({} pin entries, SDK {})",
                pins.entries().len(),
                sdk
            );
            Ok(EngineHandle {
                engine: env.new_global_ref(engine)?,
            })
        })
        .map_err(|e| {
            // Where a swallowed pin assembly surfaces — without this line
            // it fails silently into the rustls fallback (or worse, a
            // pin-less engine).
            log::info!("httpengine: engine build failed: {e}");
            jni_err("engine build")(e)
        })
}

/// One PinConfig entry → `addPublicKeyPins(host, Set<byte[]>,
/// includeSubdomains, Instant)`.
fn install_pins(
    env: &mut jni::Env<'_>,
    builder: &JObject<'_>,
    entry: &monet_core::pin::PinEntry,
) -> JniResult<()> {
    let set_class: JClass<'_> = env.find_class(jni::jni_str!("java/util/HashSet"))?;
    let set = env.new_object(&set_class, jni::jni_sig!("()V"), &[])?;
    for hash in &entry.hashes {
        let array = env.new_byte_array(hash.len())?;
        array.set_region(env, 0, bytes_as_i8(hash))?;
        env.call_method(
            &set,
            jni::jni_str!("add"),
            jni::jni_sig!("(Ljava/lang/Object;)Z"),
            &[(&array).into()],
        )?;
    }
    let host = env.new_string(&entry.host)?;
    let expiration = entry.expiration.unix_time_millis();
    let instant = env.call_static_method(
        jni::jni_str!("java/time/Instant"),
        jni::jni_str!("ofEpochMilli"),
        jni::jni_sig!("(J)Ljava/time/Instant;"),
        &[expiration.into()],
    )?;
    env.call_method(
        builder,
        jni::jni_str!("addPublicKeyPins"),
        jni::jni_sig!(
            "(Ljava/lang/String;Ljava/util/Set;ZLjava/time/Instant;)Landroid/net/http/HttpEngine$Builder;"
        ),
        &[
            (&host).into(),
            (&set).into(),
            entry.include_subdomains.into(),
            (&instant).into(),
        ],
    )?;
    log::info!(
        "httpengine: addPublicKeyPins({} = {} hash(es), subdomains={}, expires {})",
        entry.host,
        entry.hashes.len(),
        entry.include_subdomains,
        entry.expiration.unix_time_millis()
    );
    Ok(())
}

/// `&[u8]` as `&[i8]` for JNI byte-array regions.
fn bytes_as_i8(bytes: &[u8]) -> &[i8] {
    // SAFETY: u8 and i8 share layout; the borrow is read-only.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast(), bytes.len()) }
}

/// Reads a `java.lang.String` object's contents (runtime-checked cast:
/// `JObject` has no `Into<JString>` in jni 0.22).
fn read_string(env: &mut jni::Env<'_>, object: &JObject<'_>) -> JniResult<String> {
    let string = env.as_cast::<JString>(object)?;
    Ok((*string).to_string())
}

/// Executes one request: buffer the body (v1), set the request up over
/// JNI, start it, and race the head against the token — the same shape
/// as every other monet transport.
pub fn execute(
    engine: &EngineHandle,
    mut req: Request,
    cancel: CancelToken,
) -> BoxFuture<Result<Response>> {
    // One extra global ref for the async block's lifetime ('static).
    let engine = duplicate_global(&engine.engine);
    Box::pin(async move {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        // v1 uploads are fully buffered (nitro-fetch / monet-cronet parity).
        let body = buffer_body(std::mem::replace(
            &mut req.body,
            monet_core::body::Body::empty(),
        ))
        .await?;

        let (id, head_rx) = start_request(&engine, &req, &cancel, body)?;
        let req_timeout_ms = req.timeout_ms.map(std::time::Duration::from_millis);

        // The per-request head deadline (Cronet has no timeout API —
        // the deadline races the head here and fires the native cancel).
        let deadline = req_timeout_ms.map(futures_timer::Delay::new);
        enum Step<T> {
            Head(T),
            Cancelled,
            TimedOut,
        }
        let head = async { Step::Head(head_rx.await) };
        let early = async {
            futures_lite::future::race(
                async {
                    cancel.cancelled().await;
                    Step::Cancelled
                },
                async {
                    match deadline {
                        Some(mut delay) => {
                            delay.await;
                            Step::TimedOut
                        }
                        None => std::future::pending().await,
                    }
                },
            )
            .await
        };
        match futures_lite::future::race(head, early).await {
            Step::Head(Ok(response)) => response,
            Step::Head(Err(_)) => Err(Error::transport(
                "httpengine: request state dropped before the response head",
            )),
            Step::Cancelled => Err(Error::Cancelled),
            Step::TimedOut => {
                registry::cancel_native(id);
                Err(Error::transport("httpengine: response head timed out"))
            }
        }
    })
}

/// Builds, arms, and starts one UrlRequest. The error path removes the
/// registry entry (the request never started, so no callback will).
/// Allocates the request, starts it, and arms both native closures.
/// Sync on purpose: constructing the JNI closures here keeps the
/// execute future's async block free of the `vm.scope` HRTB — calling
/// these constructors directly in the block breaks the future's Send
/// resolution ("not general enough" over JObject's lifetime), the same
/// reason `engine`'s global ref is duplicated before the block.
fn start_request(
    engine: &Global<JObject<'static>>,
    req: &Request,
    cancel: &CancelToken,
    body: Vec<u8>,
) -> Result<(u64, oneshot::Receiver<Result<Response>>)> {
    let (id, head_rx) = registry::allocate(cancel.clone());
    let request = setup_and_start(engine, id, req, body).inspect_err(|_| {
        registry::remove(id);
    })?;
    // Cancellation any time after start maps to UrlRequest.cancel().
    registry::arm_native_cancel(id, request_cancel(duplicate_global(&request)));
    // The read-loop resume (v2/W2b): the body watcher fires it after
    // draining the consumer's backlog — the same read the callback
    // path issues, from whatever thread fires it.
    registry::arm_resume_read(id, request_resume_read(id, request));
    Ok((id, head_rx))
}

fn setup_and_start(
    engine: &Global<JObject<'static>>,
    id: u64,
    req: &Request,
    body: Vec<u8>,
) -> Result<Global<JObject<'static>>> {
    // The per-request no-follow policy rides the bridge's redirect
    // callback (the platform builder has no redirect knob).
    if req.follow_redirects == Some(false) {
        crate::bridge::mark_no_follow(id);
    }
    let globals = GLOBALS
        .get()
        .ok_or_else(|| Error::transport("httpengine: init(vm, context) must run first"))?;
    globals
        .vm
        .scope(|env| -> JniResult<Global<JObject<'static>>> {
            let loader = &globals.loader;
            let callback_name = env.new_string(format!("{}.MonetRequestCallback", bridge_pkg()))?;
            let callback_class: JClass<'_> = loader.load_class(env, callback_name)?;
            let callback =
                env.new_object(&callback_class, jni::jni_sig!("(J)V"), &[(id as i64).into()])?;
            let executor_name = env.new_string(format!("{}.MonetDirectExecutor", bridge_pkg()))?;
            let executor_class: JClass<'_> = loader.load_class(env, executor_name)?;
            let executor = env.new_object(&executor_class, jni::jni_sig!("()V"), &[])?;

            let url = env.new_string(req.url.as_str())?;
            let builder = env
                .call_method(
                    &**engine,
                    jni::jni_str!("newUrlRequestBuilder"),
                    jni::jni_sig!(
                        "(Ljava/lang/String;Ljava/util/concurrent/Executor;Landroid/net/http/UrlRequest$Callback;)Landroid/net/http/UrlRequest$Builder;"
                    ),
                    &[(&url).into(), (&executor).into(), (&callback).into()],
                )?
                .l()?;

            let method = env.new_string(req.method.as_str())?;
            env.call_method(
                &builder,
                jni::jni_str!("setHttpMethod"),
                jni::jni_sig!("(Ljava/lang/String;)Landroid/net/http/UrlRequest$Builder;"),
                &[(&method).into()],
            )?;

            for (name, value) in req.headers.iter_flattened() {
                let name = env.new_string(name)?;
                let value = env.new_string(value)?;
                env.call_method(
                    &builder,
                    jni::jni_str!("addHeader"),
                    jni::jni_sig!(
                        "(Ljava/lang/String;Ljava/lang/String;)Landroid/net/http/UrlRequest$Builder;"
                    ),
                    &[(&name).into(), (&value).into()],
                )?;
            }

            if !body.is_empty() {
                let provider_name = env.new_string(format!("{}.MonetUploadProvider", bridge_pkg()))?;
                let provider_class: JClass<'_> = loader.load_class(env, provider_name)?;
                let array = env.new_byte_array(body.len())?;
                array.set_region(env, 0, bytes_as_i8(&body))?;
                let provider =
                    env.new_object(&provider_class, jni::jni_sig!("([B)V"), &[(&array).into()])?;
                env.call_method(
                    &builder,
                    jni::jni_str!("setUploadDataProvider"),
                    jni::jni_sig!(
                        "(Landroid/net/http/UploadDataProvider;Ljava/util/concurrent/Executor;)Landroid/net/http/UrlRequest$Builder;"
                    ),
                    &[(&provider).into(), (&executor).into()],
                )?;
            }

            env.call_method(
                &builder,
                jni::jni_str!("setDirectExecutorAllowed"),
                jni::jni_sig!("(Z)Landroid/net/http/UrlRequest$Builder;"),
                &[true.into()],
            )?;

            let request = env
                .call_method(
                    &builder,
                    jni::jni_str!("build"),
                    jni::jni_sig!("()Landroid/net/http/UrlRequest;"),
                    &[],
                )?
                .l()?;
            let request = env.new_global_ref(request)?;
            env.call_method(&*request, jni::jni_str!("start"), jni::jni_sig!("()V"), &[])?;
            Ok(request)
        })
        .map_err(jni_err("request setup"))
}

/// The native-cancel closure handed to the registry; re-attaches
/// whatever thread fires it.
fn request_cancel(request: Global<JObject<'static>>) -> registry::CancelNative {
    let vm = GLOBALS
        .get()
        .expect("request exists only after init")
        .vm
        .clone();
    Arc::new(move || {
        let _ = vm.scope(|env| -> JniResult<()> {
            env.call_method(
                &*request,
                jni::jni_str!("cancel"),
                jni::jni_sig!("()V"),
                &[],
            )?;
            Ok(())
        });
    })
}

/// A second JNI global ref for the same object (each holder owns and
/// drops its own handle — `Global` is not Clone by design).
fn duplicate_global(global: &Global<JObject<'static>>) -> Global<JObject<'static>> {
    let vm = GLOBALS
        .get()
        .expect("handle duplication requires init")
        .vm
        .clone();
    vm.scope(|env| -> JniResult<Global<JObject<'static>>> { Ok(env.new_global_ref(&**global)?) })
        .expect("duplicating a live global ref cannot fail")
}

/// The read-resume closure handed to the registry (v2/W2b): the body
/// watcher fires it after the consumer has drained the overflow — the
/// same read the JNI callback issues, re-attaching whatever thread
/// fires it.
fn request_resume_read(id: u64, request: Global<JObject<'static>>) -> registry::ResumeRead {
    Arc::new(move || {
        issue_read(id, &request);
    })
}

/// Issues the next body read: a fresh direct ByteBuffer whose backing
/// lives in the slot map until the matching onReadCompleted replaces it.
pub(crate) fn issue_read(id: u64, request: &Global<JObject<'static>>) {
    let Some(globals) = GLOBALS.get() else {
        return;
    };
    let result = globals.vm.scope(|env| -> JniResult<()> {
        let backing: Pin<Box<[u8]>> = Pin::from(vec![0u8; READ_CHUNK].into_boxed_slice());
        // SAFETY: the backing is pinned for the buffer's lifetime and is
        // placed in the slot map below before `read` can complete.
        // SAFETY: the backing is pinned for the buffer's lifetime and is
        // placed in the slot map below before `read` can complete.
        let buffer =
            unsafe { env.new_direct_byte_buffer(backing.as_ptr().cast_mut(), backing.len())? };
        env.call_method(
            &**request,
            jni::jni_str!("read"),
            jni::jni_sig!("(Ljava/nio/ByteBuffer;)V"),
            &[(&*buffer).into()],
        )?;
        READ_BUFFERS
            .lock()
            .unwrap()
            .insert(id, ReadBuffer { _backing: backing });
        Ok(())
    });
    if result.is_err() {
        registry::on_error(id, "issuing read failed");
    }
}

/// Drops a request's read buffer slot (terminal events, or between
/// reads — the buffer global must die after Cronet is done with it).
pub(crate) fn drop_read_buffer(id: u64) {
    READ_BUFFERS.lock().unwrap().remove(&id);
}

/// Collects a request body into one buffer (buffered-upload v1).
async fn buffer_body(mut body: monet_core::body::Body) -> Result<Vec<u8>> {
    use futures_lite::StreamExt;
    let mut buf = Vec::new();
    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf)
}

/// Copies the raw `JavaVM` pointer into a fresh `JavaVM` handle (the
/// pointer is process-global and never dangling).
fn vm_handle(vm: &JavaVM) -> JavaVM {
    // SAFETY: JavaVM wraps a *mut sys::JavaVM owned by the process; the
    // handle is a value copy of the pointer, and the JVM outlives all
    // transport threads (shutdown happens via the embedder, never here).
    unsafe { std::ptr::read(vm as *const JavaVM) }
}

/// Handle-copy extension used at the boundaries above.
trait JavaVmCopy {
    fn clone_handle(&self) -> JavaVM;
}

impl JavaVmCopy for JavaVM {
    fn clone_handle(&self) -> JavaVM {
        vm_handle(self)
    }
}
