//! The Android bootstrap: the JNI entry behind
//! `MonetBootstrap.init(context)` (standard name mangling — this is the
//! one symbol that cannot wait for RegisterNatives, because it is what
//! registers the rest).
//!
//! Three consumptions of the one Context the app hands over:
//!
//! - `rustls_platform_verifier::android`'s host settings (JVM + Context
//!   + class loader). monet-ws and monet-rustls connections with
//!   `TlsTrust::PlatformVerifier` cannot evaluate trust without it —
//!   every TLS handshake rejects with UnknownIssuer. Unconditional: the
//!   verifier serves the WebSocket stack in every configuration, and
//!   the rustls fallback too.
//! - The platform stash (unconditional): the app's cache directory for
//!   `write_cache_file`, and a Context global for the `content://`
//!   reader (`android_content`) — both must work on fallback-rustls
//!   builds, not only under httpengine.
//! - The platform HttpEngine stack (feature `httpengine`): stores the JVM
//!   + app Context and binds the bridge callbacks onto
//!   `com.monet.rn.httpengine.HttpEngineBridge`. No-op without the
//!   feature — the Java side calls init unconditionally; a missing
//!   stack must never crash the app (the rustls fallback applies).

#[cfg(target_os = "android")]
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_rn_httpengine_MonetBootstrap_nativeInit(
    mut env: jni::EnvUnowned<'_>,
    _class: jni::objects::JClass<'_>,
    context: jni::objects::JObject<'_>,
) {
    // First thing: route the `log` facade into logcat. Every monet-rn
    // and vendored-crate log:: call before this point is lost — there
    // are none by construction (this is the module's entry). Info level
    // keeps the battery quiet while wiring failures and pin assembly
    // stay visible.
    android_logger::init_once(
        android_logger::Config::default().with_max_level(log::LevelFilter::Info),
    );
    let mut captured: Option<(
        jni::JavaVM,
        jni::objects::Global<jni::objects::JObject<'static>>,
    )> = None;
    let () = env
        .with_env(|env| -> Result<(), jni::errors::Error> {
            // The verifier init consumes the JObject handle, so every
            // later consumer takes a local ref to the same Context
            // first: the platform stash below and the HttpEngine prep.
            let stashed_context = env.new_local_ref(&context)?;
            #[cfg(feature = "httpengine")]
            let httpengine_context = env.new_local_ref(&context)?;
            // The platform verifier's host settings: every
            // TlsTrust::PlatformVerifier connection (the WebSocket stack
            // in any configuration, the rustls fallback for HTTP)
            // depends on them — without the init, every TLS handshake
            // rejects with UnknownIssuer.
            rustls_platform_verifier::android::init_with_env(env, context)?;
            // The app's cache directory (write_cache_file's landing
            // zone) and the live Context for the content:// reader —
            // unconditional: both must work on fallback-rustls builds
            // too, not only under httpengine.
            let cache = env
                .call_method(&stashed_context, jni::jni_str!("getCacheDir"), jni::jni_sig!("()Ljava/io/File;"), &[])?
                .l()?;
            let absolute = env
                .call_method(&cache, jni::jni_str!("getAbsolutePath"), jni::jni_sig!("()Ljava/lang/String;"), &[])?
                .l()?;
            let dir = (*env.as_cast::<jni::objects::JString>(&absolute)?).to_string();
            log::info!("monet-rn: cache dir = {dir}");
            crate::platform::set_cache_dir(dir);
            crate::android_content::stash(
                env.get_java_vm()?,
                env.new_global_ref(stashed_context)?,
            );
            #[cfg(feature = "httpengine")]
            {
                let vm = env.get_java_vm()?;
                captured = Some((vm, env.new_global_ref(httpengine_context)?));
            }
            Ok(())
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>();
    #[cfg(feature = "httpengine")]
    if let Some((vm, context)) = captured {
        if let Err(error) =
            monet_httpengine::init(&vm, context, "com.monet.rn.httpengine.HttpEngineBridge")
        {
            log::info!(
                "monet-rn: HttpEngine bootstrap failed ({error}) — the rustls fallback applies"
            );
        }
    }
}
