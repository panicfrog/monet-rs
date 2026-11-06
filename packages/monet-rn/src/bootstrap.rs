//! The Android bootstrap for the platform HttpEngine stack: the JNI
//! entry behind `MonetBootstrap.init(context)` (standard name mangling —
//! this is the one symbol that cannot wait for RegisterNatives, because
//! it is what registers the rest). Stores the JVM + app Context and
//! binds the six bridge callbacks onto
//! `com.monet.rn.httpengine.HttpEngineBridge`.
//!
//! No-op when the crate was built without the `httpengine` feature (the
//! Java side calls it unconditionally; a missing Rust stack must never
//! crash the app — the rustls fallback applies instead).

#[cfg(all(target_os = "android", feature = "httpengine"))]
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_rn_httpengine_MonetBootstrap_nativeInit(
    mut env: jni::EnvUnowned<'_>,
    _class: jni::objects::JClass<'_>,
    context: jni::objects::JObject<'_>,
) {
    let mut captured: Option<(
        jni::JavaVM,
        jni::objects::Global<jni::objects::JObject<'static>>,
    )> = None;
    let () = env
        .with_env(|env| -> Result<(), jni::errors::Error> {
            let vm = env.get_java_vm()?;
            let context = env.new_global_ref(context)?;
            captured = Some((vm, context));
            Ok(())
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>();
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

#[cfg(all(target_os = "android", not(feature = "httpengine")))]
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_rn_httpengine_MonetBootstrap_nativeInit(
    _env: jni::EnvUnowned<'_>,
    _class: jni::objects::JClass<'_>,
    _context: jni::objects::JObject<'_>,
) {
    // No HttpEngine stack linked: nothing to initialize.
}
