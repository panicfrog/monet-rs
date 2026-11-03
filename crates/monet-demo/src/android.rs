//! The Android JNI surface: battery entry points plus the
//! platform-verifier bootstrap the app calls with its Context. Everything
//! heavy lives in [`crate::run_battery`], shared with the host test.

// Exported symbols are the whole point of this module.
#![allow(unsafe_code)]

use jni::EnvUnowned;
use jni::objects::{JClass, JObject};
use jni::sys::jstring;

/// Runs the full battery and returns `name: OK/FAILED` lines plus a
/// summary. Called from MainActivity.runBattery().
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_demo_MainActivity_runBattery(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jstring {
    let report = std::panic::catch_unwind(crate::run_battery)
        .map(|reports| crate::format_battery(&reports))
        .unwrap_or_else(|_| "battery panicked".to_string());
    string_out(&mut env, &report)
}

/// Runs the platform-verifier battery (see
/// [`crate::run_platform_battery`]) — requires initPlatformVerifier to
/// have run first.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_demo_MainActivity_runPlatformBattery(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
) -> jstring {
    let report = std::panic::catch_unwind(crate::run_platform_battery)
        .map(|reports| crate::format_battery(&reports))
        .unwrap_or_else(|_| "platform battery panicked".to_string());
    string_out(&mut env, &report)
}

/// Hands the JVM and the app Context to rustls-platform-verifier's
/// Android bootstrap; must run before any `TlsTrust::PlatformVerifier`
/// connection. The outcome is reported back as the returned string.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_demo_MainActivity_initPlatformVerifier(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
    context: JObject<'_>,
) -> jstring {
    let mut message = String::new();
    let outcome = env.with_env(|env| {
        match rustls_platform_verifier::android::init_with_env(env, context) {
            Ok(()) => {
                message = "platform verifier initialized".to_string();
                Ok(())
            }
            Err(err) => {
                message = format!("platform verifier init failed: {err}");
                Err(err)
            }
        }
    });
    let () = outcome.resolve::<jni::errors::LogErrorAndDefault>();
    string_out(&mut env, &message)
}

/// Produces a Java String from a report. A failure here can only be a
/// broken JNI contract; the policy logs it and hands Java a null.
fn string_out(env: &mut EnvUnowned<'_>, text: &str) -> jstring {
    env.with_env(|env| -> Result<jstring, jni::errors::Error> {
        Ok(env.new_string(text)?.into_raw())
    })
    .resolve::<jni::errors::LogErrorAndDefault>()
}
