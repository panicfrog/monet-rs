//! The Android JNI surface: one exported function the demo activity
//! calls, returning the formatted battery report. Everything heavy lives
//! in [`crate::run_battery`], shared with the host test.

// Exported symbols are the whole point of this module.
#![allow(unsafe_code)]

use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::jstring;

/// Runs the full battery and returns `name: OK/FAILED` lines plus a
/// summary. Called from MainActivity.runBattery().
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_demo_MainActivity_runBattery(
    mut env: JNIEnv,
    _class: JClass<'_>,
) -> jstring {
    let report = std::panic::catch_unwind(crate::run_battery)
        .map(|reports| crate::format_battery(&reports))
        .unwrap_or_else(|_| "battery panicked".to_string());
    let output = env
        .new_string(report)
        .expect("battery report is valid UTF-8");
    output.into_raw()
}

/// Reserved for the platform-verifier leg (phase 2): hands the JVM and
/// the app Context to rustls-platform-verifier's Android bootstrap.
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_monet_demo_MainActivity_initPlatformVerifier(
    env: JNIEnv,
    _class: JClass<'_>,
) -> jstring {
    let message = "initPlatformVerifier: not wired yet (phase 2)";
    let output = env.new_string(message).expect("message is valid UTF-8");
    output.into_raw()
}
