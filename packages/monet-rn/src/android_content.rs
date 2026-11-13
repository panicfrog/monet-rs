//! The `content://` reader (Android): ContentResolver →
//! ParcelFileDescriptor → a raw fd handed to Rust.
//!
//! The fd route is the whole trick: `ContentResolver.openFileDescriptor`
//! returns a `ParcelFileDescriptor`, `detachFd()` surrenders the raw int,
//! and from there it is an ordinary seek+read `std::fs::File` — the same
//! code `read_file_chunk` runs for `file://` paths. JNI appears only at
//! open time (once per chunk read, matching the file path's open-per-call
//! shape).
//!
//! One boundary to know: some providers (Google Drive's stream, some
//! custom ones) serve a pipe, and a pipe cannot `seek` — those surface
//! as an ESPIPE error from the seek. The common cases (SAF documents,
//! MediaStore, FileProvider) are real files and seek fine.
//!
//! The bootstrap stashes the app Context's global ref here; this module
//! is inert without it (the error names the cause).

use std::os::fd::FromRawFd;
use std::sync::OnceLock;

use jni::objects::Global;
use jni::objects::JObject;

/// The bootstrap's stash: the JVM handle + the app Context, both
/// process-global (the Context outlives every call this module makes).
static CONTEXT: OnceLock<(jni::JavaVM, Global<JObject<'static>>)> = OnceLock::new();

/// Called once from the bootstrap, before any read can happen.
pub(crate) fn stash(vm: jni::JavaVM, context: Global<JObject<'static>>) {
    let _ = CONTEXT.set((vm, context));
}

/// Opens `uri` through the ContentResolver and returns its fd, detached
/// from the Java object (the returned `OwnedFd` owns it — dropping
/// closes).
pub(crate) fn open_fd(uri: &str) -> anyhow::Result<std::os::fd::OwnedFd> {
    let Some((vm, context)) = CONTEXT.get() else {
        anyhow::bail!(
            "monet-rn: content:// needs the bootstrap's Context stash — \
             MonetBootstrap.init must run first"
        );
    };
    // The JS thread this is called on is already attached (it is a JVM
    // thread calling into Rust); this is the cheap already-attached path.
    // The closure's error type is anyhow (it From-converts jni::Error,
    // which attach_current_thread requires of E).
    let fd = vm
        .attach_current_thread(|env| -> anyhow::Result<i32> {
            let resolver = env
                .call_method(
                    context,
                    jni::jni_str!("getContentResolver"),
                    jni::jni_sig!("()Landroid/content/ContentResolver;"),
                    &[],
                )?
                .l()?;
            let uri_string = env.new_string(uri)?;
            let uri = env
                .call_static_method(
                    jni::jni_str!("android/net/Uri"),
                    jni::jni_str!("parse"),
                    jni::jni_sig!("(Ljava/lang/String;)Landroid/net/Uri;"),
                    &[(&uri_string).into()],
                )?
                .l()?;
            let mode = env.new_string("r")?;
            let descriptor = env
                .call_method(
                    &resolver,
                    jni::jni_str!("openFileDescriptor"),
                    jni::jni_sig!(
                        "(Landroid/net/Uri;Ljava/lang/String;)Landroid/os/ParcelFileDescriptor;"
                    ),
                    &[(&uri).into(), (&mode).into()],
                )?
                .l()?;
            // detachFd hands ownership of the fd to the caller; the Java
            // object is dead afterwards (no close from its Drop — the
            // OwnedFd we build is the only owner).
            let fd = env
                .call_method(
                    &descriptor,
                    jni::jni_str!("detachFd"),
                    jni::jni_sig!("()I"),
                    &[],
                )?
                .i()?;
            if fd < 0 {
                // detachFd's contract is a non-negative fd or a thrown
                // exception; a negative here means the contract broke.
                anyhow::bail!("ParcelFileDescriptor.detachFd returned {fd}");
            }
            Ok(fd)
        })
        .map_err(|error| anyhow::anyhow!("monet-rn: open {uri}: {error}"))?;
    // SAFETY: detachFd transferred ownership of this fd to us.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}
