//! The Kotlin bridge's native entry points. Cronet invokes the callback
//! on our direct executor's thread, already inside the JVM; each entry
//! pulls the event's data over JNI and forwards it into the registry.
//! Mechanical by design — the state machine lives in `registry.rs`.
//!
//! Ownership: `Global` handles are owned (no Clone; dropping deletes the
//! JNI global ref). Each holder creates its own: the cancel closure owns
//! one (made in `setup_and_start`), the per-request map here owns one
//! (made in `on_started`). Method calls borrow through the map.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, OnceLock};

use jni::EnvUnowned;
use jni::objects::{Global, JClass, JObject, JString};
use jni::sys::jlong;

use crate::jni::{drop_read_buffer, issue_read};
use crate::registry;

type RequestMap = Mutex<HashMap<u64, Global<JObject<'static>>>>;

static REQUESTS: OnceLock<RequestMap> = OnceLock::new();

fn requests() -> &'static RequestMap {
    REQUESTS.get_or_init(Default::default)
}

/// The policy used to resolve `with_env` outcomes: JNI-level failures
/// here are bugs (the data extraction is mechanical), so log-and-default
/// keeps the callback returning instead of throwing back into Cronet.
type Policy = jni::errors::ThrowRuntimeExAndDefault;

/// Requests whose per-request policy is "do not follow redirects": the
/// redirect callback synthesizes the 3xx response instead of calling
/// `followRedirect` (the platform UrlRequest.Builder has no
/// redirect-behavior knob — the decision belongs to the callback).
static NO_FOLLOW: LazyLock<Mutex<HashSet<u64>>> = LazyLock::new(Default::default);

/// Marks `id` as no-follow (called at request assembly).
pub fn mark_no_follow(id: u64) {
    NO_FOLLOW.lock().unwrap().insert(id);
}

fn is_no_follow(id: u64) -> bool {
    NO_FOLLOW.lock().unwrap().remove(&id)
}

/// Extracts status + headers from a `UrlResponseInfo` into the registry
/// and completes the head. Shared by the normal response path and the
/// no-follow redirect synthesis (both receive an `info`).
fn deliver_head_from_info(
    env: &mut jni::Env<'_>,
    id: u64,
    info: &JObject<'_>,
) -> Result<(), jni::errors::Error> {
    let status: i32 = env
        .call_method(
            info,
            jni::jni_str!("getHttpStatusCode"),
            jni::jni_sig!("()I"),
            &[],
        )?
        .i()?;
    let headers = env.call_method(
        info,
        jni::jni_str!("getHeaders"),
        jni::jni_sig!("()Landroid/net/http/HeaderBlock;"),
        &[],
    )?;
    let list = env.call_method(
        &headers.l()?,
        jni::jni_str!("getAsList"),
        jni::jni_sig!("()Ljava/util/List;"),
        &[],
    )?;
    let list = list.l()?;
    let count: i32 = env
        .call_method(&list, jni::jni_str!("size"), jni::jni_sig!("()I"), &[])?
        .i()?;
    for index in 0..count {
        let entry = env.call_method(
            &list,
            jni::jni_str!("get"),
            jni::jni_sig!("(I)Ljava/lang/Object;"),
            &[index.into()],
        )?;
        let entry = entry.l()?;
        let key = string_arg(env, &entry, jni::jni_str!("getKey"))?;
        let value = string_arg(env, &entry, jni::jni_str!("getValue"))?;
        registry::on_header(id, key.as_bytes(), value.as_bytes());
    }
    registry::head_delivered(id, status as u16);
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "system" fn native_on_started(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
    id: jlong,
    request: JObject<'_>,
    info: JObject<'_>,
) {
    let () = env
        .with_env(|env| -> Result<(), jni::errors::Error> {
            deliver_head_from_info(env, id as u64, &info)?;

            // This map's own global ref; dropped at the terminal event.
            let request_global = env.new_global_ref(request)?;
            requests().lock().unwrap().insert(id as u64, request_global);

            // Borrow the stored global for the first read.
            let guard = requests().lock().unwrap();
            if let Some(request) = guard.get(&(id as u64)) {
                issue_read(id as u64, request);
            }
            Ok(())
        })
        .resolve::<Policy>();
}

#[unsafe(no_mangle)]
pub extern "system" fn native_on_read_completed(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
    id: jlong,
    _request: JObject<'_>,
    _info: JObject<'_>,
    byte_buffer: JObject<'_>,
) {
    let () = env
        .with_env(|env| -> Result<(), jni::errors::Error> {
            let position: i32 = env
                .call_method(
                    &byte_buffer,
                    jni::jni_str!("position"),
                    jni::jni_sig!("()I"),
                    &[],
                )?
                .i()?;
            let buffer = env.as_cast::<jni::objects::JByteBuffer>(&byte_buffer)?;
            let address = env.get_direct_buffer_address(&*buffer)?;
            // SAFETY: the address points at the direct buffer backing we
            // allocated (pinned, slot-owned); position is Cronet's byte count.
            let data: &[u8] = if position as usize > 0 {
                unsafe { std::slice::from_raw_parts(address, position as usize) }
            } else {
                &[][..]
            };
            registry::on_chunk(id as u64, data);

            // The slot's buffer is consumed; the next read wraps a fresh
            // one (issue_read replaces the slot).
            drop_read_buffer(id as u64);
            let guard = requests().lock().unwrap();
            if let Some(request) = guard.get(&(id as u64)) {
                issue_read(id as u64, request);
            }
            Ok(())
        })
        .resolve::<Policy>();
}

#[unsafe(no_mangle)]
pub extern "system" fn native_on_redirect(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
    id: jlong,
    request: JObject<'_>,
    info: JObject<'_>,
    _location: JObject<'_>,
) {
    // The per-request policy decides: follow (the default, monet-cronet
    // parity), or deliver the 3xx itself. The platform engine's manual
    // mode never surfaces the redirect body, so the synthesized response
    // is head + empty body — the status and location header are the
    // contract a no-follow consumer reads.
    if is_no_follow(id as u64) {
        let () = env
            .with_env(|env| -> Result<(), jni::errors::Error> {
                deliver_head_from_info(env, id as u64, &info)?;
                terminal(id);
                registry::on_done(id as u64);
                Ok(())
            })
            .resolve::<Policy>();
        return;
    }
    let () = env
        .with_env(|env| -> Result<(), jni::errors::Error> {
            env.call_method(
                &request,
                jni::jni_str!("followRedirect"),
                jni::jni_sig!("()V"),
                &[],
            )?;
            Ok(())
        })
        .resolve::<Policy>();
}

#[unsafe(no_mangle)]
pub extern "system" fn native_on_succeeded(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
    id: jlong,
    _request: JObject<'_>,
    _info: JObject<'_>,
) {
    terminal(id);
    registry::on_done(id as u64);
}

#[unsafe(no_mangle)]
pub extern "system" fn native_on_failed(
    mut env: EnvUnowned<'_>,
    _class: JClass<'_>,
    id: jlong,
    _request: JObject<'_>,
    _info: JObject<'_>,
    error: JObject<'_>,
) {
    terminal(id);
    let () = env
        .with_env(|env| -> Result<(), jni::errors::Error> {
            let message = env
                .call_method(
                    &error,
                    jni::jni_str!("toString"),
                    jni::jni_sig!("()Ljava/lang/String;"),
                    &[],
                )?
                .l()?;
            let message = string_arg_read(env, &message)?;
            registry::on_error(id as u64, &message);
            Ok(())
        })
        .resolve::<Policy>();
}

#[unsafe(no_mangle)]
pub extern "system" fn native_on_canceled(
    _env: EnvUnowned<'_>,
    _class: JClass<'_>,
    id: jlong,
    _request: JObject<'_>,
    _info: JObject<'_>,
) {
    terminal(id);
    registry::on_cancelled(id as u64);
}

/// Terminal cleanup: drop the read buffer slot and this map's request
/// handle (the cancel closure owns a separate one).
fn terminal(id: jlong) {
    drop_read_buffer(id as u64);
    requests().lock().unwrap().remove(&(id as u64));
}

/// Reads a `()Ljava/lang/String;`-shaped getter off `object`.
fn string_arg(
    env: &mut jni::Env<'_>,
    object: &JObject<'_>,
    getter: &jni::strings::JNIStr,
) -> Result<String, jni::errors::Error> {
    let value = env
        .call_method(object, getter, jni::jni_sig!("()Ljava/lang/Object;"), &[])?
        .l()?;
    let string = env.as_cast::<JString>(&value)?;
    Ok((*string).to_string())
}

/// Reads a string result object (the runtime-checked cast jni 0.22
/// requires — `JObject` has no `Into<JString>`).
fn string_arg_read(
    env: &mut jni::Env<'_>,
    object: &JObject<'_>,
) -> Result<String, jni::errors::Error> {
    let string = env.as_cast::<JString>(object)?;
    Ok((*string).to_string())
}
