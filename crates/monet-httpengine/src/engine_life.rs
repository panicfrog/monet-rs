//! Engine lifetime: an HttpEngine may only be shut down once none of its
//! native requests is still running — `shutdown()` throws otherwise, and
//! a failed shutdown leaks the engine's broadcast receiver for good.
//!
//! A request is live from `start()` until Cronet's final callback
//! (onSucceeded / onFailed / onCanceled — exactly one per request). The
//! transport's drop can come first: a cancelled request's pump lets go
//! of the engine before the cancellation's onCanceled arrives.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

/// The deferred `shutdown()` call.
pub(crate) type Shutdown = Box<dyn FnOnce() + Send>;

/// One engine's live requests and its pending shutdown.
#[derive(Default)]
pub(crate) struct EngineLife {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    live: HashSet<u64>,
    shutdown: Option<Shutdown>,
}

/// Request id → its engine's lifetime, for the final callbacks (which
/// only know the id).
static REQUESTS: LazyLock<Mutex<HashMap<u64, Arc<EngineLife>>>> = LazyLock::new(Default::default);

impl EngineLife {
    /// Shuts the engine down once it has no live requests: now, or from
    /// the last live request's final callback.
    pub(crate) fn shut_down_when_idle(&self, shutdown: Shutdown) {
        let now = {
            let mut inner = self.inner.lock().unwrap();
            if inner.live.is_empty() {
                Some(shutdown)
            } else {
                inner.shutdown = Some(shutdown);
                None
            }
        };
        // Outside the lock: the call re-enters the JVM.
        if let Some(shutdown) = now {
            shutdown();
        }
    }
}

/// `id` was started on `life`'s engine.
pub(crate) fn request_started(life: &Arc<EngineLife>, id: u64) {
    life.inner.lock().unwrap().live.insert(id);
    REQUESTS.lock().unwrap().insert(id, Arc::clone(life));
}

/// Cronet delivered `id`'s final callback.
pub(crate) fn request_finished(id: u64) {
    let Some(life) = REQUESTS.lock().unwrap().remove(&id) else {
        return;
    };
    let due = {
        let mut inner = life.inner.lock().unwrap();
        inner.live.remove(&id);
        if inner.live.is_empty() {
            inner.shutdown.take()
        } else {
            None
        }
    };
    // Outside the lock: the call re-enters the JVM. Cronet counts the
    // request finished before invoking its final callback, so the
    // engine is idle by now.
    if let Some(shutdown) = due {
        shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn counter() -> (Arc<AtomicUsize>, Shutdown) {
        let count = Arc::new(AtomicUsize::new(0));
        let probe = Arc::clone(&count);
        (
            count,
            Box::new(move || {
                probe.fetch_add(1, Ordering::SeqCst);
            }),
        )
    }

    #[test]
    fn an_idle_engine_shuts_down_at_once() {
        let life = Arc::new(EngineLife::default());
        request_started(&life, 9001);
        request_finished(9001);
        let (count, shutdown) = counter();
        life.shut_down_when_idle(shutdown);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// The transport drops while a cancelled request's onCanceled is
    /// still on its way: the shutdown waits for it.
    #[test]
    fn shutdown_waits_for_the_last_live_request() {
        let life = Arc::new(EngineLife::default());
        request_started(&life, 9101);
        request_started(&life, 9102);
        let (count, shutdown) = counter();
        life.shut_down_when_idle(shutdown);
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "shut down with live requests"
        );
        request_finished(9101);
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "shut down with a live request"
        );
        request_finished(9102);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        // A duplicate final callback must not shut down twice.
        request_finished(9102);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// Requests of other engines do not hold this one up.
    #[test]
    fn engines_are_independent() {
        let old = Arc::new(EngineLife::default());
        let new = Arc::new(EngineLife::default());
        request_started(&new, 9201);
        let (count, shutdown) = counter();
        old.shut_down_when_idle(shutdown);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        request_finished(9201);
    }
}
