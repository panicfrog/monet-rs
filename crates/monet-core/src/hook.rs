//! Request/response hook chain.
//!
//! Rust-native hooks and (later, via monet-rn) JS-registered hooks adapt to
//! the same traits and share one chain.
//!
//! Hooks take their argument **by value** and return it — the Tower-style
//! shape. References can't cross an `await` into a `'static` boxed future,
//! and ownership keeps hooks composable without lifetime gymnastics: a hook
//! that needs async work (e.g. fetching a token) simply awaits, then
//! mutates, then returns the request.
//!
//! Observers are the read-only half of the surface: they see the same
//! values but cannot affect the pipeline (no return value, the chain does
//! not wait). They run at a fixed position — after every mutating hook, so
//! they observe the final form (pre-wire for requests, pre-delivery for
//! responses). Observers get a shared reference; anything async they need
//! to do (logging, metrics sinks) is their own concern to spawn.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::request::Request;
use crate::response::Response;
use crate::transport::BoxFuture;

/// What a request hook hands back: the (possibly rewritten) request
/// continues to the transport, or the hook answered the request itself —
/// no transport runs, nothing goes on the wire, and the pipeline
/// continues at the response chain with this response (v2/W3's mock).
#[derive(Debug)]
pub enum HookOutcome {
    /// The request, possibly rewritten, continues.
    Modified(Request),
    /// The hook short-circuits the transport: this response (already
    /// carrying whatever extensions the pipeline threads) proceeds
    /// through the response hooks and delivery.
    Mocked(Response),
}

/// Runs before a request hits the transport; may rewrite URL, headers, or
/// body, and must return the request for the next hook — or answer the
/// request outright with [`HookOutcome::Mocked`].
pub trait RequestHook: Send + Sync {
    /// A stable name for error reporting.
    fn name(&self) -> &str {
        "request-hook"
    }

    /// Called with the request, in registration order. The first hook to
    /// return [`HookOutcome::Mocked`] ends the chain (there is no request
    /// left to mutate).
    fn on_request(&self, req: Request) -> BoxFuture<Result<HookOutcome>>;
}

/// Runs when the response head arrives, before the body streams to the
/// consumer; may rewrite status or headers, or swap the body.
pub trait ResponseHook: Send + Sync {
    /// A stable name for error reporting.
    fn name(&self) -> &str {
        "response-hook"
    }

    /// Called with the response, in registration order.
    fn on_response(&self, res: Response) -> BoxFuture<Result<Response>>;
}

/// Read-only view of the final outgoing request (after every mutating
/// hook, before the transport). Fire-and-forget: the pipeline does not
/// wait, and the observer cannot affect it — there is nothing to return.
pub trait RequestObserver: Send + Sync {
    /// Called with the final request, in registration order, after every
    /// mutating hook has run.
    fn on_request(&self, req: &Request);
}

/// Read-only view of the final response head (after every mutating hook,
/// before delivery to the consumer). Same fire-and-forget contract as
/// [`RequestObserver`].
pub trait ResponseObserver: Send + Sync {
    /// Called with the final response, in registration order, after every
    /// mutating hook has run.
    fn on_response(&self, res: &Response);
}

/// An ordered hook chain. Request and response hooks each run in
/// registration order; observers run after them, in registration order.
///
/// Cheap to clone (arcs to the hooks), so a front end can hand snapshots
/// to per-request tasks without holding a lock across an await.
#[derive(Default, Clone)]
pub struct HookRegistry {
    request_hooks: Vec<Arc<dyn RequestHook>>,
    response_hooks: Vec<Arc<dyn ResponseHook>>,
    request_observers: Vec<Arc<dyn RequestObserver>>,
    response_observers: Vec<Arc<dyn ResponseObserver>>,
}

impl HookRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a request hook.
    pub fn add_request_hook(&mut self, hook: impl RequestHook + 'static) -> &mut Self {
        self.request_hooks.push(Arc::new(hook));
        self
    }

    /// Appends a response hook.
    pub fn add_response_hook(&mut self, hook: impl ResponseHook + 'static) -> &mut Self {
        self.response_hooks.push(Arc::new(hook));
        self
    }

    /// Appends a read-only request observer.
    pub fn add_request_observer(&mut self, observer: impl RequestObserver + 'static) -> &mut Self {
        self.request_observers.push(Arc::new(observer));
        self
    }

    /// Appends a read-only response observer.
    pub fn add_response_observer(
        &mut self,
        observer: impl ResponseObserver + 'static,
    ) -> &mut Self {
        self.response_observers.push(Arc::new(observer));
        self
    }

    /// Number of registered request hooks.
    pub fn request_hook_count(&self) -> usize {
        self.request_hooks.len()
    }

    /// Number of registered response hooks.
    pub fn response_hook_count(&self) -> usize {
        self.response_hooks.len()
    }

    /// Number of registered request observers.
    pub fn request_observer_count(&self) -> usize {
        self.request_observers.len()
    }

    /// Number of registered response observers.
    pub fn response_observer_count(&self) -> usize {
        self.response_observers.len()
    }

    /// Drops every hook and observer (the monet-rn `clear_hooks`).
    pub fn clear(&mut self) {
        self.request_hooks.clear();
        self.response_hooks.clear();
        self.request_observers.clear();
        self.response_observers.clear();
    }

    /// Runs every request hook in order. The first failure stops the chain
    /// and is wrapped in [`Error::Hook`] with the hook's name; so does the
    /// first [`HookOutcome::Mocked`] (there is no request left to mutate).
    /// On a [`HookOutcome::Modified`] outcome, every request observer sees
    /// the final request before it returns — a mocked request never goes
    /// on the wire, so there is no final wire form for them to observe
    /// (the response observers see the mock after its response chain).
    pub async fn run_request_hooks(&self, req: Request) -> Result<HookOutcome> {
        let mut req = req;
        for hook in &self.request_hooks {
            match hook.on_request(req).await.map_err(|e| Error::Hook {
                hook: hook.name().to_string(),
                source: Box::new(e),
            })? {
                HookOutcome::Modified(next) => req = next,
                mocked @ HookOutcome::Mocked(_) => return Ok(mocked),
            }
        }
        for observer in &self.request_observers {
            observer.on_request(&req);
        }
        Ok(HookOutcome::Modified(req))
    }

    /// Runs every response hook in order, with the same error semantics as
    /// [`HookRegistry::run_request_hooks`]. On success, every response
    /// observer sees the final response before it returns.
    pub async fn run_response_hooks(&self, res: Response) -> Result<Response> {
        let mut res = res;
        for hook in &self.response_hooks {
            res = hook.on_response(res).await.map_err(|e| Error::Hook {
                hook: hook.name().to_string(),
                source: Box::new(e),
            })?;
        }
        for observer in &self.response_observers {
            observer.on_response(&res);
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use crate::body::Body;
    use crate::headers::HeaderMap;
    use crate::method::Method;
    use futures_lite::future::block_on;
    use url::Url;

    fn request() -> Request {
        Request::new(Method::Get, Url::parse("https://example.com").unwrap())
    }

    fn response() -> Response {
        Response::new(200, HeaderMap::new(), Body::empty())
    }

    /// A hook that appends its name to header `x-order` and records the visit.
    struct StampingHook {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl StampingHook {
        fn new(name: &'static str, log: &Arc<Mutex<Vec<String>>>) -> Self {
            StampingHook {
                name,
                log: log.clone(),
            }
        }
    }

    impl RequestHook for StampingHook {
        fn name(&self) -> &str {
            self.name
        }
        fn on_request(&self, req: Request) -> BoxFuture<Result<HookOutcome>> {
            let log = self.log.clone();
            let name = self.name;
            Box::pin(async move {
                log.lock().unwrap().push(format!("req:{name}"));
                let mut req = req;
                let existing = req.headers.get("x-order").unwrap_or_default();
                req.headers.set("x-order", format!("{existing}{name}"))?;
                Ok(HookOutcome::Modified(req))
            })
        }
    }

    impl ResponseHook for StampingHook {
        fn name(&self) -> &str {
            self.name
        }
        fn on_response(&self, res: Response) -> BoxFuture<Result<Response>> {
            let log = self.log.clone();
            let name = self.name;
            Box::pin(async move {
                log.lock().unwrap().push(format!("res:{name}"));
                let mut res = res;
                res.headers.set("x-order", name)?;
                Ok(res)
            })
        }
    }

    #[test]
    fn hooks_run_in_registration_order_and_mutate() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_request_hook(StampingHook::new("a", &log))
            .add_request_hook(StampingHook::new("b", &log));
        assert_eq!(registry.request_hook_count(), 2);

        let req = match block_on(registry.run_request_hooks(request())).unwrap() {
            HookOutcome::Modified(req) => req,
            HookOutcome::Mocked(_) => panic!("nothing mocks here"),
        };
        assert_eq!(req.headers.get("x-order").unwrap(), "ab");
        assert_eq!(*log.lock().unwrap(), vec!["req:a", "req:b"]);
    }

    #[test]
    fn failing_hook_stops_the_chain() {
        struct FailHook;
        impl RequestHook for FailHook {
            fn name(&self) -> &str {
                "fail"
            }
            fn on_request(&self, req: Request) -> BoxFuture<Result<HookOutcome>> {
                Box::pin(async {
                    let _ = req;
                    Err(Error::transport("nope"))
                })
            }
        }

        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_request_hook(FailHook)
            .add_request_hook(StampingHook::new("never", &log));

        let err = block_on(registry.run_request_hooks(request())).unwrap_err();
        match err {
            Error::Hook { hook, source } => {
                assert_eq!(hook, "fail");
                assert!(matches!(*source, Error::Transport { .. }));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert!(log.lock().unwrap().is_empty()); // second hook never ran
    }

    #[test]
    fn response_hooks_mutate_response() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry.add_response_hook(StampingHook::new("r", &log));

        let res = block_on(registry.run_response_hooks(response())).unwrap();
        assert_eq!(res.headers.get("x-order").unwrap(), "r");
        assert_eq!(*log.lock().unwrap(), vec!["res:r"]);
    }

    #[test]
    fn empty_registry_is_a_passthrough() {
        let registry = HookRegistry::new();
        let req = match block_on(registry.run_request_hooks(request())).unwrap() {
            HookOutcome::Modified(req) => req,
            HookOutcome::Mocked(_) => panic!("nothing mocks here"),
        };
        assert_eq!(req.url.as_str(), "https://example.com/");
        let res = block_on(registry.run_response_hooks(response())).unwrap();
        assert_eq!(res.status, 200);
    }

    /// An observer that records the headers it saw.
    struct RecordingObserver {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl RequestObserver for RecordingObserver {
        fn on_request(&self, req: &Request) {
            self.log.lock().unwrap().push(format!(
                "req:{}",
                req.headers.get("x-order").unwrap_or_default()
            ));
        }
    }

    impl ResponseObserver for RecordingObserver {
        fn on_response(&self, res: &Response) {
            self.log.lock().unwrap().push(format!(
                "res:{}",
                res.headers.get("x-order").unwrap_or_default()
            ));
        }
    }

    #[test]
    fn observers_see_the_final_form_after_hooks() {
        let hook_log = Arc::new(Mutex::new(Vec::new()));
        let observer_log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_request_hook(StampingHook::new("a", &hook_log))
            .add_request_hook(StampingHook::new("b", &hook_log))
            .add_request_observer(RecordingObserver {
                log: observer_log.clone(),
            });
        assert_eq!(registry.request_observer_count(), 1);

        block_on(registry.run_request_hooks(request())).unwrap();
        // The observer saw the request as it leaves the chain — after both
        // hooks stamped it.
        assert_eq!(*observer_log.lock().unwrap(), vec!["req:ab"]);
    }

    #[test]
    fn response_observers_see_the_final_form() {
        let hook_log = Arc::new(Mutex::new(Vec::new()));
        let observer_log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_response_hook(StampingHook::new("r", &hook_log))
            .add_response_observer(RecordingObserver {
                log: observer_log.clone(),
            });

        block_on(registry.run_response_hooks(response())).unwrap();
        assert_eq!(*observer_log.lock().unwrap(), vec!["res:r"]);
    }

    #[test]
    fn observers_do_not_run_when_a_hook_fails() {
        // Observers see the final form; a failed chain has no final form,
        // so nothing is observed (and the error passes through untouched).
        let hook_log = Arc::new(Mutex::new(Vec::new()));
        let observer_log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_request_observer(RecordingObserver {
                log: observer_log.clone(),
            })
            .add_request_hook(StampingHook::new("a", &hook_log));
        // A failing hook after the observer registration — order between
        // hooks and observers is fixed (hooks first), registration order
        // only matters within each class.
        struct FailHook;
        impl RequestHook for FailHook {
            fn name(&self) -> &str {
                "fail"
            }
            fn on_request(&self, req: Request) -> BoxFuture<Result<HookOutcome>> {
                Box::pin(async {
                    let _ = req;
                    Err(Error::transport("nope"))
                })
            }
        }
        registry.add_request_hook(FailHook);

        let err = block_on(registry.run_request_hooks(request())).unwrap_err();
        assert!(matches!(err, Error::Hook { .. }));
        assert!(observer_log.lock().unwrap().is_empty());
    }

    #[test]
    fn registry_clone_snapshots_the_chains() {
        // The cheap clone front ends hand to per-request tasks: the clone
        // runs the hooks that existed at clone time.
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry.add_request_hook(StampingHook::new("a", &log));

        let snapshot = registry.clone();
        registry.add_request_hook(StampingHook::new("b", &log));

        let req = match block_on(snapshot.run_request_hooks(request())).unwrap() {
            HookOutcome::Modified(req) => req,
            HookOutcome::Mocked(_) => panic!("nothing mocks here"),
        };
        assert_eq!(req.headers.get("x-order").unwrap(), "a");
        let req = match block_on(registry.run_request_hooks(request())).unwrap() {
            HookOutcome::Modified(req) => req,
            HookOutcome::Mocked(_) => panic!("nothing mocks here"),
        };
        assert_eq!(req.headers.get("x-order").unwrap(), "ab");
    }

    #[test]
    fn a_mocking_hook_short_circuits_the_chain() {
        // A mock ends the request chain: later hooks never run, request
        // observers see nothing (no wire form exists), and the outcome
        // carries the mocked response through.
        let hook_log = Arc::new(Mutex::new(Vec::new()));
        let observer_log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_request_hook(StampingHook::new("a", &hook_log))
            .add_request_observer(RecordingObserver {
                log: observer_log.clone(),
            });

        struct MockHook;
        impl RequestHook for MockHook {
            fn name(&self) -> &str {
                "mock"
            }
            fn on_request(&self, req: Request) -> BoxFuture<Result<HookOutcome>> {
                Box::pin(async {
                    let _ = req;
                    Ok(HookOutcome::Mocked(Response::new(
                        201,
                        HeaderMap::new(),
                        Body::from_bytes("mocked"),
                    )))
                })
            }
        }
        registry
            .add_request_hook(MockHook)
            .add_request_hook(StampingHook::new("never", &hook_log));

        match block_on(registry.run_request_hooks(request())).unwrap() {
            HookOutcome::Mocked(res) => {
                assert_eq!(res.status, 201);
            }
            HookOutcome::Modified(_) => panic!("the mock must short-circuit"),
        }
        // "a" ran before the mock; the hook after it never did; the
        // observer never fired.
        assert_eq!(*hook_log.lock().unwrap(), vec!["req:a"]);
        assert!(observer_log.lock().unwrap().is_empty());
    }

    #[test]
    fn the_mocked_response_rides_the_response_chain() {
        // The pump's mock path in miniature: the mocked response goes
        // through run_response_hooks, so response hooks rewrite it and
        // response observers see the final form.
        let observer_log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_response_hook(StampingHook::new("r", &observer_log))
            .add_response_observer(RecordingObserver {
                log: observer_log.clone(),
            });

        let mock = Response::new(201, HeaderMap::new(), Body::from_bytes("mocked"));
        let res = block_on(registry.run_response_hooks(mock)).unwrap();
        assert_eq!(res.status, 201);
        assert_eq!(res.headers.get("x-order").unwrap(), "r");
        assert_eq!(
            *observer_log.lock().unwrap(),
            vec!["res:r", "res:r"],
            "the hook and the observer both saw the mock"
        );
    }

    #[test]
    fn clear_drops_everything() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let observer_log = Arc::new(Mutex::new(Vec::new()));
        let mut registry = HookRegistry::new();
        registry
            .add_request_hook(StampingHook::new("a", &log))
            .add_request_observer(RecordingObserver {
                log: observer_log.clone(),
            });
        registry.clear();
        assert_eq!(registry.request_hook_count(), 0);
        assert_eq!(registry.request_observer_count(), 0);

        let req = match block_on(registry.run_request_hooks(request())).unwrap() {
            HookOutcome::Modified(req) => req,
            HookOutcome::Mocked(_) => panic!("nothing mocks here"),
        };
        assert_eq!(req.headers.get("x-order"), None);
        assert!(log.lock().unwrap().is_empty());
        assert!(observer_log.lock().unwrap().is_empty());
    }
}
