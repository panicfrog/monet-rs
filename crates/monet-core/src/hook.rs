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

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::request::Request;
use crate::response::Response;
use crate::transport::BoxFuture;

/// Runs before a request hits the transport; may rewrite URL, headers, or
/// body, and must return the request for the next hook.
pub trait RequestHook: Send + Sync {
    /// A stable name for error reporting.
    fn name(&self) -> &str {
        "request-hook"
    }

    /// Called with the request, in registration order.
    fn on_request(&self, req: Request) -> BoxFuture<Result<Request>>;
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

/// An ordered hook chain. Request and response hooks each run in
/// registration order.
#[derive(Default)]
pub struct HookRegistry {
    request_hooks: Vec<Arc<dyn RequestHook>>,
    response_hooks: Vec<Arc<dyn ResponseHook>>,
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

    /// Number of registered request hooks.
    pub fn request_hook_count(&self) -> usize {
        self.request_hooks.len()
    }

    /// Number of registered response hooks.
    pub fn response_hook_count(&self) -> usize {
        self.response_hooks.len()
    }

    /// Runs every request hook in order. The first failure stops the chain
    /// and is wrapped in [`Error::Hook`] with the hook's name.
    pub async fn run_request_hooks(&self, req: Request) -> Result<Request> {
        let mut req = req;
        for hook in &self.request_hooks {
            req = hook.on_request(req).await.map_err(|e| Error::Hook {
                hook: hook.name().to_string(),
                source: Box::new(e),
            })?;
        }
        Ok(req)
    }

    /// Runs every response hook in order, with the same error semantics as
    /// [`HookRegistry::run_request_hooks`].
    pub async fn run_response_hooks(&self, res: Response) -> Result<Response> {
        let mut res = res;
        for hook in &self.response_hooks {
            res = hook.on_response(res).await.map_err(|e| Error::Hook {
                hook: hook.name().to_string(),
                source: Box::new(e),
            })?;
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
        fn on_request(&self, req: Request) -> BoxFuture<Result<Request>> {
            let log = self.log.clone();
            let name = self.name;
            Box::pin(async move {
                log.lock().unwrap().push(format!("req:{name}"));
                let mut req = req;
                let existing = req.headers.get("x-order").unwrap_or_default();
                req.headers.set("x-order", format!("{existing}{name}"))?;
                Ok(req)
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

        let req = block_on(registry.run_request_hooks(request())).unwrap();
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
            fn on_request(&self, req: Request) -> BoxFuture<Result<Request>> {
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
        let req = block_on(registry.run_request_hooks(request())).unwrap();
        assert_eq!(req.url.as_str(), "https://example.com/");
        let res = block_on(registry.run_response_hooks(response())).unwrap();
        assert_eq!(res.status, 200);
    }
}
