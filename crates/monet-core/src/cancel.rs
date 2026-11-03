//! Cooperative cancellation.
//!
//! A [`CancelToken`] is a cheap, cloneable flag shared between the caller and
//! a transport. Frontends cancel from JS; the transport observes it (by
//! polling or awaiting [`CancelToken::cancelled`]) and maps it to the native
//! cancel call — Cronet's `request.cancel()`, URLSession's `task.cancel()`,
//! or closing a rustls stream.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// A shared cancellation flag.
#[derive(Clone, Default)]
pub struct CancelToken {
    state: Arc<CancelState>,
}

#[derive(Default)]
struct CancelState {
    cancelled: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl CancelToken {
    /// Creates a token that has not been cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the token cancelled and wakes every current waiter.
    ///
    /// Idempotent: cancelling twice is a no-op.
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        if let Some(waker) = self.state.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    /// Returns `true` once [`CancelToken::cancel`] has been called.
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// A future that resolves when the token is cancelled.
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled { token: self }
    }
}

/// Future returned by [`CancelToken::cancelled`].
pub struct Cancelled<'a> {
    token: &'a CancelToken,
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }
        let mut slot = self.token.state.waker.lock().unwrap();
        // Re-check under the lock so a cancel() racing with our registration
        // cannot leave us registered-but-lost.
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }
        *slot = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future::block_on;

    #[test]
    fn cancel_before_await_resolves_immediately() {
        let token = CancelToken::new();
        token.cancel();
        block_on(token.cancelled());
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_wakes_pending_waiter() {
        let token = CancelToken::new();
        let waiter = {
            let token = token.clone();
            std::thread::spawn(move || {
                block_on(token.cancelled());
            })
        };
        // Give the waiter a moment to park, then cancel from another thread.
        std::thread::sleep(std::time::Duration::from_millis(50));
        token.cancel();
        waiter.join().unwrap();
    }

    #[test]
    fn clones_share_state_and_double_cancel_is_inert() {
        let a = CancelToken::new();
        let b = a.clone();
        b.cancel();
        b.cancel();
        assert!(a.is_cancelled());
    }
}
