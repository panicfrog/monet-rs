//! Cooperative cancellation.
//!
//! A [`CancelToken`] is a cheap, cloneable flag shared between the caller and
//! a transport. Frontends cancel from JS; the transport observes it (by
//! polling or awaiting [`CancelToken::cancelled`]) and maps it to the native
//! cancel call — Cronet's `request.cancel()`, URLSession's `task.cancel()`,
//! or closing a rustls stream.

use std::collections::HashMap;
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
    /// One slot per live [`Cancelled`] future. A token is routinely
    /// awaited by several parties at once — the frontend's pump task and
    /// a transport's feeder/watcher threads — so a single shared slot
    /// would let the last registration erase the others, and `cancel()`
    /// would wake only that one (a lost wakeup: the abort is observed
    /// late, or never).
    waiters: Mutex<Waiters>,
}

#[derive(Default)]
struct Waiters {
    next_key: u64,
    wakers: HashMap<u64, Waker>,
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
        // Taken under the lock, woken outside it: a woken waiter may
        // re-poll (and lock) on this thread.
        let wakers = std::mem::take(&mut self.state.waiters.lock().unwrap().wakers);
        for waker in wakers.into_values() {
            waker.wake();
        }
    }

    /// Returns `true` once [`CancelToken::cancel`] has been called.
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// A future that resolves when the token is cancelled.
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            key: None,
        }
    }
}

/// Future returned by [`CancelToken::cancelled`].
pub struct Cancelled<'a> {
    token: &'a CancelToken,
    /// This future's slot in the waiter map, assigned on the first
    /// pending poll and released on drop.
    key: Option<u64>,
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }
        let token = self.token;
        let mut waiters = token.state.waiters.lock().unwrap();
        // Re-check under the lock so a cancel() racing with our registration
        // cannot leave us registered-but-lost.
        if token.is_cancelled() {
            return Poll::Ready(());
        }
        let key = match self.key {
            Some(key) => key,
            None => {
                let key = waiters.next_key;
                waiters.next_key += 1;
                key
            }
        };
        waiters.wakers.insert(key, cx.waker().clone());
        drop(waiters);
        self.key = Some(key);
        Poll::Pending
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        // A future dropped before the cancel (the other arm of a race won)
        // must not leave its waker behind.
        if let Some(key) = self.key {
            self.token.state.waiters.lock().unwrap().wakers.remove(&key);
        }
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

    /// Regression: concurrent waiters on one token (a frontend pump plus
    /// a transport's feeder thread) must ALL wake — a single shared
    /// waker slot let the later registration erase the earlier one.
    #[test]
    fn cancel_wakes_every_concurrent_waiter() {
        let token = CancelToken::new();
        let waiters: Vec<_> = (0..3)
            .map(|_| {
                let token = token.clone();
                std::thread::spawn(move || block_on(token.cancelled()))
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(50));
        token.cancel();
        for waiter in waiters {
            waiter.join().unwrap();
        }
    }

    /// A dropped (never-cancelled) waiter releases its slot.
    #[test]
    fn dropped_waiter_releases_its_slot() {
        let token = CancelToken::new();
        let mut fut = Box::pin(token.cancelled());
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert_eq!(token.state.waiters.lock().unwrap().wakers.len(), 1);
        drop(fut);
        assert!(token.state.waiters.lock().unwrap().wakers.is_empty());
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
