//! The per-request read buffer slots: the direct ByteBuffer handed to
//! each `UrlRequest.read` is backed by Rust memory that must stay alive,
//! unmoved, until Cronet has finished writing into it.
//!
//! A request has at most one read outstanding, so the slot holds exactly
//! the buffer of the latest read: issuing the next read replaces it (the
//! previous read has completed — its data was copied out in the
//! callback), and the terminal event releases it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;

/// Request id → the backing of its in-flight (or last completed) read.
pub(crate) struct ReadSlots {
    slots: Mutex<HashMap<u64, Pin<Box<[u8]>>>>,
}

impl ReadSlots {
    pub(crate) fn new() -> Self {
        ReadSlots {
            slots: Mutex::new(HashMap::new()),
        }
    }

    /// Allocates a `len`-byte buffer for `id`'s next read and starts the
    /// read on it via `start(ptr, len)`.
    ///
    /// The buffer goes into the slot *before* the read starts: the read
    /// can complete — and its callback issue the next read — before
    /// `start` returns (a read resumed off the callback thread), and a
    /// late insert would then replace and free that next read's buffer
    /// while Cronet writes into it. The lock is not held across `start`.
    pub(crate) fn issue<E>(
        &self,
        id: u64,
        len: usize,
        start: impl FnOnce(*mut u8, usize) -> Result<(), E>,
    ) -> Result<(), E> {
        let backing: Pin<Box<[u8]>> = Pin::from(vec![0u8; len].into_boxed_slice());
        let ptr = backing.as_ptr().cast_mut();
        self.slots.lock().unwrap().insert(id, backing);
        let result = start(ptr, len);
        if result.is_err() {
            // The read never started; drop the buffer unless a newer
            // one has already replaced it.
            let mut slots = self.slots.lock().unwrap();
            if slots.get(&id).is_some_and(|b| b.as_ptr() == ptr.cast_const()) {
                slots.remove(&id);
            }
        }
        result
    }

    /// Releases `id`'s buffer: the request has terminated, so Cronet will
    /// not touch it again.
    pub(crate) fn release(&self, id: u64) {
        self.slots.lock().unwrap().remove(&id);
    }

    #[cfg(test)]
    fn current(&self, id: u64) -> Option<*const u8> {
        self.slots.lock().unwrap().get(&id).map(|b| b.as_ptr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The resume race: a read issued off the callback thread (the body
    /// watcher resuming after a stall) can complete — and its callback
    /// issue the next read — before `issue` returns. The next read's
    /// buffer must still be the live one afterwards.
    #[test]
    fn a_read_completing_inside_start_keeps_the_next_buffer_alive() {
        let slots = ReadSlots::new();
        let mut next_ptr = None;
        slots
            .issue(1, 16, |_first, _| {
                // Cronet completes the first read at once; the callback
                // issues the next one.
                slots
                    .issue(1, 16, |second, _| {
                        next_ptr = Some(second.cast_const());
                        Ok::<(), ()>(())
                    })
                    .unwrap();
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(
            slots.current(1),
            next_ptr,
            "the in-flight read's buffer was replaced (and freed)"
        );
    }

    /// The common case: each read's buffer is in the slot while it runs.
    #[test]
    fn the_in_flight_buffer_is_held_until_replaced_or_released() {
        let slots = ReadSlots::new();
        let mut first = None;
        slots
            .issue(1, 16, |ptr, _| {
                first = Some(ptr.cast_const());
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(slots.current(1), first);
        slots.release(1);
        assert_eq!(slots.current(1), None);
    }

    /// A read that fails to start leaves no buffer behind.
    #[test]
    fn a_failed_start_leaves_no_slot() {
        let slots = ReadSlots::new();
        let result = slots.issue(1, 16, |_, _| Err("request already finished"));
        assert!(result.is_err());
        assert_eq!(slots.current(1), None);
    }
}
