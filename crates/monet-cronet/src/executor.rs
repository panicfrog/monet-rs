//! The pass-through [`Cronet_Executor`] implementation.
//!
//! Cronet requires an executor for every request; callbacks arrive as
//! runnables handed to it. Running them inline means our callback code
//! executes on whatever thread Cronet calls from (its network thread), with
//! no extra hop — every piece of per-request state must therefore be `Send`.

use monet_cronet_sys as sys;

pub(crate) struct PassThroughExecutor {
    ptr: sys::Cronet_ExecutorPtr,
}

impl PassThroughExecutor {
    pub(crate) fn new() -> Self {
        let ptr = unsafe { sys::Cronet_Executor_CreateWith(Some(execute)) };
        Self { ptr }
    }

    pub(crate) fn ptr(&self) -> sys::Cronet_ExecutorPtr {
        self.ptr
    }
}

unsafe extern "C" fn execute(_self: sys::Cronet_ExecutorPtr, command: sys::Cronet_RunnablePtr) {
    unsafe {
        sys::Cronet_Runnable_Run(command);
        sys::Cronet_Runnable_Destroy(command);
    }
}

impl Drop for PassThroughExecutor {
    fn drop(&mut self) {
        unsafe { sys::Cronet_Executor_Destroy(self.ptr) };
    }
}

// Safety: the executor carries no state — it runs runnables inline on the
// calling thread — so sharing it across threads is sound.
unsafe impl Send for PassThroughExecutor {}
unsafe impl Sync for PassThroughExecutor {}
