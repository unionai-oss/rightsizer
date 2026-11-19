//! The process-wide tokio runtime (SPEC §10.3) and its bridge to
//! `pyo3-async-runtimes`.
//!
//! - **One runtime per process**, created lazily on first use.
//! - **Fork-safe.** The runtime remembers the pid it was created in. After
//!   `fork()` the child has none of the parent's worker threads, so the first
//!   call in the child sees a different pid, leaks the parent's runtime
//!   (dropping it would try to join threads that do not exist) and builds a
//!   fresh one. Engines do the same with their own pid check
//!   ([`crate::engine`]).
//! - **`block_on` from anywhere.** A Python estimator may call back into
//!   rightsizer from a blocking-pool thread, which is inside the runtime's
//!   context, where `Runtime::block_on` would panic. [`block_on`] then spawns
//!   the future on the runtime and waits for it on the current thread.
//! - [`PyRt`] implements `pyo3_async_runtimes::generic::{Runtime, ContextExt}`
//!   over this runtime, so `future_into_py` (awaitables for `adecide`) and
//!   `into_future_with_locals` (async Python estimators on the caller's event
//!   loop) use it instead of `pyo3-async-runtimes`'s own global runtime, which
//!   is not fork-safe.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicPtr, Ordering};

use futures::FutureExt;
use pyo3_async_runtimes::TaskLocals;
use pyo3_async_runtimes::generic::{ContextExt, JoinError, Runtime as GenericRuntime};
use tokio::runtime::Runtime;

/// A runtime and the process it belongs to. Leaked: never freed, so a
/// `&'static` to it stays valid for the life of the process.
struct Slot {
    pid: u32,
    rt: Runtime,
}

static SLOT: AtomicPtr<Slot> = AtomicPtr::new(std::ptr::null_mut());
/// Serializes creation only (the slow path: once per process).
static CREATE: Mutex<()> = Mutex::new(());

fn current() -> Option<&'static Slot> {
    let p = SLOT.load(Ordering::Acquire);
    if p.is_null() {
        return None;
    }
    // SAFETY: every non-null pointer stored in SLOT comes from `Box::leak`
    // and is never freed, so it is valid for 'static.
    let s: &'static Slot = unsafe { &*p };
    (s.pid == std::process::id()).then_some(s)
}

fn build() -> Runtime {
    let workers = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2).clamp(1, 4);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("rightsizer-rt")
        .enable_all()
        .build()
        .expect("building the rightsizer tokio runtime")
}

/// The process-wide runtime, created lazily and re-created after `fork()`.
pub fn runtime() -> &'static Runtime {
    if let Some(s) = current() {
        return &s.rt;
    }
    let _g = CREATE.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(s) = current() {
        return &s.rt;
    }
    // Either the first call in this process or the first after a fork; the
    // parent's slot (if any) is simply abandoned.
    let slot: &'static Slot = Box::leak(Box::new(Slot { pid: std::process::id(), rt: build() }));
    SLOT.store(slot as *const Slot as *mut Slot, Ordering::Release);
    &slot.rt
}

/// The pid the current runtime was created in (tests).
pub fn runtime_pid() -> Option<u32> {
    current().map(|s| s.pid)
}

/// A panic payload as text.
pub fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".into())
}

/// Run `fut` to completion on the process runtime and wait for it on this
/// thread. Panics inside `fut` come back as `Err(message)`.
pub fn block_on<F>(fut: F) -> Result<F::Output, String>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let rt = runtime();
    if tokio::runtime::Handle::try_current().is_ok() {
        // Inside a runtime context (e.g. a Python estimator's blocking-pool
        // thread calling back into rightsizer): `Runtime::block_on` would
        // panic, so run the future as a task and park this thread on it.
        let h = rt.spawn(fut);
        return futures::executor::block_on(h).map_err(|e| if e.is_panic() { panic_message(&*e.into_panic()) } else { e.to_string() });
    }
    std::panic::catch_unwind(AssertUnwindSafe(|| rt.block_on(AssertUnwindSafe(fut).catch_unwind())))
        .map_err(|p| panic_message(&*p))?
        .map_err(|p| panic_message(&*p))
}

tokio::task_local! {
    static LOCALS: TaskLocals;
}

/// `pyo3-async-runtimes` over the fork-safe runtime.
pub struct PyRt;

/// A tokio join error for `pyo3-async-runtimes`.
pub struct PyRtJoinError(tokio::task::JoinError);

impl JoinError for PyRtJoinError {
    fn is_panic(&self) -> bool {
        self.0.is_panic()
    }
    fn into_panic(self) -> Box<dyn std::any::Any + Send + 'static> {
        self.0.into_panic()
    }
}

impl GenericRuntime for PyRt {
    type JoinError = PyRtJoinError;
    type JoinHandle = Pin<Box<dyn Future<Output = Result<(), PyRtJoinError>> + Send>>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let h = runtime().spawn(fut);
        Box::pin(async move { h.await.map_err(PyRtJoinError) })
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let h = runtime().spawn_blocking(f);
        Box::pin(async move { h.await.map_err(PyRtJoinError) })
    }
}

impl ContextExt for PyRt {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        Box::pin(LOCALS.scope(locals, fut))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        LOCALS.try_with(|l| l.clone()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_runtime_per_process() {
        let a = runtime() as *const Runtime;
        let b = runtime() as *const Runtime;
        assert_eq!(a, b);
        assert_eq!(runtime_pid(), Some(std::process::id()));
    }

    #[test]
    fn block_on_outside_and_inside_the_runtime() {
        assert_eq!(block_on(async { 1 + 1 }).unwrap(), 2);
        // From a blocking-pool thread (inside the runtime's context).
        let r = block_on(async { tokio::task::spawn_blocking(|| block_on(async { 40 + 2 })).await.unwrap() }).unwrap();
        assert_eq!(r.unwrap(), 42);
    }

    #[test]
    fn block_on_catches_panics() {
        let e = block_on(async {
            if std::hint::black_box(true) {
                panic!("boom");
            }
            1
        })
        .unwrap_err();
        assert!(e.contains("boom"), "{e}");
    }
}
