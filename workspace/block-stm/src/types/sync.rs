use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool as stdAtomicBool, AtomicUsize as stdAtomicUsize, Ordering},
        Arc, Condvar as stdCondvar, Mutex as stdMutex, MutexGuard,
    },
};
/// AtomicUsize wrapper
pub struct AtomicUsize {
    inner: stdAtomicUsize,
}
impl AtomicUsize {
    pub fn new(v: usize) -> Self {
        Self {
            inner: stdAtomicUsize::new(v),
        }
    }
    pub fn increment(&self) -> usize {
        self.inner.fetch_add(1, Ordering::SeqCst)
    }
    pub fn decrement(&self) -> usize {
        self.inner.fetch_sub(1, Ordering::SeqCst)
    }
    pub fn load(&self) -> usize {
        self.inner.load(Ordering::SeqCst)
    }
}
impl Deref for AtomicUsize {
    type Target = stdAtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
/// AtomicBool wrapper
pub struct AtomicBool(stdAtomicBool);
impl AtomicBool {
    pub fn new(v: bool) -> Self {
        Self(stdAtomicBool::new(v))
    }
    pub fn load(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    pub fn store(&self, val: bool) {
        self.0.store(val, Ordering::SeqCst)
    }
}
/// Mutex wrapper
pub struct Mutex<T>(stdMutex<T>);
impl<T> Mutex<T> {
    pub fn new(t: T) -> Self {
        Self(stdMutex::new(t))
    }
    pub fn lock(&self) -> MutexGuard<T> {
        self.0.lock().expect("lock error")
    }
}
/// Condvar wrapper
#[derive(Clone, Default)]
pub struct Condvar {
    inner: Arc<(stdMutex<bool>, stdCondvar)>,
}
impl Condvar {
    pub fn new() -> Self {
        Self {
            inner: Arc::new((stdMutex::new(false), stdCondvar::new())),
        }
    }
    pub fn notify_one(&self) {
        let mut cond = self.inner.0.lock().expect("lock error");
        *cond = true;
        self.inner.1.notify_one();
    }
    pub fn wait(&self) {
        let mut cond = self.inner.0.lock().expect("lock error");
        while !*cond {
            cond = self.inner.1.wait(cond).expect("wait error");
        }
    }
}
