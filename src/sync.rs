use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool as stdAtomicBool, AtomicUsize as stdAtomicUsize, Ordering},
        Mutex as stdMutex, MutexGuard,
    },
};

use tracing::instrument;

use crate::rayon_trace;

pub struct AtomicUsize {
    name: String,
    inner: stdAtomicUsize,
}
impl AtomicUsize {
    pub fn new(name: &str, v: usize) -> Self {
        Self {
            name: name.into(),
            inner: stdAtomicUsize::new(v),
        }
    }
    #[instrument(skip(self))]
    pub fn increment(&self) -> usize {
        let prev = self.inner.fetch_add(1, Ordering::SeqCst);
        rayon_trace!("{} increment", self.name);
        prev
    }
    #[instrument(skip(self))]
    pub fn decrement(&self) -> usize {
        let prev = self.inner.fetch_sub(1, Ordering::SeqCst);
        rayon_trace!("{} decrement", self.name);
        prev
    }
    #[instrument(skip(self))]
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

pub struct AtomicBool(stdAtomicBool);
impl AtomicBool {
    pub fn new(v: bool) -> Self {
        Self(stdAtomicBool::new(v))
    }
    #[instrument(skip(self))]
    pub fn load(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    #[instrument(skip(self))]
    pub fn store(&self, val: bool) {
        self.0.store(val, Ordering::SeqCst)
    }
}

pub struct Mutex<T>(stdMutex<T>);
impl<T> Mutex<T> {
    pub fn new(t: T) -> Self {
        Self(stdMutex::new(t))
    }
    pub fn lock(&self) -> MutexGuard<T> {
        self.0.lock().expect("lock error")
    }
}
