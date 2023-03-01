/// synchronization primitives wrappers
mod sync;
pub use sync::{AtomicBool, AtomicUsize, Condvar, Mutex};

/// transaction index (start from 0)
pub type TxnIndex = usize;
/// incarnation number (start from 0)
pub type Incarnation = usize;
/// version
pub type Version = (TxnIndex, Incarnation);
