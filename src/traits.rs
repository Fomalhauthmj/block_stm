use std::{fmt::Debug, hash::Hash};

use crate::{executor::ExecutorView, mvmemory::MVMemoryError, types::TransactionIndex};
/// trait used to convert `Vec<u8>` to `Self` or reverse
pub trait ValueBytes {
    /// from `Vec<u8>` to `Self`
    fn from_raw_bytes(bytes: Vec<u8>) -> Self;
    /// from `Self` to `Vec<u8>`
    fn to_raw_bytes(&self) -> Vec<u8>;
}
/// transaction type
///
/// `Sync` needed by rayon
pub trait Transaction: Sync {
    /// memory location accessed by transaction
    ///
    /// `Eq + Hash` needed by dashmap
    ///
    /// `Clone` needed by collect
    ///
    /// `Send + Sync` needed by rayon
    ///
    /// `Debug` needed by tracing::instrument
    type Key: Eq + Hash + Clone + Send + Sync + Debug;
    /// memory value type read/written by transaction
    ///
    /// `Send + Sync` needed by rayon
    ///
    /// `ValueBytes` needed by mvmemory snapshot
    ///
    /// `Debug` needed by tracing::instrument
    type Value: Send + Sync + ValueBytes + Debug;
}
/// storage type
///
/// `Sync` needed by rayon
pub trait Storage: Sync {
    /// transaction type
    type T: Transaction;
    /// read from storage
    fn read(&self, key: &<Self::T as Transaction>::Key) -> <Self::T as Transaction>::Value;
}
/// error type
pub trait IsReadError {
    /// is read error
    fn is_read_error(&self) -> bool;
    /// get blocking txn idx
    fn get_blocking_txn_idx(&self) -> Option<TransactionIndex>;
}
/// execution engine
///
/// `Sync` needed by rayon
pub trait VM: Sync {
    /// transaction type
    type T: Transaction;
    /// execution output
    type Output;
    /// execution error
    type Error: IsReadError + From<MVMemoryError>;
    /// storage type
    type S: Storage<T = Self::T>;
    /// create execution engine instance
    fn new() -> Self;
    /// execute single transaction
    fn execute(
        &self,
        txn: &Self::T,
        view: &mut ExecutorView<Self::T, Self::S>,
    ) -> Result<Self::Output, Self::Error>;
}
