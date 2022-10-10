/// abstract traits
use std::{fmt::Debug, hash::Hash};

use crate::{executor::ExecutorView, mvmemory::MVMemoryError, types::TransactionIndex};

// TODO:
//  less number of trait constraints
//  more accurate trait constraints
/// abstract transaction type
pub trait Transaction: Sync {
    /// memory location type accessed by transaction
    type Key: Eq + Hash + Copy + Sync + Send + Debug;
    /// memory value type read/written by transaction
    type Value: Clone + Sync + Send + Debug;
}
/// abstract storage type
pub trait Storage: Send + Clone + Sync {
    /// abstract transaction type
    type T: Transaction;
    /// read from storage
    fn read(&self, key: &<Self::T as Transaction>::Key) -> <Self::T as Transaction>::Value;
}
/// abstract error type
pub trait IsReadError {
    /// is read error
    fn is_read_error(&self) -> bool;
    /// get blocking txn idx
    fn get_blocking_txn_idx(&self) -> Option<TransactionIndex>;
}
/// abstract execution engine
pub trait VM: Sync {
    /// transaction type
    type T: Transaction;
    /// execution output
    type Output;
    /// execution error
    type Error: IsReadError + From<MVMemoryError>;
    /// storage type
    type S: Storage<T = Self::T>;
    /// create a new execution engine instance
    fn new() -> Self;
    /// execute single transaction
    fn execute(
        &self,
        txn: &Self::T,
        view: &mut ExecutorView<Self::T, Self::S>,
    ) -> Result<Self::Output, Self::Error>;
}
