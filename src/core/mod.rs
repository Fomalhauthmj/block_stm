use std::{fmt::Debug, hash::Hash};

use crate::mvmemory::MVMemoryView;
/// trait used to serialize/deserialize Value
pub trait ValueBytes {
    /// serialize `Self` to `Vec<u8>`,`None` indicates deletion.
    fn serialize(&self) -> Option<Vec<u8>>;
    /// deserialize `Vec<u8>` to `Self`
    fn deserialize(bytes: &[u8]) -> Self;
}
/// trait used to support deltaop
pub trait Mergeable: Eq {
    /// deltaop
    type DeltaOp: Send + Sync + Copy + Eq;
    ///
    fn partial_mergeable() -> bool;
    ///
    fn partial_merge(left: &Self::DeltaOp, right: &Self::DeltaOp) -> Self::DeltaOp;
    ///
    fn apply_delta(&self, delta: &Self::DeltaOp) -> Self;
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
    type Key: Eq + Hash + Send + Sync + Clone + Debug;
    /// memory value type read/written by transaction
    ///
    /// `Send + Sync` needed by rayon
    ///
    /// `ValueBytes` needed by mvmemory snapshot
    ///
    type Value: Eq + Send + Sync + ValueBytes + Mergeable + Debug;
}
/// transaction output,which used to get transaction's write set
#[allow(clippy::type_complexity)]
pub trait TransactionOutput: Sync + Send {
    /// transaction type
    type T: Transaction;
    /// get write set of transaction
    fn get_write_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <Self::T as Transaction>::Value,
    )>;
    ///
    fn get_delta_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <<Self::T as Transaction>::Value as Mergeable>::DeltaOp,
    )>;
}
/// execution engine
///
/// `Sync` needed by rayon
pub trait VM: Sync {
    /// transaction type
    type T: Transaction;
    /// execution output
    type Output: TransactionOutput<T = Self::T>;
    /// execution error
    type Error;
    /// parameter needed by creating new execution engine instance
    type Parameter: Send + Sync + Clone;
    /// create new execution engine instance
    fn new(parameter: Self::Parameter) -> Self;
    /// execute single transaction with given mvmemory view
    fn execute_transaction(
        &self,
        txn: &Self::T,
        view: &MVMemoryView<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>,
    ) -> Result<Self::Output, Self::Error>;
}
