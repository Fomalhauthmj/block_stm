use std::{fmt::Debug, hash::Hash};

use crate::mvmemory::MVMemoryView;
/// trait used to serialize/deserialize Value
pub trait ValueBytes {
    /// serialize `Self` to `Vec<u8>`,`None` indicates deletion.
    fn serialize(&self) -> Option<Vec<u8>>;
    /// deserialize `Vec<u8>` to `Self`
    fn deserialize(bytes: &[u8]) -> Self;
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
    type Key: Eq + Hash + Clone + Send + Sync + Debug;
    /// memory value type read/written by transaction
    ///
    /// `Send + Sync` needed by rayon
    ///
    /// `ValueBytes` needed by mvmemory snapshot
    ///
    type Value: Send + Sync + ValueBytes + Debug;
}
/// transaction output,which used to get transaction's write set
#[allow(clippy::type_complexity)]
pub trait TransactionOutput {
    /// transaction type
    type T: Transaction;
    /// get write set of transaction
    fn get_write_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <Self::T as Transaction>::Value,
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
