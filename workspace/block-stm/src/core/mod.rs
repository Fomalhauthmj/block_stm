use crate::mvmemory::MVMapView;
use std::{fmt::Debug, hash::Hash};

/// TODO: more specific trait bounds,and explain reasons.

/// abstract transaction
pub trait Transaction: Send + Sync + 'static {
    /// abstract memory address accessed by transaction
    type Key: Eq + Hash + Send + Sync + Clone + Debug;
    /// abstract memory data read/written by transaction
    type Value: Eq + Send + Sync + Mergeable + Debug;
}
/// trait used to support delta
pub trait Mergeable: Eq {
    /// abstract delta operation
    type Delta: Send + Sync + Copy + Eq;
    /// whether or not support partial merge
    fn partial_mergeable() -> bool;
    /// merge two delta to a new delta
    fn partial_merge(left: &Self::Delta, right: &Self::Delta) -> Self::Delta;
    /// apply delta to self,produce a new Self
    fn apply_delta(&self, delta: &Self::Delta) -> Self;
}
/// write set of transaction output
pub type WriteSet<K, V> = Vec<(K, V)>;
/// delta set of transaction output
pub type DeltaSet<K, D> = Vec<(K, D)>;
/// abstract transaction execution output
pub trait TransactionOutput: Send + Sync {
    /// associate transaction type
    type T: Transaction;
    /// get the write set of output,which won't overlap with delta set
    fn get_write_set(
        &self,
    ) -> WriteSet<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>;
    /// get the delta set of output,which won't overlap with write set
    fn get_delta_set(
        &self,
    ) -> DeltaSet<
        <Self::T as Transaction>::Key,
        <<Self::T as Transaction>::Value as Mergeable>::Delta,
    >;
}
/// abstract virtual machine for transaction execution
pub trait VM: Sync + Sized {
    /// associate transaction type
    type T: Transaction;
    /// transaction execution output
    type Output: TransactionOutput<T = Self::T>;
    /// transaction execution error
    type Error;
    /// parameter passed to VM,such as state view,which needed by transaction execution
    type Parameter: Send + Sync + Clone;
    /// create a new virtual machine instance
    fn new(parameter: Self::Parameter) -> Self;
    /// execute single transaction with given mvmemory view
    fn execute_transaction(
        &self,
        txn: &Self::T,
        view: &MVMapView<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>,
    ) -> Result<Self::Output, Self::Error>;
}
