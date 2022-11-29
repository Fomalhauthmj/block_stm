use crate::{
    core::{Mergeable, Transaction, TransactionOutput, ValueBytes, VM},
    mvmemory::ReadResult,
    ParallelExecutor,
};

use super::{Ledger, TransferTransaction, TransferTransactionOutput};
impl Mergeable for usize {
    type DeltaOp = ();

    fn partial_mergeable() -> bool {
        false
    }

    fn partial_merge(_left: &Self::DeltaOp, _right: &Self::DeltaOp) -> Self::DeltaOp {
        unreachable!()
    }

    fn apply_delta(&self, _delta: &Self::DeltaOp) -> Self {
        unreachable!()
    }
}
impl Transaction for TransferTransaction {
    type Key = usize;

    type Value = usize;
}
impl ValueBytes for <TransferTransaction as Transaction>::Value {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_ne_bytes().to_vec())
    }

    fn deserialize(bytes: &[u8]) -> Self {
        let bytes: [u8; 8] = bytes.try_into().unwrap();
        usize::from_ne_bytes(bytes)
    }
}
impl TransactionOutput for TransferTransactionOutput {
    type T = TransferTransaction;

    fn get_write_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <Self::T as Transaction>::Value,
    )> {
        self.0.clone()
    }

    fn get_delta_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <<Self::T as Transaction>::Value as Mergeable>::DeltaOp,
    )> {
        vec![]
    }
}
struct ParallelVM<'a>(&'a Ledger);
impl<'a> VM for ParallelVM<'a> {
    type T = TransferTransaction;

    type Output = TransferTransactionOutput;

    type Error = ();

    type Parameter = &'a Ledger;

    fn new(argument: Self::Parameter) -> Self {
        Self(argument)
    }

    fn execute_transaction(
        &self,
        txn: &Self::T,
        view: &crate::mvmemory::MVMemoryView<
            <Self::T as Transaction>::Key,
            <Self::T as Transaction>::Value,
        >,
    ) -> Result<Self::Output, Self::Error> {
        let read = |k| match view.read(k) {
            ReadResult::Value(v) => Ok(*v),
            ReadResult::NotFound => Ok(*self.0.get(k).unwrap()),
            _ => unreachable!(),
        };
        let from_balance = read(&txn.from)?;
        let output = if from_balance >= txn.money {
            let to_balance = read(&txn.to)?;
            vec![
                (txn.from, from_balance - txn.money),
                (txn.to, to_balance + txn.money),
            ]
        } else {
            vec![]
        };
        Ok(TransferTransactionOutput(output))
    }
}
/// parallel execute txns
pub fn my_parallel_execute(
    txns: &Vec<TransferTransaction>,
    ledger: &Ledger,
    concurrency_level: usize,
) -> Vec<TransferTransactionOutput> {
    let pe = ParallelExecutor::<TransferTransaction, ParallelVM>::new(concurrency_level);
    let (outputs, _) = pe.execute_transactions(txns, ledger);
    outputs
}
