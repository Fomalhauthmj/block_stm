use std::time::Instant;

use aptos_aggregator::delta_change_set::DeltaOp;
use aptos_parallel_executor::{
    executor::{MVHashMapView, ParallelTransactionExecutor, ReadResult},
    task::{ExecutionStatus, ExecutorTask, ModulePath, Transaction, TransactionOutput},
};
use aptos_types::{access_path::AccessPath, write_set::TransactionWrite};

use crate::test_utils::BenchmarkInfo;

use super::{Ledger, TransferTransaction, TransferTransactionOutput};
#[derive(PartialEq, Eq, Clone, Hash, PartialOrd)]
pub struct KeyWrapper(usize);
impl From<usize> for KeyWrapper {
    fn from(inner: usize) -> Self {
        Self(inner)
    }
}
pub struct ValueWrapper(usize);
impl From<usize> for ValueWrapper {
    fn from(inner: usize) -> Self {
        Self(inner)
    }
}
impl ModulePath for KeyWrapper {
    fn module_path(&self) -> Option<AccessPath> {
        None
    }
}
impl TransactionWrite for ValueWrapper {
    fn extract_raw_bytes(&self) -> Option<Vec<u8>> {
        todo!()
    }
}
impl Transaction for TransferTransaction {
    type Key = KeyWrapper;
    type Value = ValueWrapper;
}
impl TransactionOutput for TransferTransactionOutput {
    type T = TransferTransaction;

    fn get_writes(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <Self::T as Transaction>::Value,
    )> {
        self.0
            .iter()
            .map(|(k, v)| (KeyWrapper(*k), ValueWrapper(*v)))
            .collect()
    }

    fn get_deltas(&self) -> Vec<(<Self::T as Transaction>::Key, DeltaOp)> {
        vec![]
    }

    fn skip_output() -> Self {
        Self(vec![])
    }
}
struct ParallelVM<'a>(&'a Ledger);
impl<'a> ExecutorTask for ParallelVM<'a> {
    type T = TransferTransaction;

    type Output = TransferTransactionOutput;

    type Error = ();

    type Argument = &'a Ledger;

    fn init(args: Self::Argument) -> Self {
        Self(args)
    }

    fn execute_transaction(
        &self,
        view: &MVHashMapView<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>,
        txn: &Self::T,
    ) -> ExecutionStatus<Self::Output, Self::Error> {
        #[cfg(feature = "benchmark")]
        std::thread::sleep(std::time::Duration::from_micros(100));

        let read = |key| match view.read(key) {
            ReadResult::Value(value) => (*value).0,
            ReadResult::None => *self.0.get(&key.0).expect("get error"),
            _ => unreachable!(),
        };
        let txn_from = txn.from.into();
        let from_balance = read(&txn_from);
        let output = if from_balance >= txn.money {
            let to_balance = read(&txn.to.into());
            vec![
                (txn.from, from_balance - txn.money),
                (txn.to, to_balance + txn.money),
            ]
        } else {
            vec![]
        };
        ExecutionStatus::Success(TransferTransactionOutput(output))
    }
}
///
pub fn aptos_parallel_execute(
    txns: &Vec<TransferTransaction>,
    ledger: &Ledger,
    concurrency_level: usize,
) -> (Vec<TransferTransactionOutput>, BenchmarkInfo) {
    let total = Instant::now();
    let pe = ParallelTransactionExecutor::<TransferTransaction, ParallelVM>::new(concurrency_level);
    if let Ok((outputs, _, execute, collect)) =
        pe.execute_transactions_parallel_benchmark(ledger, txns)
    {
        (
            outputs,
            BenchmarkInfo {
                total_time: total.elapsed(),
                execute_time: Some(execute),
                collect_time: Some(collect.elapsed()),
            },
        )
    } else {
        unreachable!()
    }
}
