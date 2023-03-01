use super::types::{Ledger, TransferTxn, TransferTxnOutput};
use block_stm::ParallelExecutor;
use std::sync::Arc;

mod wrapper {
    use std::sync::Arc;

    use anyhow::anyhow;
    use block_stm::{
        DeltaSet, MVMapView, Mergeable, ReadResult, Transaction, TransactionOutput, WriteSet, VM,
    };

    use crate::simulated::types::{Ledger, TransferTxn, TransferTxnOutput};

    #[derive(PartialEq, Eq, Debug)]
    pub struct ValueWrapper(usize);
    impl From<usize> for ValueWrapper {
        fn from(value: usize) -> Self {
            ValueWrapper(value)
        }
    }
    impl Mergeable for ValueWrapper {
        type Delta = ();

        fn partial_mergeable() -> bool {
            false
        }

        fn partial_merge(_left: &Self::Delta, _right: &Self::Delta) -> Self::Delta {
            unreachable!()
        }

        fn apply_delta(&self, _delta: &Self::Delta) -> Self {
            unreachable!()
        }
    }
    impl Transaction for TransferTxn {
        type Key = usize;

        type Value = ValueWrapper;
    }
    impl TransactionOutput for TransferTxnOutput {
        type T = TransferTxn;

        fn get_write_set(
            &self,
        ) -> WriteSet<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value> {
            self.0.iter().map(|(k, v)| (*k, (*v).into())).collect()
        }

        fn get_delta_set(
            &self,
        ) -> DeltaSet<
            <Self::T as Transaction>::Key,
            <<Self::T as Transaction>::Value as Mergeable>::Delta,
        > {
            vec![]
        }
    }
    pub struct VMWrapper(Arc<Ledger>);
    impl VM for VMWrapper {
        type T = TransferTxn;

        type Output = TransferTxnOutput;

        type Error = anyhow::Error;

        type Parameter = Arc<Ledger>;

        fn new(parameter: Self::Parameter) -> Self {
            Self(parameter)
        }

        fn execute_transaction(
            &self,
            txn: &Self::T,
            view: &MVMapView<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>,
        ) -> Result<Self::Output, Self::Error> {
            let read = |k| match view.read(k) {
                ReadResult::Value(v) => Ok(v.0),
                ReadResult::NotFound => Ok(*self.0.get(k).unwrap()),
                _ => Err(anyhow!("unreasonable read result")),
            };
            let from_balance = read(&txn.from)?;
            if from_balance >= txn.money {
                let to_balance = read(&txn.to)?;
                let output = TransferTxnOutput(vec![
                    (txn.from, from_balance - txn.money),
                    (txn.to, to_balance + txn.money),
                ]);
                Ok(output)
            } else {
                Err(anyhow!("balance not enough"))
            }
        }
    }
}

/// parallel execute simulated txns
pub fn simulated_parallel_execute(
    txns: Vec<TransferTxn>,
    ledger: &Ledger,
    concurrency_level: usize,
) -> Vec<TransferTxnOutput> {
    use self::wrapper::VMWrapper;
    let ledger = Arc::new(ledger.clone());
    let executor = ParallelExecutor::<TransferTxn, VMWrapper>::new(concurrency_level);
    let (outputs, _) = executor.execute_transactions(txns, ledger);
    outputs
}
