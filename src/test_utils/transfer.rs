use std::collections::HashMap;

use crate::{
    executor::ExecutorView,
    mvmemory::MVMemoryError,
    traits::{IsReadError, Storage, Transaction, VM},
    types::TransactionIndex,
    ParallelExecutor,
};
use rand::{distributions::Uniform, prelude::Distribution};
use thiserror::Error;
/// simple transfer transaction for test and benchmark
#[derive(Clone, Debug)]
pub struct TransferTransaction {
    /// transfer money from
    pub from: usize,
    /// transfer money to
    pub to: usize,
    /// transfer money
    pub money: usize,
}
impl Transaction for TransferTransaction {
    type Key = usize;

    type Value = usize;
}
/// simple hashmap ledger
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Ledger(pub HashMap<usize, usize>);
impl Storage for Ledger {
    type T = TransferTransaction;

    fn read(&self, key: &<Self::T as Transaction>::Key) -> <Self::T as Transaction>::Value {
        *self.0.get(key).expect("storage read error")
    }
}

#[derive(Debug, Error)]
pub enum TransferError {
    #[error(transparent)]
    MVMemory(#[from] MVMemoryError),
}
impl IsReadError for TransferError {
    fn is_read_error(&self) -> bool {
        if let Self::MVMemory(MVMemoryError::ReadError(_)) = self {
            return true;
        }
        false
    }

    fn get_blocking_txn_idx(&self) -> Option<TransactionIndex> {
        if let Self::MVMemory(MVMemoryError::ReadError(idx)) = self {
            return Some(*idx);
        }
        None
    }
}
/// simple transfer vm
pub struct TransferVM;
impl VM for TransferVM {
    type T = TransferTransaction;

    type Output = ();

    type Error = TransferError;

    type S = Ledger;

    fn new() -> Self {
        Self
    }

    fn execute(
        &self,
        txn: &Self::T,
        view: &mut ExecutorView<Self::T, Self::S>,
    ) -> Result<Self::Output, Self::Error> {
        #[cfg(feature = "benchmark")]
        std::thread::sleep(std::time::Duration::from_millis(1));
        let from_balance = view.read(&txn.from)?;
        if from_balance >= txn.money {
            let to_balance = view.read(&txn.to)?;
            view.write(txn.from, from_balance - txn.money);
            view.write(txn.to, to_balance + txn.money);
        }
        Ok(())
    }
}
/// sequential execute txns,update view directly.
pub fn sequential_execute(txns: &Vec<TransferTransaction>, ledger: &mut Ledger) {
    for txn in txns {
        #[cfg(feature = "benchmark")]
        std::thread::sleep(std::time::Duration::from_millis(1));
        let from_balance = ledger.0.get(&txn.from).unwrap();
        if from_balance >= &txn.money {
            let to_balance = *ledger.0.get(&txn.to).unwrap();
            ledger.0.insert(txn.from, from_balance - txn.money);
            ledger.0.insert(txn.to, to_balance + txn.money);
        }
    }
}
/// parallel execute txns,apply changeset to view directly.
pub fn parallel_execute(
    txns: &Vec<TransferTransaction>,
    ledger: &mut Ledger,
    concurrency_level: usize,
) {
    let pe = ParallelExecutor::<TransferTransaction, Ledger, TransferVM>::new(concurrency_level);
    let changeset = pe.execute_transactions(txns, ledger);
    // TODO: can we use rayon par iter to optimize?
    for (k, v) in changeset {
        ledger.0.insert(k, v);
    }
}
/// generate a ledger and random txns with the given parameters
pub fn generate_ledger_and_txns(
    accounts_num: usize,
    init_balance: usize,
    txns_num: usize,
    min_txn_money: usize,
    max_txn_money: usize,
) -> (Vec<TransferTransaction>, Ledger) {
    let mut ledger = Ledger(HashMap::new());
    (0..accounts_num).into_iter().for_each(|account| {
        let _ = ledger.0.insert(account, init_balance);
    });
    let mut txns = Vec::new();
    let mut rng = rand::thread_rng();
    let account_distribution = Uniform::from(0..accounts_num);
    let money_distribution = Uniform::from(min_txn_money..=max_txn_money);
    while txns.len() < txns_num {
        let from = account_distribution.sample(&mut rng);
        let to = account_distribution.sample(&mut rng);
        let money = money_distribution.sample(&mut rng);
        if from != to {
            txns.push(TransferTransaction { from, to, money });
        }
    }
    (txns, ledger)
}
