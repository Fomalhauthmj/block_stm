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

#[derive(Clone, Debug)]
pub struct TransferTransaction {
    from: usize,
    to: usize,
    money: usize,
}
impl Transaction for TransferTransaction {
    type Key = usize;

    type Value = usize;
}

#[derive(Clone, PartialEq, Debug)]
pub struct Ledger(HashMap<usize, usize>);
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
        let from_balance = view.read(&txn.from)?;
        if from_balance >= txn.money {
            let to_balance = view.read(&txn.to)?;
            view.write(txn.from, from_balance - txn.money);
            view.write(txn.to, to_balance + txn.money);
        }
        Ok(())
    }
}
/// Note:
/// Here,our purpose is ensuring the outcome of `parallel_execute` is same with outcome of `sequential_execute`.
/// so for convenient, we update view directly,which is unfair for the benchmark performance.
///
/// sequential execute txns,update view directly.
pub fn sequential_execute(txns: Vec<TransferTransaction>, mut view: Ledger) -> Ledger {
    for txn in txns {
        let from_balance = view.0.get(&txn.from).unwrap();
        if from_balance >= &txn.money {
            let to_balance = *view.0.get(&txn.to).unwrap();
            view.0.insert(txn.from, from_balance - txn.money);
            view.0.insert(txn.to, to_balance + txn.money);
        }
    }
    view
}
/// parallel execute txns,apply changeset to view directly.
pub fn parallel_execute(txns: Vec<TransferTransaction>, mut view: Ledger) -> Ledger {
    let change_set =
        ParallelExecutor::<TransferTransaction, Ledger, TransferVM>::execute_transactions(
            txns,
            view.clone(),
        );
    for (k, v) in change_set {
        view.0.insert(k, v);
    }
    view
}
/// generate a ledger and random txns with the given parameters
pub fn generate_ledger_and_txns(
    accounts_num: usize,
    init_balance: usize,
    txns_num: usize,
    min_txn_money: usize,
    max_txn_money: usize,
) -> (Ledger, Vec<TransferTransaction>) {
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
    (ledger, txns)
}
