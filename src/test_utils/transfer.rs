use std::collections::HashMap;

use crate::{
    core::{Transaction, TransactionOutput, ValueBytes, VM},
    mvmemory::ReadResult,
    ParallelExecutor,
};
use rand::{distributions::Uniform, prelude::Distribution};
/// transfer transaction used for tests and benches
#[derive(Debug)]
pub struct TransferTransaction {
    /// transfer money from
    pub from: usize,
    /// transfer money to
    pub to: usize,
    /// transfer money amount
    pub money: usize,
}
impl Transaction for TransferTransaction {
    type Key = usize;

    type Value = usize;
}
impl ValueBytes for usize {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_ne_bytes().to_vec())
    }

    fn deserialize(bytes: &[u8]) -> Self {
        let bytes: [u8; 8] = bytes.try_into().unwrap();
        usize::from_ne_bytes(bytes)
    }
}
/// hashmap ledger
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Ledger(pub HashMap<usize, usize>);
pub struct TransferTransactionOutput(Vec<(usize, usize)>);
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
}
/// transfer vm
pub struct TransferVM {
    ledger: Ledger,
}
impl VM for TransferVM {
    type T = TransferTransaction;

    type Output = TransferTransactionOutput;

    type Error = ();

    type Parameter = Ledger;

    fn new(argument: Self::Parameter) -> Self {
        Self { ledger: argument }
    }

    fn execute_transaction(
        &self,
        txn: &Self::T,
        view: &crate::mvmemory::MVMemoryView<
            <Self::T as Transaction>::Key,
            <Self::T as Transaction>::Value,
        >,
    ) -> Result<Self::Output, Self::Error> {
        #[cfg(feature = "simulated_test_utils")]
        std::thread::sleep(std::time::Duration::from_micros(100));
        let read = |k| match view.read(k) {
            ReadResult::Value(v) => Ok(*v),
            ReadResult::NotFound => Ok(*self.ledger.0.get(k).unwrap()),
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
/// sequential execute txns,update ledger directly.
pub fn sequential_execute(txns: &Vec<TransferTransaction>, ledger: &mut Ledger) {
    for txn in txns {
        #[cfg(feature = "simulated_test_utils")]
        std::thread::sleep(std::time::Duration::from_micros(100));
        let from_balance = ledger.0.get(&txn.from).unwrap();
        if from_balance >= &txn.money {
            let to_balance = *ledger.0.get(&txn.to).unwrap();
            ledger.0.insert(txn.from, from_balance - txn.money);
            ledger.0.insert(txn.to, to_balance + txn.money);
        }
    }
}
/// parallel execute txns,apply changeset to ledger directly.
pub fn parallel_execute(
    txns: &Vec<TransferTransaction>,
    ledger: &mut Ledger,
    concurrency_level: usize,
) {
    let pe = ParallelExecutor::<TransferTransaction, TransferVM>::new(concurrency_level);
    let changeset = pe.execute_transactions(txns, ledger);
    for (k, v) in changeset {
        match v {
            Some(v) => ledger.0.insert(k, v),
            None => ledger.0.remove(&k),
        };
    }
}
/// generate random txns and genesis ledger with the given parameters
pub fn generate_txns_and_ledger(
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
