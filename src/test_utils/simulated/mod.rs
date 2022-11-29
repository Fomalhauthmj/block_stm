use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use anyhow::anyhow;
use either::Either;
use rand::{distributions::Uniform, prelude::Distribution};

mod my_impl;
pub use my_impl::my_parallel_execute;

///
#[derive(Clone, Debug)]
pub struct TransferTransaction {
    /// transfer money from
    pub from: usize,
    /// transfer money to
    pub to: usize,
    /// transfer money amount
    pub money: usize,
}
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTransactionOutput(Vec<(usize, usize)>);
///
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ledger(HashMap<usize, usize>);
impl Deref for Ledger {
    type Target = HashMap<usize, usize>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Ledger {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl Ledger {
    ///
    pub fn apply(
        mut self,
        output: Either<Vec<TransferTransactionOutput>, Vec<(usize, Option<usize>)>>,
    ) -> Self {
        use either::{Left, Right};
        match output {
            Left(outputs) => {
                outputs.into_iter().for_each(|output| {
                    for (k, v) in output.0 {
                        self.0.insert(k, v);
                    }
                });
            }
            Right(output) => {
                output.into_iter().for_each(|(k, v)| {
                    if let Some(v) = v {
                        self.0.insert(k, v);
                    } else {
                        self.0.remove(&k);
                    }
                });
            }
        }
        self
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
    let mut ledger = Ledger::default();
    (0..accounts_num).into_iter().for_each(|account| {
        let _ = ledger.insert(account, init_balance);
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
struct SequentialVM(Ledger);
impl SequentialVM {
    fn new(ledger: Ledger) -> Self {
        Self(ledger)
    }
    fn apply_output(&mut self, output: TransferTransactionOutput) {
        for (k, v) in output.0 {
            self.0.insert(k, v);
        }
    }
    fn execute_transaction(
        &self,
        txn: &TransferTransaction,
    ) -> anyhow::Result<TransferTransactionOutput> {
        let read = |k| match self.0.get(k) {
            Some(v) => Ok(*v),
            None => Err(anyhow!("value not found")),
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
/// sequential execute txns
pub fn sequential_execute(
    txns: &[TransferTransaction],
    ledger: &Ledger,
) -> Vec<TransferTransactionOutput> {
    let mut vm = SequentialVM::new(ledger.clone());
    txns.iter()
        .map(|txn| {
            let output = vm.execute_transaction(txn).expect("execute error");
            vm.apply_output(output.clone());
            output
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::{my_impl::my_parallel_execute, *};
    #[test]
    fn test_my_parallel_execute() {
        let (txns, ledger) = generate_txns_and_ledger(5, 1_000_000, 1_000, 1, 1_000);
        let s_output = sequential_execute(&txns, &ledger);
        let mp_output = my_parallel_execute(&txns, &ledger, num_cpus::get());
        assert_eq!(s_output, mp_output);
    }
}
