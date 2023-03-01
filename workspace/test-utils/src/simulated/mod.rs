mod parallel;
pub use parallel::simulated_parallel_execute;
mod sequential;
pub use sequential::simulated_sequential_execute;

mod types {
    use std::collections::HashMap;

    /// simulated transfer txn
    #[derive(Debug, Clone)]
    pub struct TransferTxn {
        /// transfer money from
        pub from: usize,
        /// transfer money to
        pub to: usize,
        /// transfer money amount
        pub money: usize,
    }
    /// simulated transfer txn output (account,balance)
    #[derive(Debug, PartialEq, Eq)]
    pub struct TransferTxnOutput(pub Vec<(usize, usize)>);
    /// simulated ledger (account,balance)
    pub type Ledger = HashMap<usize, usize>;
}

use self::types::{Ledger, TransferTxn};
/// generate txns and ledger with the given parameters
pub fn generate_txns_and_ledger(
    accounts_num: usize,
    txns_num: usize,
) -> (Vec<TransferTxn>, Ledger) {
    use rand::{distributions::Uniform, prelude::Distribution, thread_rng};

    static INIT_BALANCE: usize = usize::MAX / 2;
    static MIN_AMOUNT: usize = 10;
    static MAX_AMOUNT: usize = 10_000;

    let mut txns = Vec::with_capacity(txns_num);
    let mut ledger = Ledger::default();
    // set init balance
    (0..accounts_num).into_iter().for_each(|account| {
        ledger.insert(account, INIT_BALANCE);
    });
    // create uniform distribution
    // TODO: other dists,like zipfan
    let account_dist = Uniform::from(0..accounts_num);
    let amount_dist = Uniform::from(MIN_AMOUNT..=MAX_AMOUNT);
    let mut rng = thread_rng();
    // generate txns
    while txns.len() < txns_num {
        let from = account_dist.sample(&mut rng);
        let to = account_dist.sample(&mut rng);
        if from == to {
            continue;
        }
        let money = amount_dist.sample(&mut rng);
        txns.push(TransferTxn { from, to, money });
    }
    (txns, ledger)
}

#[cfg(test)]
mod tests {
    use crate::simulated::{
        generate_txns_and_ledger, simulated_parallel_execute, simulated_sequential_execute,
    };

    #[test]
    fn simulated_correctness() {
        let (txns, ledger) = generate_txns_and_ledger(5, 10_000);
        let se = simulated_sequential_execute(txns.clone(), &ledger);
        let pe = simulated_parallel_execute(txns, &ledger, num_cpus::get());
        assert_eq!(se, pe);
    }
}
