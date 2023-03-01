mod aptos_impl;
pub use aptos_impl::{aptos_parallel_execute, aptos_sequential_execute};
mod my_impl;
pub use my_impl::{my_parallel_execute, my_sequential_execute};

use aptos_language_e2e_tests::data_store::FakeDataStore;
use aptos_types::transaction::{Transaction, TransactionOutput};
use aptos_vm::{
    adapter_common::{preprocess_transaction, PreprocessedTransaction},
    AptosVM,
};
use once_cell::sync::Lazy;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

type SEfunc = fn(Vec<PreprocessedTransaction>, &FakeDataStore) -> Vec<TransactionOutput>;
type PEfunc = fn(Vec<PreprocessedTransaction>, &FakeDataStore, usize) -> Vec<TransactionOutput>;
pub static AVAILABLE_SEQUENTIAL_EXECUTION: Lazy<Vec<(String, SEfunc)>> = Lazy::new(|| {
    let table: Vec<(String, SEfunc)> = vec![
        ("aptos_sequential_execute".into(), aptos_sequential_execute),
        ("my_sequential_execute".into(), my_sequential_execute),
    ];
    table
});
pub static AVAILABLE_PARALLEL_EXECUTION: Lazy<Vec<(String, PEfunc)>> = Lazy::new(|| {
    let table: Vec<(String, PEfunc)> = vec![
        ("aptos_parallel_execute".into(), aptos_parallel_execute),
        ("my_parallel_execute".into(), my_parallel_execute),
    ];
    table
});
///
pub fn generate_txns_and_state(
    accounts_num: usize,
    txns_num: usize,
) -> (Vec<PreprocessedTransaction>, FakeDataStore) {
    use aptos_cached_packages::aptos_stdlib;
    use aptos_crypto::{ed25519::Ed25519PrivateKey, PrivateKey};
    use aptos_language_e2e_tests::{account::AccountData, data_store::GENESIS_CHANGE_SET_TESTNET};
    use rand::{distributions::Uniform, prelude::Distribution, rngs::StdRng, SeedableRng};

    static INIT_BALANCE: u64 = u64::MAX / 2;
    static INIT_SEQ_NUM: u64 = 0;
    static GAS_UNIT_PRICE: u64 = 100;

    let mut state = FakeDataStore::default();
    state.add_write_set(GENESIS_CHANGE_SET_TESTNET.write_set());
    let mut accounts = Vec::with_capacity(accounts_num);
    let mut seqs = Vec::with_capacity(accounts_num);
    for i in 0..accounts_num {
        let mut rng = StdRng::seed_from_u64(i as u64);
        let private_key = <Ed25519PrivateKey as aptos_crypto::Uniform>::generate(&mut rng);
        let public_key = private_key.public_key();
        let account_data =
            AccountData::with_keypair(private_key, public_key, INIT_BALANCE, INIT_SEQ_NUM);
        state.add_account_data(&account_data);
        accounts.push(account_data.into_account());
        seqs.push(INIT_SEQ_NUM);
    }
    let from = Uniform::from(0..accounts_num);
    let to = Uniform::from(0..accounts_num);
    let amount = Uniform::from(1..10_000);
    let mut rng = rand::thread_rng();

    let mut txns = Vec::with_capacity(txns_num);
    while txns.len() < txns_num {
        let from = from.sample(&mut rng);
        let to = to.sample(&mut rng);
        if from == to {
            continue;
        }
        let amount = amount.sample(&mut rng);
        let txn = accounts[from]
            .transaction()
            .gas_unit_price(GAS_UNIT_PRICE)
            .sequence_number(seqs[from])
            .payload(aptos_stdlib::aptos_coin_transfer(
                *accounts[to].address(),
                amount,
            ))
            .sign();
        seqs[from] += 1;
        txns.push(Transaction::UserTransaction(txn));
    }
    let txns: Vec<PreprocessedTransaction> = txns
        .clone()
        .into_par_iter()
        .map(preprocess_transaction::<AptosVM>)
        .collect();
    (txns, state)
}
#[cfg(test)]
mod tests {
    use aptos_types::transaction::{ExecutionStatus, TransactionStatus};

    use crate::aptos::{
        aptos_parallel_execute, aptos_sequential_execute, generate_txns_and_state,
        my_parallel_execute, my_sequential_execute,
    };

    #[test]
    fn aptos_correctness() {
        let (txns, state) = generate_txns_and_state(5, 10_000);

        let mse = my_sequential_execute(txns.clone(), &state);
        let mpe = my_parallel_execute(txns.clone(), &state, num_cpus::get());
        let ase = aptos_sequential_execute(txns.clone(), &state);
        let ape = aptos_parallel_execute(txns.clone(), &state, num_cpus::get());

        let zipped = mse.iter().zip(mpe.iter()).zip(ase.iter()).zip(ape.iter());

        for (((mse, mpe), ase), ape) in zipped {
            assert_eq!(mse, mpe);
            assert_eq!(mse, ase);
            assert_eq!(mse, ape);
            assert_eq!(
                mse.status(),
                &TransactionStatus::Keep(ExecutionStatus::Success)
            );
            assert_eq!(
                mpe.status(),
                &TransactionStatus::Keep(ExecutionStatus::Success)
            );
            assert_eq!(
                ase.status(),
                &TransactionStatus::Keep(ExecutionStatus::Success)
            );
            assert_eq!(
                ape.status(),
                &TransactionStatus::Keep(ExecutionStatus::Success)
            );
        }
    }
}
