use std::time::Instant;

use aptos_e2e_tests::data_store::FakeDataStore;
use aptos_types::transaction::TransactionOutput;
use aptos_vm::{adapter_common::PreprocessedTransaction, AptosVM};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use super::BenchmarkInfo;

mod aptos_impl;
pub use aptos_impl::aptos_parallel_execute;
mod my_impl;
pub use my_impl::my_parallel_execute;

///
pub fn generate_txns_and_state(
    num_accounts: usize,
    num_transactions: usize,
) -> (
    Vec<aptos_vm::adapter_common::PreprocessedTransaction>,
    aptos_e2e_tests::data_store::FakeDataStore,
) {
    use aptos_bitvec::BitVec;
    use aptos_crypto::HashValue;
    use aptos_e2e_tests::{
        account_universe::{AUTransactionGen, AccountUniverse, AccountUniverseGen, P2PTransferGen},
        data_store::GENESIS_CHANGE_SET_HEAD,
    };
    use aptos_types::{
        block_metadata::BlockMetadata,
        on_chain_config::{OnChainConfig, ValidatorSet},
        transaction::{ExecutionStatus, Transaction, TransactionStatus},
    };
    use aptos_vm::{adapter_common::preprocess_transaction, data_cache::AsMoveResolver};
    use proptest::{
        prelude::any_with,
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    // prepare
    let max_balance = 1_000_000 * num_transactions as u64;
    let universe_strategy =
        AccountUniverseGen::strategy(num_accounts, max_balance..1_000 * max_balance);
    // generate
    let mut runner = TestRunner::default();
    let universe = universe_strategy
        .new_tree(&mut runner)
        .expect("creating a new value should succeed")
        .current();

    let mut state = FakeDataStore::default();
    state.add_write_set(GENESIS_CHANGE_SET_HEAD.write_set());
    let mut universe = {
        for account_data in &universe.accounts {
            state.add_account_data(account_data);
        }
        AccountUniverse::new(universe.accounts, universe.pick_style, true)
    };

    let mut txns = Vec::new();
    while txns.len() < num_transactions {
        let txn_gen = any_with::<P2PTransferGen>((1_00, 10_000))
            .new_tree(&mut runner)
            .expect("creating a new value should succeed")
            .current();
        let (txn, (status, _)) = txn_gen.apply(&mut universe);
        if let TransactionStatus::Keep(ExecutionStatus::Success) = status {
            txns.push(Transaction::UserTransaction(txn));
        }
    }
    // Insert a blockmetadata transaction at the beginning to better simulate the real life traffic.
    let validator_set = ValidatorSet::fetch_config(&state.as_move_resolver())
        .expect("Unable to retrieve the validator set from storage");

    let new_block = BlockMetadata::new(
        HashValue::zero(),
        0,
        0,
        *validator_set.payload().next().unwrap().account_address(),
        BitVec::with_num_bits(validator_set.num_validators() as u16).into(),
        vec![],
        1,
    );
    txns.insert(0, Transaction::BlockMetadata(new_block));

    let txns = txns
        .into_par_iter()
        .map(preprocess_transaction::<AptosVM>)
        .collect();

    (txns, state)
}
/// sequential execute txns
pub fn sequential_execute(
    txns: &Vec<PreprocessedTransaction>,
    state: &FakeDataStore,
) -> (Vec<TransactionOutput>, BenchmarkInfo) {
    let total = Instant::now();
    if let Ok(output) = AptosVM::execute_block_and_keep_vm_status_benchmark(txns, state) {
        let total_time = total.elapsed();
        (
            output.into_iter().map(|(_, output)| output).collect(),
            BenchmarkInfo {
                total_time,
                execute_time: None,
                collect_time: None,
            },
        )
    } else {
        unreachable!();
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aptos_e2e_tests::data_store::FakeDataStore;
    use aptos_types::{
        state_store::state_key::StateKey, transaction::TransactionOutput, write_set::WriteOp,
    };
    use either::Either;

    use super::{
        aptos_impl::aptos_parallel_execute, generate_txns_and_state, my_impl::my_parallel_execute,
        sequential_execute,
    };

    fn apply(
        mut state: FakeDataStore,
        output: Either<Vec<TransactionOutput>, Vec<(StateKey, Option<WriteOp>)>>,
    ) -> HashMap<StateKey, Vec<u8>> {
        match output {
            Either::Left(outputs) => {
                outputs.into_iter().for_each(|output| {
                    state.add_write_set(output.write_set());
                });
            }
            Either::Right(output) => {
                for (k, v) in output {
                    match v {
                        Some(WriteOp::Creation(blob)) | Some(WriteOp::Modification(blob)) => {
                            state.set(k, blob);
                        }
                        None => {
                            state.remove(&k);
                        }
                        _ => unreachable!(),
                    }
                }
            }
        };
        state.state_data
    }

    #[test]
    fn test_aptos_parallel_execute() {
        let (txns, state) = generate_txns_and_state(5, 1_000);
        let (s_output, _) = sequential_execute(&txns, &state);
        let (ap_output, _) = aptos_parallel_execute(&txns, &state, num_cpus::get());
        assert_eq!(s_output, ap_output);
    }
    #[test]
    fn test_my_parallel_execute() {
        let (txns, state) = generate_txns_and_state(5, 1_000);
        let (s_output, _) = sequential_execute(&txns, &state);
        let (mp_output, _) = my_parallel_execute(&txns, &state, num_cpus::get());
        let cloned = state.clone();
        assert_eq!(
            apply(state, Either::Left(s_output)),
            apply(cloned, Either::Right(mp_output))
        );
    }
}
