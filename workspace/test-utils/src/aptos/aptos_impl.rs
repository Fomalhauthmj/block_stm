use aptos_block_executor::executor::{BlockExecutor, RAYON_EXEC_POOL};
use aptos_language_e2e_tests::data_store::FakeDataStore;
use aptos_types::{transaction::TransactionOutput, write_set::WriteSetMut};
use aptos_vm::{adapter_common::PreprocessedTransaction, AptosExecutorTask};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use tracing::instrument;
#[instrument(level = "trace", skip(txns, state_view))]
pub fn aptos_sequential_execute(
    txns: Vec<PreprocessedTransaction>,
    state_view: &FakeDataStore,
) -> Vec<TransactionOutput> {
    let executor = BlockExecutor::<
        PreprocessedTransaction,
        AptosExecutorTask<FakeDataStore>,
        FakeDataStore,
    >::new(1);
    if let Ok(outputs) = executor
        .execute_block(state_view, txns.to_vec(), state_view)
        .map(|results| {
            // Process the outputs in parallel, combining delta writes with other writes.
            RAYON_EXEC_POOL.install(|| {
                results
                    .into_par_iter()
                    .map(|(output, delta_writes)| {
                        output      // AptosTransactionOutput
                    .into()     // TransactionOutputExt
                    .output_with_delta_writes(WriteSetMut::new(delta_writes))
                    })
                    .collect()
            })
        })
    {
        outputs
    } else {
        unreachable!()
    }
}
#[instrument(level = "trace", skip(txns, state_view))]
#[no_mangle]
pub fn aptos_parallel_execute(
    txns: Vec<PreprocessedTransaction>,
    state_view: &FakeDataStore,
    concurrency_level: usize,
) -> Vec<TransactionOutput> {
    let executor = BlockExecutor::<
        PreprocessedTransaction,
        AptosExecutorTask<FakeDataStore>,
        FakeDataStore,
    >::new(concurrency_level);
    if let Ok(outputs) = executor
        .execute_block(state_view, txns, state_view)
        .map(|results| {
            // Process the outputs in parallel, combining delta writes with other writes.
            RAYON_EXEC_POOL.install(|| {
                results
                    .into_par_iter()
                    .map(|(output, delta_writes)| {
                        output      // AptosTransactionOutput
                        .into()     // TransactionOutputExt
                        .output_with_delta_writes(WriteSetMut::new(delta_writes))
                    })
                    .collect()
            })
        })
    {
        outputs
    } else {
        unreachable!()
    }
}
