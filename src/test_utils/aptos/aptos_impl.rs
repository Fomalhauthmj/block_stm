use std::time::Instant;

use aptos_e2e_tests::data_store::FakeDataStore;
use aptos_types::transaction::TransactionOutput;
use aptos_vm::adapter_common::PreprocessedTransaction;

use crate::test_utils::BenchmarkInfo;

/// parallel executor from `aptos-core`
pub fn aptos_parallel_execute(
    txns: &Vec<PreprocessedTransaction>,
    state: &FakeDataStore,
    concurrency_level: usize,
) -> (Vec<TransactionOutput>, BenchmarkInfo) {
    let total = Instant::now();
    if let Ok((output, _, execute, collect)) =
        aptos_vm::parallel_executor::ParallelAptosVM::execute_block_benchmark(
            txns,
            state,
            concurrency_level,
        )
    {
        (
            output,
            BenchmarkInfo {
                total_time: total.elapsed(),
                execute_time: Some(execute),
                collect_time: Some(collect),
            },
        )
    } else {
        unreachable!()
    }
}
