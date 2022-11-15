use std::time::Instant;

use aptos_e2e_tests::data_store::FakeDataStore;
use aptos_parallel_executor::{
    executor::MVHashMapView,
    task::{ExecutionStatus, ExecutorTask, Transaction},
};
use aptos_state_view::StateView;
use aptos_types::{transaction::TransactionOutput, vm_status::VMStatus};
use aptos_vm::{
    adapter_common::PreprocessedTransaction, parallel_executor::AptosTransactionOutput,
    AptosVMWrapper,
};

use crate::test_utils::BenchmarkInfo;
struct AptosVMWrapperWithTracing<'a, S: StateView>(AptosVMWrapper<'a, S>);
impl<'a, S: 'a + StateView> ExecutorTask for AptosVMWrapperWithTracing<'a, S> {
    type T = PreprocessedTransaction;

    type Output = AptosTransactionOutput;

    type Error = VMStatus;

    type Argument = &'a S;

    fn init(args: Self::Argument) -> Self {
        Self(AptosVMWrapper::init(args))
    }

    fn execute_transaction(
        &self,
        view: &MVHashMapView<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>,
        txn: &Self::T,
    ) -> ExecutionStatus<Self::Output, Self::Error> {
        #[cfg(feature = "trace_single_txn")]
        let start = std::time::Instant::now();

        let result = self.0.execute_transaction(view, txn);

        #[cfg(feature = "trace_single_txn")]
        crate::rayon_trace!(
            "<AptosVMWrapperWithTracing as ExecutorTask>::execute_transaction():{:?}",
            start.elapsed()
        );

        result
    }
}
/// parallel executor from `aptos-core`
pub fn aptos_parallel_execute(
    txns: &Vec<PreprocessedTransaction>,
    state: &FakeDataStore,
    concurrency_level: usize,
) -> (Vec<TransactionOutput>, BenchmarkInfo) {
    let total = Instant::now();
    if let Ok((output, _, execute, collect)) =
        aptos_vm::parallel_executor::ParallelAptosVM::execute_block_benchmark::<
            FakeDataStore,
            AptosVMWrapperWithTracing<FakeDataStore>,
        >(txns, state, concurrency_level)
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
