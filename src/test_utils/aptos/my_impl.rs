use std::time::Instant;

use aptos_e2e_tests::data_store::FakeDataStore;
use aptos_move_deps::move_core_types::{ident_str, language_storage::ModuleId};
use aptos_state_view::StateView;
use aptos_types::{
    account_config::CORE_CODE_ADDRESS,
    state_store::{state_key::StateKey, state_storage_usage::StateStorageUsage},
    vm_status::VMStatus,
    write_set::WriteOp,
};
use aptos_vm::{
    adapter_common::{PreprocessedTransaction, VMAdapter},
    data_cache::{IntoMoveResolver, StorageAdapter, StorageAdapterOwned},
    logging::AdapterLogSchema,
    AptosVM,
};

use crate::{
    core::{Transaction, TransactionOutput, ValueBytes, VM},
    mvmemory::{MVMemoryView, ReadResult},
    test_utils::BenchmarkInfo,
    ParallelExecutor,
};

/// serialize/deserialize WriteOp
impl ValueBytes for WriteOp {
    fn serialize(&self) -> Option<Vec<u8>> {
        match self {
            WriteOp::Creation(v) | WriteOp::Modification(v) => Some(v.clone()),
            WriteOp::Deletion => None,
        }
    }

    fn deserialize(bytes: &[u8]) -> Self {
        Self::Modification(bytes.to_vec())
    }
}
/// smart contract transaction
impl Transaction for PreprocessedTransaction {
    type Key = StateKey;

    type Value = WriteOp;
}
/// executor view
pub struct ExecutorView<'a, S: StateView> {
    base_view: &'a S,
    hashmap_view: &'a MVMemoryView<'a, StateKey, WriteOp>,
}
impl<'a, S: StateView> ExecutorView<'a, S> {
    pub fn new_view(
        base_view: &'a S,
        hashmap_view: &'a MVMemoryView<StateKey, WriteOp>,
    ) -> StorageAdapterOwned<ExecutorView<'a, S>> {
        Self {
            base_view,
            hashmap_view,
        }
        .into_move_resolver()
    }
}
impl<'a, S: StateView> StateView for ExecutorView<'a, S> {
    // read from hashmap or from storage
    fn get_state_value(&self, state_key: &StateKey) -> anyhow::Result<Option<Vec<u8>>> {
        match self.hashmap_view.read(state_key) {
            ReadResult::Value(v) => Ok(match v.as_ref() {
                WriteOp::Creation(w) | WriteOp::Modification(w) => Some(w.clone()),
                WriteOp::Deletion => None,
            }),
            ReadResult::NotFound => self.base_view.get_state_value(state_key),
        }
    }

    fn is_genesis(&self) -> bool {
        self.base_view.is_genesis()
    }

    fn get_usage(&self) -> anyhow::Result<StateStorageUsage> {
        self.base_view.get_usage()
    }
}
/// smart contract transaction output
impl TransactionOutput for aptos_types::transaction::TransactionOutput {
    type T = PreprocessedTransaction;

    fn get_write_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <Self::T as Transaction>::Value,
    )> {
        self.write_set()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
/// smart contract execution engine
pub struct AptosVMWrapper<'a, S>
where
    S: StateView,
{
    vm: AptosVM,
    base_view: &'a S,
}
impl<'a, S> VM for AptosVMWrapper<'a, S>
where
    S: StateView,
{
    type T = PreprocessedTransaction;

    type Output = aptos_types::transaction::TransactionOutput;

    type Error = VMStatus;

    type Parameter = &'a S;

    fn new(parameter: Self::Parameter) -> Self {
        let vm = AptosVM::new(parameter);
        let _ = vm.load_module(
            &ModuleId::new(CORE_CODE_ADDRESS, ident_str!("account").to_owned()),
            &StorageAdapter::new(parameter),
        );
        Self {
            vm,
            base_view: parameter,
        }
    }

    fn execute_transaction(
        &self,
        txn: &Self::T,
        view: &MVMemoryView<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>,
    ) -> Result<Self::Output, Self::Error> {
        let log_context = AdapterLogSchema::new(self.base_view.id(), view.txn_idx());
        let executor_view = ExecutorView::new_view(self.base_view, view);
        match self
            .vm
            .execute_single_transaction(txn, &executor_view, &log_context)
        {
            Ok((_vm_status, output_ext, _sender)) => {
                // TODO: add delta support/disable gas charging
                let output = output_ext.into_transaction_output(&executor_view);
                Ok(output)
            }
            Err(err) => Err(err),
        }
    }
}
/// parallel execute,without applying writeset,return execution duration
pub fn my_parallel_execute(
    txns: &Vec<PreprocessedTransaction>,
    state: &FakeDataStore,
    concurrency_level: usize,
) -> (Vec<(StateKey, Option<WriteOp>)>, BenchmarkInfo) {
    let pe = ParallelExecutor::<PreprocessedTransaction, AptosVMWrapper<FakeDataStore>>::new(
        concurrency_level,
    );
    let total_time = Instant::now();
    let (output, execute, collect) = pe.execute_transactions_benchmark(txns, state);
    (
        output,
        BenchmarkInfo {
            total_time: total_time.elapsed(),
            execute_time: Some(execute),
            collect_time: Some(collect),
        },
    )
}
