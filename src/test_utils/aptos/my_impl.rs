use std::{collections::HashMap, time::Instant};

use aptos_aggregator::{
    delta_change_set::{deserialize, serialize, DeltaOp},
    transaction::TransactionOutputExt,
};
use aptos_e2e_tests::data_store::FakeDataStore;
use aptos_move_deps::move_core_types::{ident_str, language_storage::ModuleId};
use aptos_state_view::StateView;
use aptos_types::{
    account_config::CORE_CODE_ADDRESS,
    state_store::{state_key::StateKey, state_storage_usage::StateStorageUsage},
    transaction::TransactionOutput,
    vm_status::VMStatus,
    write_set::{WriteOp, WriteSetMut},
};
use aptos_vm::{
    adapter_common::{PreprocessedTransaction, VMAdapter},
    data_cache::{IntoMoveResolver, StorageAdapter, StorageAdapterOwned},
    logging::AdapterLogSchema,
    AptosVM,
};

use crate::{
    core::{Mergeable, Transaction, TransactionOutput as MyTransactionOutput, ValueBytes, VM},
    mvmemory::{mvmap::EntryCell, MVMemoryView, ReadResult},
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
impl Mergeable for WriteOp {
    type DeltaOp = DeltaOp;

    fn partial_mergeable() -> bool {
        true
    }

    fn partial_merge(left: &Self::DeltaOp, right: &Self::DeltaOp) -> Self::DeltaOp {
        let mut result = *right;
        result.merge_onto(*left).expect("merge error");
        result
    }

    fn apply_delta(&self, delta: &Self::DeltaOp) -> Self {
        if let Some(bytes) = self.serialize() {
            let base = u128::from_ne_bytes(bytes.try_into().expect("convert error"));
            let result = delta.apply_to(base).expect("apply error").to_ne_bytes();
            WriteOp::deserialize(&result)
        } else {
            unreachable!()
        }
    }
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
            ReadResult::Value(v) | ReadResult::Merged(v) => Ok(match v.as_ref() {
                WriteOp::Creation(w) | WriteOp::Modification(w) => Some(w.clone()),
                WriteOp::Deletion => None,
            }),
            ReadResult::NotFound => self.base_view.get_state_value(state_key),
            ReadResult::PartialMerged(d) => {
                let base = self.base_view.get_state_value(state_key)?;
                match base {
                    Some(bytes) => {
                        let v = WriteOp::deserialize(&bytes).apply_delta(&d);
                        match v {
                            WriteOp::Creation(w) | WriteOp::Modification(w) => Ok(Some(w)),
                            WriteOp::Deletion => Ok(None),
                        }
                    }
                    None => Ok(None),
                }
            }
            ReadResult::Unmerged(ds) => {
                let base = self.base_view.get_state_value(state_key)?;
                match base {
                    Some(bytes) => {
                        let mut v = WriteOp::deserialize(&bytes);
                        for d in ds {
                            v = v.apply_delta(&d);
                        }
                        match v {
                            WriteOp::Creation(w) | WriteOp::Modification(w) => Ok(Some(w)),
                            WriteOp::Deletion => Ok(None),
                        }
                    }
                    None => Ok(None),
                }
            }
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
impl MyTransactionOutput for TransactionOutputExt {
    type T = PreprocessedTransaction;

    fn get_write_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <Self::T as Transaction>::Value,
    )> {
        self.txn_output()
            .write_set()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn get_delta_set(
        &self,
    ) -> Vec<(
        <Self::T as Transaction>::Key,
        <<Self::T as Transaction>::Value as Mergeable>::DeltaOp,
    )> {
        self.delta_change_set()
            .iter()
            .map(|(k, d)| (k.clone(), d.clone()))
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

    type Output = TransactionOutputExt;

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
        #[cfg(feature = "trace_single_txn")]
        let start = std::time::Instant::now();

        let log_context = AdapterLogSchema::new(self.base_view.id(), view.txn_idx());
        let executor_view = ExecutorView::new_view(self.base_view, view);
        let result = match self
            .vm
            .execute_single_transaction(txn, &executor_view, &log_context)
        {
            Ok((_vm_status, output_ext, _sender)) => Ok(output_ext),
            Err(err) => Err(err),
        };

        #[cfg(feature = "trace_single_txn")]
        crate::rayon_trace!(
            "<AptosVMWrapper as VM>::execute_transaction():{:?}",
            start.elapsed()
        );

        result
    }
}
/// parallel execute,without applying writeset,return execution duration
pub fn my_parallel_execute(
    txns: &Vec<PreprocessedTransaction>,
    state: &FakeDataStore,
    concurrency_level: usize,
) -> (Vec<TransactionOutput>, BenchmarkInfo) {
    let pe = ParallelExecutor::<PreprocessedTransaction, AptosVMWrapper<FakeDataStore>>::new(
        concurrency_level,
    );
    let total_time = Instant::now();
    let (outputs, mvmemory, execute, collect) = pe.execute_transactions_benchmark(txns, state);
    let base_values: Vec<(StateKey, anyhow::Result<Option<Vec<u8>>>)> = {
        let mut result = HashMap::new();
        outputs.iter().for_each(|output| {
            for (k, _) in output.delta_change_set().iter() {
                if !result.contains_key(k) {
                    result.insert(k.clone(), state.get_state_value(k));
                }
            }
        });
        result.into_iter().collect()
    };
    let mut final_values: Vec<Vec<(StateKey, WriteOp)>> =
        (0..txns.len()).map(|_| Vec::new()).collect();
    for (key, base_value) in base_values.into_iter() {
        let mut latest_value: Option<u128> = match base_value
            .ok() // Was anything found in storage
            .map(|value| value.map(|bytes| deserialize(&bytes)))
        {
            None => None,
            Some(v) => v,
        };

        let indexed_entries = mvmemory
            .entry_map_for_key(&key)
            .expect("No entries found for the provided key");
        for (idx, entry) in indexed_entries.iter() {
            match &entry.cell {
                EntryCell::Write(_, data) => {
                    latest_value = data.serialize().map(|bytes| deserialize(&bytes))
                }
                EntryCell::Delta(delta) => {
                    // Apply to the latest value and store in outputs.
                    let aggregator_value = delta
                        .apply_to(
                            latest_value
                                .expect("Failed to apply delta to (non-existent) aggregator"),
                        )
                        .expect("Failed to apply aggregator delta output");

                    final_values[*idx].push((
                        key.clone(),
                        WriteOp::Modification(serialize(&aggregator_value)),
                    ));
                    latest_value = Some(aggregator_value);
                }
            }
        }
    }
    let outputs = outputs
        .into_iter()
        .zip(final_values.into_iter())
        .map(|(output, delta)| output.output_with_delta_writes(WriteSetMut::new(delta)))
        .collect();
    (
        outputs,
        BenchmarkInfo {
            total_time: total_time.elapsed(),
            execute_time: Some(execute),
            collect_time: Some(collect),
        },
    )
}
