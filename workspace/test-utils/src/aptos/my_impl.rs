use std::sync::Arc;

use aptos_aggregator::transaction::TransactionOutputExt;
use aptos_language_e2e_tests::data_store::FakeDataStore;
use aptos_types::{transaction::TransactionOutput, write_set::TransactionWrite};
use aptos_vm::adapter_common::PreprocessedTransaction;
use block_stm::{EntryCell, ParallelExecutor};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use tracing::instrument;

mod wrapper {
    use std::sync::Arc;

    use aptos_aggregator::{
        delta_change_set::{deserialize, serialize, DeltaOp},
        transaction::TransactionOutputExt,
    };
    use aptos_state_view::{StateView, TStateView};
    use aptos_types::{
        account_config::CORE_CODE_ADDRESS,
        state_store::{
            state_key::StateKey, state_storage_usage::StateStorageUsage, state_value::StateValue,
        },
        vm_status::{StatusCode, VMStatus},
        write_set::{TransactionWrite, WriteOp},
    };
    use aptos_vm::{
        adapter_common::PreprocessedTransaction,
        data_cache::{IntoMoveResolver, StorageAdapter, StorageAdapterOwned},
        logging::AdapterLogSchema,
        AptosVM,
    };

    use block_stm::{
        DeltaSet, MVMapView, Mergeable, ReadResult, Transaction, TransactionOutput, WriteSet, VM,
    };
    use move_binary_format::errors::Location;
    use move_core_types::{ident_str, language_storage::ModuleId};

    #[derive(PartialEq, Eq, Debug)]
    pub struct ValueWrapper(pub WriteOp);
    impl ValueWrapper {
        pub fn serialize(&self) -> Vec<u8> {
            bcs::to_bytes(&self.0).expect("bcs serialize error")
        }
        fn deserialize(bytes: &[u8]) -> Self {
            Self(bcs::from_bytes::<WriteOp>(bytes).expect("bcs deserialize error"))
        }
    }
    impl Mergeable for ValueWrapper {
        type Delta = DeltaOp;

        fn partial_mergeable() -> bool {
            true
        }

        fn partial_merge(left: &Self::Delta, right: &Self::Delta) -> Self::Delta {
            let mut result = *right;
            result.merge_onto(*left).expect("merge error");
            result
        }

        fn apply_delta(&self, delta: &Self::Delta) -> Self {
            let bytes = self.serialize();
            let base = u128::from_ne_bytes(bytes.try_into().expect("convert error"));
            let result = delta.apply_to(base).expect("apply error").to_ne_bytes();
            ValueWrapper::deserialize(&result)
        }
    }
    pub struct TxnWrapper(pub PreprocessedTransaction);
    impl Transaction for TxnWrapper {
        type Key = StateKey;
        type Value = ValueWrapper;
    }
    pub struct OutputWrapper(pub TransactionOutputExt);
    impl TransactionOutput for OutputWrapper {
        type T = TxnWrapper;

        fn get_write_set(
            &self,
        ) -> WriteSet<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value> {
            self.0
                .txn_output()
                .write_set()
                .iter()
                .map(|(k, v)| (k.clone(), ValueWrapper(v.clone())))
                .collect()
        }

        fn get_delta_set(
            &self,
        ) -> DeltaSet<
            <Self::T as Transaction>::Key,
            <<Self::T as Transaction>::Value as Mergeable>::Delta,
        > {
            self.0
                .delta_change_set()
                .iter()
                .map(|(k, d)| (k.clone(), *d))
                .collect()
        }
    }

    struct AptosExecutorView<'a, S>
    where
        S: TStateView<Key = StateKey> + Send,
    {
        base_view: Arc<S>,
        hashmap_view: &'a MVMapView<StateKey, ValueWrapper>,
    }
    impl<'a, S> AptosExecutorView<'a, S>
    where
        S: TStateView<Key = StateKey> + Send,
    {
        fn new_view(
            base_view: Arc<S>,
            hashmap_view: &'a MVMapView<StateKey, ValueWrapper>,
        ) -> StorageAdapterOwned<Self> {
            Self {
                base_view,
                hashmap_view,
            }
            .into_move_resolver()
        }
    }
    impl<'a, S> TStateView for AptosExecutorView<'a, S>
    where
        S: TStateView<Key = StateKey> + Send,
    {
        type Key = StateKey;
        // read from hashmap or from storage
        fn get_state_value(
            &self,
            state_key: &StateKey,
        ) -> Result<Option<StateValue>, anyhow::Error> {
            #[cfg(feature = "tracing")]
            let read = std::time::Instant::now();

            let result =
                match self.hashmap_view.read(state_key) {
                    ReadResult::Value(v) | ReadResult::Merged(v) => Ok(
                        v.0.extract_raw_bytes().map(StateValue::new), //match &v.0 {
                                                                      //WriteOp::Creation(w) | WriteOp::Modification(w) => Some(w.clone()),
                                                                      //WriteOp::Deletion => None,
                                                                      //}
                    ),
                    ReadResult::NotFound => self.base_view.get_state_value(state_key),
                    ReadResult::PartialMerged(d) => {
                        let base =
                            self.base_view.get_state_value_bytes(state_key)?.map_or(
                                Err(VMStatus::Error(StatusCode::STORAGE_ERROR)),
                                |bytes| Ok(deserialize(&bytes)),
                            )?;
                        let result = d
                            .apply_to(base)
                            .map_err(|pe| pe.finish(Location::Undefined).into_vm_status())?;
                        Ok(Some(StateValue::new(serialize(&result))))
                    }
                    ReadResult::Unmerged(ds) => {
                        let mut base =
                            self.base_view.get_state_value_bytes(state_key)?.map_or(
                                Err(VMStatus::Error(StatusCode::STORAGE_ERROR)),
                                |bytes| Ok(deserialize(&bytes)),
                            )?;
                        for d in ds {
                            base = d
                                .apply_to(base)
                                .map_err(|pe| pe.finish(Location::Undefined).into_vm_status())?;
                        }
                        Ok(Some(StateValue::new(serialize(&base))))
                    }
                };
            #[cfg(feature = "tracing")]
            tracing::trace!("read = {:?}", read.elapsed().as_nanos());
            result
        }

        fn is_genesis(&self) -> bool {
            self.base_view.is_genesis()
        }

        fn get_usage(&self) -> anyhow::Result<StateStorageUsage> {
            self.base_view.get_usage()
        }
    }
    pub struct VMWrapper<S>
    where
        S: StateView + Send + Sync,
    {
        vm: AptosVM,
        base_view: Arc<S>,
    }
    impl<S> VM for VMWrapper<S>
    where
        S: StateView + Send + Sync + 'static,
    {
        type T = TxnWrapper;

        type Output = OutputWrapper;

        type Error = VMStatus;

        type Parameter = Arc<S>;

        fn new(parameter: Self::Parameter) -> Self {
            let vm = AptosVM::new(&parameter);
            let _ = vm.load_module(
                &ModuleId::new(CORE_CODE_ADDRESS, ident_str!("account").to_owned()),
                &StorageAdapter::new(&parameter),
            );
            Self {
                vm,
                base_view: parameter,
            }
        }

        fn execute_transaction(
            &self,
            txn: &Self::T,
            view: &MVMapView<<Self::T as Transaction>::Key, <Self::T as Transaction>::Value>,
        ) -> Result<Self::Output, Self::Error> {
            use aptos_vm::adapter_common::VMAdapter;
            #[cfg(feature = "trace_single_txn")]
            let start = std::time::Instant::now();
            let log_context = AdapterLogSchema::new(self.base_view.id(), view.txn_idx());
            let executor_view = AptosExecutorView::new_view(self.base_view.clone(), view);
            match self
                .vm
                .execute_single_transaction(&txn.0, &executor_view, &log_context)
            {
                Ok((_vm_status, output_ext, _sender)) => Ok(OutputWrapper(output_ext)),
                Err(err) => Err(err),
            }

            #[cfg(feature = "trace_single_txn")]
            tracing::trace!(
                "<AptosVMWrapper as VM>::execute_transaction():{:?}",
                start.elapsed()
            );
        }
    }
}
#[instrument(level = "trace", skip(txns, state_view))]
#[no_mangle]
pub fn my_parallel_execute(
    txns: Vec<PreprocessedTransaction>,
    state_view: &FakeDataStore,
    concurrency_level: usize,
) -> Vec<TransactionOutput> {
    use self::wrapper::{TxnWrapper, VMWrapper};
    let txns: Vec<TxnWrapper> = txns.into_par_iter().map(TxnWrapper).collect();

    let txns_len = txns.len();
    let cloned_state = Arc::new(state_view.clone());

    #[cfg(feature = "tracing")]
    let execution = std::time::Instant::now();

    let executor = ParallelExecutor::<TxnWrapper, VMWrapper<FakeDataStore>>::new(concurrency_level);
    let (outputs, map) = executor.execute_transactions(txns, cloned_state);

    #[cfg(feature = "tracing")]
    tracing::trace!("execution = {:?}", execution.elapsed());
    // convert outputext to output
    #[cfg(feature = "tracing")]
    let output_resolve = std::time::Instant::now();

    use aptos_aggregator::delta_change_set::{deserialize, serialize};
    use aptos_state_view::TStateView;
    use aptos_types::{
        state_store::state_key::StateKey,
        write_set::{WriteOp, WriteSetMut},
    };
    use std::collections::HashMap;
    let base_values: Vec<(StateKey, Option<u128>)> = {
        #[allow(clippy::mutable_key_type)]
        let mut result = HashMap::new();
        outputs.iter().for_each(|output| {
            for (key, _) in output.0.delta_change_set().iter() {
                if !result.contains_key(key) {
                    let base_value: Option<u128> = state_view
                        .get_state_value_bytes(key)
                        .ok() // Was anything found in storage
                        .and_then(|value| value.map(|bytes| deserialize(&bytes)));

                    result.insert(key.clone(), base_value);
                }
            }
        });
        result.into_iter().collect()
    };
    let mut final_values: Vec<Vec<(StateKey, WriteOp)>> = vec![vec![]; txns_len];

    for (key, mut latest_value) in base_values.into_iter() {
        let indexed_entries = map
            .entry_map(&key)
            .expect("No entries found for the provided key");

        for (idx, entry) in indexed_entries.iter() {
            match &entry.cell {
                EntryCell::Write(_, data) => {
                    latest_value = data.0.extract_raw_bytes().map(|bytes| deserialize(&bytes))
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

    let outputs: Vec<(TransactionOutputExt, Vec<(StateKey, WriteOp)>)> = outputs
        .into_iter()
        .map(|output| output.0)
        .zip(final_values.into_iter())
        .collect();
    let result: Vec<TransactionOutput> = outputs
        .into_par_iter()
        .map(|(output, delta)| output.output_with_delta_writes(WriteSetMut::new(delta)))
        .collect();

    #[cfg(feature = "tracing")]
    tracing::trace!("output_resolve = {:?}", output_resolve.elapsed());

    result
}
#[instrument(level = "trace", skip(txns, state_view))]
pub fn my_sequential_execute(
    txns: Vec<PreprocessedTransaction>,
    state_view: &FakeDataStore,
) -> Vec<TransactionOutput> {
    // TODO: use BTreeMap instead of MVMap
    my_parallel_execute(txns, state_view, 1)
}
