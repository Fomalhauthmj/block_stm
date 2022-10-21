use std::time::{Duration, Instant};

use aptos_bitvec::BitVec;
use aptos_crypto::HashValue;
use aptos_e2e_tests::{
    account_universe::{
        log_balance_strategy, AUTransactionGen, AccountUniverse, AccountUniverseGen, P2PTransferGen,
    },
    data_store::{FakeDataStore, GENESIS_CHANGE_SET_HEAD},
};
use aptos_move_deps::move_core_types::{
    ident_str,
    language_storage::{ModuleId, CORE_CODE_ADDRESS},
};
use aptos_state_view::StateView;
use aptos_types::{
    block_metadata::BlockMetadata,
    on_chain_config::{OnChainConfig, ValidatorSet},
    state_store::{state_key::StateKey, state_storage_usage::StateStorageUsage},
    transaction::{
        ExecutionStatus, Transaction as AptosTransaction,
        TransactionOutput as AptosTransactionOutput, TransactionStatus,
    },
    vm_status::VMStatus,
    write_set::WriteOp,
};
use aptos_vm::{
    adapter_common::{PreprocessedTransaction, VMAdapter},
    data_cache::{AsMoveResolver, IntoMoveResolver, StorageAdapter, StorageAdapterOwned},
    logging::AdapterLogSchema,
    AptosVM, VMExecutor,
};
use proptest::{
    prelude::any_with,
    strategy::{Strategy, ValueTree},
    test_runner::TestRunner,
};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{Transaction, TransactionOutput, ValueBytes, VM},
    mvmemory::{MVMemoryView, ReadResult},
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
impl TransactionOutput for AptosTransactionOutput {
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

    type Output = AptosTransactionOutput;

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
                Ok(output_ext.into_transaction_output(&executor_view))
            }
            Err(err) => Err(err),
        }
    }
}
/// Check the signature (if any) of a transaction. If the signature is OK, the result
/// is a PreprocessedTransaction, where a user transaction is translated to a
/// SignatureCheckedTransaction and also categorized into either a UserTransaction
/// or a WriteSet transaction.
fn preprocess_transaction<A: VMAdapter>(txn: AptosTransaction) -> PreprocessedTransaction {
    match txn {
        AptosTransaction::BlockMetadata(b) => PreprocessedTransaction::BlockMetadata(b),
        AptosTransaction::GenesisTransaction(ws) => PreprocessedTransaction::WaypointWriteSet(ws),
        AptosTransaction::UserTransaction(txn) => {
            let checked_txn = match A::check_signature(txn) {
                Ok(checked_txn) => checked_txn,
                _ => {
                    return PreprocessedTransaction::InvalidSignature;
                }
            };
            PreprocessedTransaction::UserTransaction(Box::new(checked_txn))
        }
        AptosTransaction::StateCheckpoint(_) => PreprocessedTransaction::StateCheckpoint,
    }
}
///
pub fn generate_aptos_txns_and_state(
    num_accounts: usize,
    num_transactions: usize,
) -> (Vec<AptosTransaction>, FakeDataStore) {
    // prepare
    let max_balance = 500_000 * num_transactions as u64 * 5;
    let universe_strategy =
        AccountUniverseGen::strategy(num_accounts, log_balance_strategy(max_balance));
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
            txns.push(AptosTransaction::UserTransaction(txn));
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
    txns.insert(0, AptosTransaction::BlockMetadata(new_block));
    (txns, state)
}

/// sequential execute,without applying writeset,return execution duration
pub fn aptos_sequential_execute(txns: Vec<AptosTransaction>, state: &FakeDataStore) -> Duration {
    // The output is ignored here since we're just testing transaction performance, not trying
    // to assert correctness.
    AptosVM::execute_block_and_keep_vm_status_benchmark(txns, &state)
        .expect("VM should not fail to start")
}
/// sequential execute,with applying writeset,return fakedatastore
pub fn aptos_sequential_execute_and_apply(
    txns: Vec<AptosTransaction>,
    mut state: FakeDataStore,
) -> FakeDataStore {
    AptosVM::execute_block(txns, &state)
        .expect("VM should not fail to start")
        .into_iter()
        .for_each(|output| {
            state.add_write_set(output.write_set());
        });
    state
}
/// parallel execute,without applying writeset,return execution duration
pub fn aptos_parallel_execute(
    txns: Vec<AptosTransaction>,
    state: &FakeDataStore,
    concurrency_level: usize,
) -> Duration {
    let pe = ParallelExecutor::<PreprocessedTransaction, AptosVMWrapper<FakeDataStore>>::new(
        concurrency_level,
    );
    let txns = txns
        .par_iter()
        .map(|txn| preprocess_transaction::<AptosVM>(txn.clone()))
        .collect();
    let start = Instant::now();
    pe.execute_transactions(&txns, &state);
    start.elapsed()
}
/// official parallel executor from `aptos-core`
pub fn aptos_official_parallel_execute(
    txns: Vec<AptosTransaction>,
    state: &FakeDataStore,
    concurrency_level: usize,
) -> Duration {
    aptos_vm::parallel_executor::ParallelAptosVM::execute_block_benchmark(
        txns,
        state,
        concurrency_level,
    )
    .expect("should success")
}
/// parallel execute,with applying writeset,return fakedatastore,
pub fn aptos_parallel_execute_and_apply(
    txns: Vec<AptosTransaction>,
    mut state: FakeDataStore,
    concurrency_level: usize,
) -> FakeDataStore {
    let pe = ParallelExecutor::<PreprocessedTransaction, AptosVMWrapper<FakeDataStore>>::new(
        concurrency_level,
    );
    let txns = txns
        .par_iter()
        .map(|txn| preprocess_transaction::<AptosVM>(txn.clone()))
        .collect();
    pe.execute_transactions(&txns, &&state)
        .into_iter()
        .for_each(|(k, v)| match v {
            Some(WriteOp::Creation(bytes)) | Some(WriteOp::Modification(bytes)) => {
                state.set(k, bytes);
            }
            Some(WriteOp::Deletion) => unreachable!(),
            None => {
                state.remove(&k);
            }
        });
    state
}
