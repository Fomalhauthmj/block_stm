#![deny(missing_docs)]
//! block_stm implementation
/// abstract traits,used to implement user own execution engine
mod core;
pub use crate::core::{DeltaSet, Mergeable, Transaction, TransactionOutput, WriteSet, VM};
mod executor;
mod mvmemory;
pub use mvmemory::{EntryCell, MVMapView, ReadResult};
mod scheduler;
mod types;

use executor::{last_txn_io::LastTxnsIO, Executor};
use mvmemory::MVMap;
use once_cell::sync::Lazy;
use scheduler::Scheduler;
use std::{marker::PhantomData, sync::Arc};

static RAYON_EXEC_POOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .thread_name(|index| format!("rayon_exec_pool_{}", index))
        .build()
        .unwrap()
});
/// parallel executor
pub struct ParallelExecutor<T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    concurrency_level: usize,
    phantom: PhantomData<(T, V)>,
}
impl<T, V> ParallelExecutor<T, V>
where
    T: Transaction,
    V: VM<T = T> + 'static,
{
    /// create a parallel executor with given concurrency_level (0 < `concurrency_level` <= `num_cpus::get()`)
    pub fn new(concurrency_level: usize) -> Self {
        assert!(
            concurrency_level > 0 && concurrency_level <= num_cpus::get(),
            "concurrency level {} should be between 1 and number of CPUs",
            concurrency_level
        );
        Self {
            concurrency_level,
            phantom: PhantomData,
        }
    }
    /// parallel execute txns with given view
    pub fn execute_transactions(
        &self,
        txns: Vec<T>,
        parameter: V::Parameter,
    ) -> (Vec<V::Output>, MVMap<T::Key, T::Value>) {
        let txns_num = txns.len();

        let txns = Arc::new(txns);
        let mvmemory = Arc::new(MVMap::new());
        let scheduler = Arc::new(Scheduler::new(txns_num));
        let last_txn_io = Arc::new(LastTxnsIO::<T::Key, T::Value, V::Output>::new(txns_num));

        RAYON_EXEC_POOL.scope(|s| {
            for _ in 0..self.concurrency_level {
                s.spawn(|_| {
                    let executor = Executor::<T, V>::new(
                        parameter.clone(),
                        txns.clone(),
                        last_txn_io.clone(),
                        mvmemory.clone(),
                        scheduler.clone(),
                    );
                    executor.run();
                });
            }
        });
        let mut result = Vec::with_capacity(txns_num);
        for idx in 0..txns_num {
            result.push(last_txn_io.take_output(idx));
        }
        if let Ok(mvmemory) = Arc::try_unwrap(mvmemory) {
            (result, mvmemory)
        } else {
            unreachable!()
        }
    }
}
