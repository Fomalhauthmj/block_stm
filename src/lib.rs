#![deny(missing_docs)]
//! block_stm implementation
/// abstract traits,used to implement user own execution engine
pub mod core;
mod executor;
mod mvmemory;
mod scheduler;
/// test utils used by benches and tests
pub mod test_utils;
mod types;

use crate::core::{Transaction, VM};
use executor::{Executor, DEG};
use mvmemory::MVMemory;
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
    ) -> Vec<(T::Key, Option<T::Value>)> {
        let txns = Arc::new(txns);
        let txns_num = txns.len();
        let mvmemory = Arc::new(MVMemory::new(txns_num));
        let scheduler = Arc::new(Scheduler::new(txns_num));
        let (deg, deg_handle) = DEG::new(self.concurrency_level);
        {
            let parameter = parameter.clone();
            let txns = txns.clone();
            let mvmemory = mvmemory.clone();
            let scheduler = scheduler.clone();
            let _ = std::thread::Builder::new()
                .name("deg".to_string())
                .spawn(move || {
                    deg.run::<T, V>(parameter, txns, mvmemory, scheduler);
                });
        }
        RAYON_EXEC_POOL.scope(|s| {
            for _ in 0..self.concurrency_level {
                s.spawn(|_| {
                    let executor = Executor::<T, V>::new(
                        parameter.clone(),
                        txns.clone(),
                        mvmemory.clone(),
                        scheduler.clone(),
                        deg_handle.clone(),
                    );
                    executor.run();
                });
            }
        });
        mvmemory.snapshot()
    }
    /// execute transactions for benchmark
    pub fn execute_transactions_benchmark(
        &self,
        txns: Vec<T>,
        parameter: V::Parameter,
    ) -> (
        Vec<(T::Key, Option<T::Value>)>,
        std::time::Duration,
        std::time::Duration,
    ) {
        use std::time::Instant;

        let txns = Arc::new(txns);
        let txns_num = txns.len();
        let mvmemory = Arc::new(MVMemory::new(txns_num));
        let scheduler = Arc::new(Scheduler::new(txns_num));
        let (deg, deg_handle) = DEG::new(self.concurrency_level);
        {
            let parameter = parameter.clone();
            let txns = txns.clone();
            let mvmemory = mvmemory.clone();
            let scheduler = scheduler.clone();
            let _ = std::thread::spawn(move || {
                deg.run::<T, V>(parameter, txns, mvmemory, scheduler);
            });
        }

        let execute_start = Instant::now();

        RAYON_EXEC_POOL.scope(|s| {
            for _ in 0..self.concurrency_level {
                s.spawn(|_| {
                    let executor = Executor::<T, V>::new(
                        parameter.clone(),
                        txns.clone(),
                        mvmemory.clone(),
                        scheduler.clone(),
                        deg_handle.clone(),
                    );
                    executor.run();
                });
            }
        });

        let execute_end = execute_start.elapsed();

        let collect_start = Instant::now();

        let result = mvmemory.snapshot();

        let collect_end = collect_start.elapsed();

        (result, execute_end, collect_end)
    }
}
