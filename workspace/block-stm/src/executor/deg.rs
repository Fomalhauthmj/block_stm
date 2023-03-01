use std::{cmp::min, collections::VecDeque, sync::Arc, thread::JoinHandle};

use tracing::error;

use crate::{
    core::{Transaction, VM},
    mvmemory::MVMap,
    scheduler::Scheduler,
    types::{AtomicBool, AtomicUsize},
};

use super::{last_txn_io::LastTxnsIO, Executor};
/// dynamic executor generator
pub(crate) struct DEG {
    upper_bound: usize,
    handle: DEGHandle,
}
impl DEG {
    pub fn new(upper_bound: usize) -> (Self, DEGHandle) {
        let handle = DEGHandle(Arc::new(AtomicUsize::new(0)));
        (
            Self {
                upper_bound,
                handle: handle.clone(),
            },
            handle,
        )
    }
    pub fn run<T, V>(
        &self,
        parameter: V::Parameter,
        txns: Arc<Vec<T>>,
        last_txn_io: Arc<LastTxnsIO<T::Key, T::Value, V::Output>>,
        mvmemory: Arc<MVMap<T::Key, T::Value>>,
        scheduler: Arc<Scheduler>,
    ) where
        T: Transaction,
        V: VM<T = T> + 'static,
    {
        let mut flags = VecDeque::new();
        let mut prev_demand = 0;
        loop {
            if scheduler.done() {
                break;
            }
            let curr_demand = self.handle.demand();
            // deg logic
            let idx = flags.len();
            if idx < min(self.upper_bound, curr_demand) {
                if let Some(thread) = self.spawn::<T, V>(
                    idx,
                    parameter.clone(),
                    txns.clone(),
                    last_txn_io.clone(),
                    mvmemory.clone(),
                    scheduler.clone(),
                ) {
                    flags.push_back(thread);
                }
            }
            if curr_demand < prev_demand {
                if let Some((thread, flag)) = flags.pop_front() {
                    flag.store(true);
                    let _ = thread.join();
                }
            }
            prev_demand = curr_demand;
        }
        flags.into_iter().for_each(|(_, flag)| flag.store(true));
    }
    pub fn spawn<T, V>(
        &self,
        idx: usize,
        parameter: V::Parameter,
        txns: Arc<Vec<T>>,
        last_txn_io: Arc<LastTxnsIO<T::Key, T::Value, V::Output>>,
        mvmemory: Arc<MVMap<T::Key, T::Value>>,
        scheduler: Arc<Scheduler>,
    ) -> Option<(JoinHandle<()>, Arc<AtomicBool>)>
    where
        T: Transaction,
        V: VM<T = T> + 'static,
    {
        let flag = Arc::new(AtomicBool::new(false));
        let shared_flag = flag.clone();
        let handle = self.handle.clone();
        match std::thread::Builder::new()
            .name(format!("dynamic_executor_{}", idx))
            .spawn(move || {
                let de = Executor::<T, V>::new(
                    parameter,
                    txns,
                    last_txn_io,
                    mvmemory,
                    scheduler,
                    handle,
                );
                de.run_with_flag(shared_flag);
            }) {
            Ok(handle) => Some((handle, flag)),
            Err(e) => {
                error!("create dynamic thread error:{}", e);
                None
            }
        }
    }
}
/// deg handle
#[derive(Clone)]
pub struct DEGHandle(Arc<AtomicUsize>);
impl DEGHandle {
    pub fn order(&self) {
        self.0.increment();
    }
    pub fn cancel(&self) {
        self.0.decrement();
    }
    pub fn demand(&self) -> usize {
        self.0.load()
    }
}
