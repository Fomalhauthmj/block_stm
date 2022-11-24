use std::{cmp::min, collections::VecDeque, sync::Arc, thread::JoinHandle};

use tracing::error;

use crate::{
    core::{Transaction, TransactionOutput, VM},
    mvmemory::{MVMemory, MVMemoryView},
    scheduler::{Scheduler, SchedulerTask, TaskGuard},
    types::{AtomicBool, AtomicUsize, Version},
};
pub struct DEG {
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
        mvmemory: Arc<MVMemory<T::Key, T::Value>>,
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
        mvmemory: Arc<MVMemory<T::Key, T::Value>>,
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
                let de = Executor::<T, V>::new(parameter, txns, mvmemory, scheduler, handle);
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
/// executor
pub struct Executor<T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    txns: Arc<Vec<T>>,
    mvmemory: Arc<MVMemory<T::Key, T::Value>>,
    scheduler: Arc<Scheduler>,
    deg: DEGHandle,
    vm: V,
}
/// public methods used by parallel executor
impl<T, V> Executor<T, V>
where
    T: Transaction,
    V: VM<T = T> + 'static,
{
    pub fn new(
        parameter: V::Parameter,
        txns: Arc<Vec<T>>,
        mvmemory: Arc<MVMemory<T::Key, T::Value>>,
        scheduler: Arc<Scheduler>,
        deg: DEGHandle,
    ) -> Self {
        let vm = V::new(parameter);
        Self {
            txns,
            mvmemory,
            scheduler,
            deg,
            vm,
        }
    }
    pub fn run(&self) {
        let mut task = SchedulerTask::NoTask;
        loop {
            task = match task {
                SchedulerTask::Execution(version, None, guard) => self.try_execute(version, guard),
                SchedulerTask::Execution(_, Some(condvar), _) => {
                    condvar.notify_one();
                    SchedulerTask::NoTask
                }
                SchedulerTask::Validation(version, guard) => self.try_validate(version, guard),
                SchedulerTask::NoTask => self.scheduler.next_task(),
                SchedulerTask::Done => break,
            }
        }
    }
    pub fn run_with_flag(&self, flag: Arc<AtomicBool>) {
        let mut task = SchedulerTask::NoTask;
        loop {
            task = match task {
                SchedulerTask::Execution(version, None, guard) => self.try_execute(version, guard),
                SchedulerTask::Execution(_, Some(condvar), _) => {
                    condvar.notify_one();
                    SchedulerTask::NoTask
                }
                SchedulerTask::Validation(version, guard) => self.try_validate(version, guard),
                SchedulerTask::NoTask => {
                    if flag.load() {
                        break;
                    }
                    self.scheduler.next_task()
                }
                SchedulerTask::Done => break,
            }
        }
    }
}
/// private methods used by executor itself
impl<T, V> Executor<T, V>
where
    T: Transaction,
    V: VM<T = T> + 'static,
{
    fn try_execute<'b>(&self, version: Version, guard: TaskGuard<'b>) -> SchedulerTask<'b> {
        let (txn_idx, incarnation) = version;
        let txn = &self.txns[txn_idx];
        let mut mvmeory_view = MVMemoryView::new(
            self.deg.clone(),
            txn_idx,
            self.mvmemory.clone(),
            self.scheduler.clone(),
        );
        match self.vm.execute_transaction(txn, &mvmeory_view) {
            Ok(output) => {
                let wrote_new_location = self.mvmemory.record(
                    version,
                    mvmeory_view.take_read_set(),
                    output.get_write_set(),
                );
                self.scheduler
                    .finish_execution(txn_idx, incarnation, wrote_new_location, guard)
            }
            Err(_e) => {
                // TODO: how to deal with execute errors?
                unimplemented!()
            }
        }
    }
    fn try_validate<'b>(&self, version: Version, guard: TaskGuard<'b>) -> SchedulerTask<'b> {
        let (txn_idx, incarnation) = version;
        let read_set_valid = self.mvmemory.validate_read_set(txn_idx);
        let aborted = !read_set_valid && self.scheduler.abort(txn_idx, incarnation);
        if aborted {
            self.mvmemory.convert_writes_to_estimates(txn_idx);
        }
        self.scheduler.finish_validation(txn_idx, aborted, guard)
    }
}
