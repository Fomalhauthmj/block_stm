use std::sync::Arc;

use crate::{
    core::{Transaction, TransactionOutput, VM},
    mvmemory::{MVMemory, MVMemoryView},
    scheduler::{Scheduler, SchedulerTask, TaskGuard},
    types::{AtomicBool, Version},
    ACTIVE_STEALING_WORKER, STEALING_WORKER_LIMIT,
};
/// executor
pub struct Executor<T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    parameter: V::Parameter,
    txns: Arc<Vec<T>>,
    mvmemory: Arc<MVMemory<T::Key, T::Value>>,
    scheduler: Arc<Scheduler>,
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
    ) -> Self {
        let vm = V::new(parameter.clone());
        Self {
            parameter,
            txns,
            mvmemory,
            scheduler,
            vm,
        }
    }
    pub fn run(&self) {
        let mut task = SchedulerTask::NoTask;
        loop {
            crate::rayon_trace!("get task = {:?}", task);
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
            self.parameter.clone(),
            txn_idx,
            self.txns.clone(),
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
///
pub struct StealingExecutor<T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    parameter: V::Parameter,
    txns: Arc<Vec<T>>,
    mvmemory: Arc<MVMemory<T::Key, T::Value>>,
    scheduler: Arc<Scheduler>,
    vm: V,
}
/// public methods used by parallel executor
impl<T, V> StealingExecutor<T, V>
where
    T: Transaction,
    V: VM<T = T> + 'static,
{
    pub fn new(
        parameter: V::Parameter,
        txns: Arc<Vec<T>>,
        mvmemory: Arc<MVMemory<T::Key, T::Value>>,
        scheduler: Arc<Scheduler>,
    ) -> Self {
        let vm = V::new(parameter.clone());
        Self {
            parameter,
            txns,
            mvmemory,
            scheduler,
            vm,
        }
    }
    pub fn stealing_run(&self, flag: Arc<AtomicBool>) {
        let mut task = SchedulerTask::NoTask;
        loop {
            crate::rayon_trace!(
                "get stealing task = {:?},stealing workers = {}/{}",
                task,
                ACTIVE_STEALING_WORKER.load(),
                *STEALING_WORKER_LIMIT
            );
            task = match task {
                SchedulerTask::Execution(version, None, guard) => self.try_execute(version, guard),
                SchedulerTask::Execution(_, Some(condvar), _) => {
                    condvar.notify_one();
                    SchedulerTask::NoTask
                }
                SchedulerTask::Validation(version, guard) => self.try_validate(version, guard),
                SchedulerTask::NoTask => {
                    crate::rayon_trace!("try to read flag");
                    if flag.load() {
                        // remove worker
                        break;
                    }
                    crate::rayon_trace!("try to sleep");
                    std::thread::sleep(std::time::Duration::from_micros(200));
                    crate::rayon_trace!("will try to steal next task");
                    self.scheduler.stealing_next_task()
                }
                SchedulerTask::Done => break,
            };
            crate::rayon_trace!("next stealing loop");
        }
        ACTIVE_STEALING_WORKER.decrement();
    }
}
/// private methods used by executor itself
impl<T, V> StealingExecutor<T, V>
where
    T: Transaction,
    V: VM<T = T> + 'static,
{
    fn try_execute<'b>(&self, version: Version, guard: TaskGuard<'b>) -> SchedulerTask<'b> {
        let (txn_idx, incarnation) = version;
        let txn = &self.txns[txn_idx];
        let mut mvmeory_view = MVMemoryView::new(
            self.parameter.clone(),
            txn_idx,
            self.txns.clone(),
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
