use crate::{
    core::{Transaction, TransactionOutput, VM},
    mvmemory::{MVMemory, MVMemoryView},
    scheduler::{Scheduler, SchedulerTask, TaskGuard},
    types::Version,
};
/// executor
pub struct Executor<'a, T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    vm: V,
    txns: &'a [T],
    mvmemory: &'a MVMemory<T::Key, T::Value>,
    scheduler: &'a Scheduler,
}
/// public methods used by parallel executor
impl<'a, T, V> Executor<'a, T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    pub fn new(
        parameter: V::Parameter,
        txns: &'a [T],
        mvmemory: &'a MVMemory<T::Key, T::Value>,
        scheduler: &'a Scheduler,
    ) -> Self {
        let vm = V::new(parameter);
        Self {
            vm,
            txns,
            mvmemory,
            scheduler,
        }
    }
    pub fn run(&self) {
        let mut task = SchedulerTask::NoTask;
        loop {
            #[cfg(feature = "tracing")]
            {
                if let SchedulerTask::NoTask = task {
                } else {
                    crate::rayon_trace!("get task = {:?}", task);
                }
            }
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
impl<'a, T, V> Executor<'a, T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    fn try_execute<'b>(&self, version: Version, guard: TaskGuard<'b>) -> SchedulerTask<'b> {
        let (txn_idx, incarnation) = version;
        let txn = &self.txns[txn_idx];
        let mut mvmeory_view = MVMemoryView::new(txn_idx, self.mvmemory, self.scheduler);
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
