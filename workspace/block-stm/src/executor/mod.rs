use std::sync::Arc;

use tracing::instrument;

use crate::{
    core::{Transaction, TransactionOutput, VM},
    mvmemory::{MVMap, MVMapView},
    scheduler::{Scheduler, SchedulerTask, TaskGuard},
    types::Version,
};

use self::last_txn_io::LastTxnsIO;

pub(crate) mod last_txn_io;
/// executor
pub(crate) struct Executor<T, V>
where
    T: Transaction,
    V: VM<T = T>,
{
    txns: Arc<Vec<T>>,
    last_txn_io: Arc<LastTxnsIO<T::Key, T::Value, V::Output>>,
    mvmemory: Arc<MVMap<T::Key, T::Value>>,
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
        last_txn_io: Arc<LastTxnsIO<T::Key, T::Value, V::Output>>,
        mvmemory: Arc<MVMap<T::Key, T::Value>>,
        scheduler: Arc<Scheduler>,
    ) -> Self {
        let vm = V::new(parameter);
        Self {
            txns,
            last_txn_io,
            mvmemory,
            scheduler,
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
}
/// private methods used by executor itself
impl<T, V> Executor<T, V>
where
    T: Transaction,
    V: VM<T = T> + 'static,
{
    #[instrument(level = "trace", skip(self, guard))]
    fn try_execute<'b>(&self, version: Version, guard: TaskGuard<'b>) -> SchedulerTask<'b> {
        #[cfg(feature = "tracing")]
        let try_execute = std::time::Instant::now();

        let (txn_idx, incarnation) = version;
        let txn = &self.txns[txn_idx];
        let mut mvmeory_view =
            MVMapView::new(txn_idx, self.mvmemory.clone(), self.scheduler.clone());
        match self.vm.execute_transaction(txn, &mvmeory_view) {
            Ok(output) => {
                let last_write_set = self.last_txn_io.last_modified_keys(txn_idx);
                let wrote_new_location = self.mvmemory.apply(
                    version,
                    last_write_set,
                    output.get_write_set(),
                    output.get_delta_set(),
                );
                self.last_txn_io
                    .record(txn_idx, mvmeory_view.take_read_set(), output);
                let next_task = self.scheduler.finish_execution(
                    txn_idx,
                    incarnation,
                    wrote_new_location,
                    guard,
                );
                #[cfg(feature = "tracing")]
                tracing::trace!("try_execute = {:?}", try_execute.elapsed().as_nanos());
                next_task
            }
            Err(_e) => {
                // TODO: how to deal with execute errors?
                unimplemented!()
            }
        }
    }
    #[instrument(level = "trace", skip(self, guard))]
    fn try_validate<'b>(&self, version: Version, guard: TaskGuard<'b>) -> SchedulerTask<'b> {
        #[cfg(feature = "tracing")]
        let try_validate = std::time::Instant::now();

        let (txn_idx, incarnation) = version;

        let read_set_valid = {
            let read_set = self.last_txn_io.last_read_set(txn_idx);
            if let Some(read_set) = read_set {
                self.mvmemory.validate_read_set(txn_idx, &read_set)
            } else {
                unreachable!()
            }
        };

        let aborted = !read_set_valid && self.scheduler.abort(txn_idx, incarnation);
        if aborted {
            let write_set = self.last_txn_io.last_modified_keys(txn_idx);
            for key in &write_set {
                self.mvmemory.mark_estimate(key, txn_idx)
            }
        }
        let next_task = self.scheduler.finish_validation(txn_idx, aborted, guard);
        #[cfg(feature = "tracing")]
        tracing::trace!("try_validate = {:?}", try_validate.elapsed().as_nanos());
        next_task
    }
}
