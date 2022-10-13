#[cfg(feature = "tracing")]
use crate::rayon_trace;
use crate::{
    mvmemory::{MVMemory, MVMemoryError, MVMemoryReadOutput},
    scheduler::{Scheduler, Task},
    traits::{IsReadError, Storage, Transaction, VM},
    types::{TransactionIndex, Version},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tracing::instrument;
/// state view used by executor,which consisted of mvmemory and storage
pub struct ExecutorView<'a, T, S>
where
    T: Transaction,
    S: Storage<T = T>,
{
    txn_idx: TransactionIndex,
    mvmemory: &'a MVMemory<T::Key, T::Value>,
    storage: &'a S,
    read_set: Option<HashSet<(T::Key, Option<Version>)>>,
    write_set: Option<HashMap<T::Key, Arc<T::Value>>>,
}
impl<'a, T, S> ExecutorView<'a, T, S>
where
    T: Transaction,
    S: Storage<T = T>,
{
    pub fn new(
        txn_idx: TransactionIndex,
        mvmemory: &'a MVMemory<T::Key, T::Value>,
        storage: &'a S,
    ) -> Self {
        Self {
            txn_idx,
            mvmemory,
            storage,
            read_set: Some(HashSet::new()),
            write_set: Some(HashMap::new()),
        }
    }
    pub fn write(&mut self, key: T::Key, value: T::Value) {
        self.write_set
            .as_mut()
            .expect("write_set should exist.")
            .insert(key, Arc::new(value));
    }
    pub fn read(&mut self, key: &T::Key) -> Result<Arc<T::Value>, MVMemoryError> {
        match self
            .write_set
            .as_ref()
            .expect("write_set should exist.")
            .get(key)
        {
            Some(value) => Ok(value.clone()),
            None => match self.mvmemory.read(key, self.txn_idx) {
                Err(MVMemoryError::NotFound) => {
                    self.read_set
                        .as_mut()
                        .expect("read_set should exist.")
                        .insert((key.clone(), None));
                    Ok(Arc::new(self.storage.read(key)))
                }
                Err(MVMemoryError::ReadError(idx)) => Err(MVMemoryError::ReadError(idx)),
                Ok(MVMemoryReadOutput::Version(version, value)) => {
                    self.read_set
                        .as_mut()
                        .expect("read_set should exist.")
                        .insert((key.clone(), Some(version)));
                    Ok(value)
                }
            },
        }
    }
    /// FIXME: if ues `into_par_iter()`,sometimes executor will block here,
    /// and this executor will ignore current task and try to get next task,
    /// which results in the execution won't be finished.
    /// I guess it is due to work stealing of `rayon`.
    pub fn take_write_set(&mut self) -> Vec<(T::Key, Arc<T::Value>)> {
        self.write_set
            .take()
            .expect("write_set should exist.")
            .into_iter()
            .collect()
    }
    pub fn take_read_set(&mut self) -> Vec<(T::Key, Option<Version>)> {
        self.read_set
            .take()
            .expect("read_set should exist.")
            .into_iter()
            .collect()
    }
}
pub struct Executor<'a, T, V, S>
where
    T: Transaction,
    V: VM<T = T>,
    S: Storage<T = T>,
{
    vm: V,
    txns: &'a [T],
    view: &'a S,
    mvmemory: &'a MVMemory<T::Key, T::Value>,
    scheduler: &'a Scheduler,
}
impl<'a, T, V, S> Executor<'a, T, V, S>
where
    T: Transaction,
    S: Storage<T = T>,
    V: VM<T = T, S = S>,
{
    #[instrument(skip(self))]
    fn try_execute(&self, version: Version) -> Task {
        let (txn_idx, incarnation) = version;
        let txn = &self.txns[txn_idx];
        let mut executor_view = ExecutorView::new(txn_idx, self.mvmemory, self.view);
        match self.vm.execute(txn, &mut executor_view) {
            Ok(_output) => {
                let wrote_new_location = self.mvmemory.record(
                    version,
                    executor_view.take_read_set(),
                    executor_view.take_write_set(),
                );
                self.scheduler
                    .finish_execution(txn_idx, incarnation, wrote_new_location)
            }
            Err(e) if e.is_read_error() => {
                let blocking_txn_idx = e
                    .get_blocking_txn_idx()
                    .expect("blocking_txn_idx should exist");
                if !self.scheduler.add_dependency(txn_idx, blocking_txn_idx) {
                    return self.try_execute(version);
                }
                Task::None
            }
            // TODO: how to deal with other execute errors?
            _ => {
                unimplemented!()
            }
        }
    }
    #[instrument(skip(self))]
    fn needs_reexecution(&self, version: Version) -> Task {
        let (txn_idx, incarnation_number) = version;
        let read_set_valid = self.mvmemory.validate_read_set(txn_idx);
        let aborted = !read_set_valid
            && self
                .scheduler
                .try_validation_abort(txn_idx, incarnation_number);
        if aborted {
            self.mvmemory.convert_writes_to_estimates(txn_idx);
        }
        self.scheduler.finish_validation(txn_idx, aborted)
    }
}
impl<'a, T, V, S> Executor<'a, T, V, S>
where
    T: Transaction,
    S: Storage<T = T>,
    V: VM<T = T, S = S>,
{
    pub fn new(
        vm: V,
        txns: &'a [T],
        view: &'a S,
        mvmemory: &'a MVMemory<T::Key, T::Value>,
        scheduler: &'a Scheduler,
    ) -> Self {
        Self {
            vm,
            txns,
            view,
            mvmemory,
            scheduler,
        }
    }
    pub fn run(&self) {
        let mut task = Task::None;
        while !self.scheduler.done() {
            task = match task {
                Task::Execution(version) => self.try_execute(version),
                Task::Validation(version) => self.needs_reexecution(version),
                Task::None => self.scheduler.next_task(),
            };
            #[cfg(feature = "tracing")]
            if task != Task::None {
                rayon_trace!("get task = {:?}", task);
            }
        }
    }
}
