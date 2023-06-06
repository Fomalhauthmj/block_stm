use crossbeam::utils::CachePadded;

use crate::types::{AtomicBool, AtomicUsize, Condvar, Incarnation, Mutex, TxnIndex, Version};
use std::{cmp::min, hint, sync::atomic::Ordering};
/// scheduler
pub struct Scheduler {
    block_size: usize,
    execution_idx: AtomicUsize,
    validation_idx: AtomicUsize,
    num_active_tasks: AtomicUsize,
    decrease_cnt: AtomicUsize,
    done_marker: AtomicBool,
    txn_dependency: Vec<CachePadded<Mutex<Vec<TxnIndex>>>>,
    txn_status: Vec<CachePadded<Mutex<TransactionStatus>>>,
}
/// public methods used by other components
impl Scheduler {
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            execution_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(0),
            num_active_tasks: AtomicUsize::new(0),
            decrease_cnt: AtomicUsize::new(0),
            done_marker: AtomicBool::new(false),
            txn_dependency: (0..block_size)
                .map(|_| CachePadded::new(Mutex::new(vec![])))
                .collect(),
            txn_status: (0..block_size)
                .map(|_| CachePadded::new(Mutex::new(TransactionStatus::ReadyToExecute(0, None))))
                .collect(),
        }
    }
    pub fn done(&self) -> bool {
        self.done_marker.load()
    }
    pub fn next_task(&self) -> SchedulerTask {
        loop {
            if self.done() {
                return SchedulerTask::Done;
            }
            let idx_to_execute = self.execution_idx.load();
            let idx_to_validate = self.validation_idx.load();
            if idx_to_execute < idx_to_validate {
                if let Some((version, condvar, guard)) = self.next_version_to_execute() {
                    return SchedulerTask::Execution(version, condvar, guard);
                }
            } else if let Some((version, guard)) = self.next_version_to_validate() {
                return SchedulerTask::Validation(version, guard);
            }
        }
    }
    pub fn abort(&self, txn_idx: TxnIndex, incarnation: Incarnation) -> bool {
        let mut guard = self.txn_status[txn_idx].lock();
        if TransactionStatus::Executed(incarnation) == *guard {
            *guard = TransactionStatus::Aborting(incarnation);
            true
        } else {
            false
        }
    }
    pub fn wait_for_dependency(
        &self,
        txn_idx: TxnIndex,
        blocking_txn_idx: TxnIndex,
    ) -> Option<Condvar> {
        let condvar = Condvar::new();
        {
            let mut dependency_guard = self.txn_dependency[blocking_txn_idx].lock();
            if self.is_executed(blocking_txn_idx).is_some() {
                return None;
            }
            self.suspend(txn_idx, condvar.clone());
            dependency_guard.push(txn_idx);
        }
        Some(condvar)
    }
    pub fn finish_execution<'a>(
        &self,
        txn_idx: TxnIndex,
        incarnation: Incarnation,
        wrote_new_path: bool,
        guard: TaskGuard<'a>,
    ) -> SchedulerTask<'a> {
        self.set_executed_status(txn_idx);
        let deps = {
            let mut guard = self.txn_dependency[txn_idx].lock();
            std::mem::take(&mut *guard)
        };
        self.resume_dependencies(deps);
        if self.validation_idx.load() > txn_idx {
            if wrote_new_path {
                self.decrease_validation_idx(txn_idx);
            } else {
                return SchedulerTask::Validation((txn_idx, incarnation), guard);
            }
        }
        SchedulerTask::NoTask
    }
    pub fn finish_validation<'a>(
        &self,
        txn_idx: TxnIndex,
        aborted: bool,
        guard: TaskGuard<'a>,
    ) -> SchedulerTask<'a> {
        if aborted {
            self.set_ready_status(txn_idx);
            self.decrease_validation_idx(txn_idx + 1);
            if self.execution_idx.load() > txn_idx {
                if let Some((incarnation, condvar)) = self.try_incarnate(txn_idx) {
                    return SchedulerTask::Execution((txn_idx, incarnation), condvar, guard);
                }
            }
        }
        SchedulerTask::NoTask
    }
}
/// private methods used by scheduler itself
impl Scheduler {
    fn decrease_execution_idx(&self, target_idx: usize) {
        self.execution_idx.fetch_min(target_idx, Ordering::SeqCst);
        self.decrease_cnt.increment();
    }
    fn decrease_validation_idx(&self, target_idx: usize) {
        self.validation_idx.fetch_min(target_idx, Ordering::SeqCst);
        self.decrease_cnt.increment();
    }
    fn check_done(&self) -> bool {
        let observed_cnt = self.decrease_cnt.load();
        let execution_idx = self.execution_idx.load();
        let validation_idx = self.validation_idx.load();
        let num_active_tasks = self.num_active_tasks.load();
        if min(execution_idx, validation_idx) < self.block_size || num_active_tasks > 0 {
            return false;
        }
        if observed_cnt == self.decrease_cnt.load() {
            // TODO: `aptos-core` use release order,why?
            self.done_marker.store(true);
            true
        } else {
            false
        }
    }
    fn try_incarnate(&self, txn_idx: TxnIndex) -> Option<(Incarnation, Option<Condvar>)> {
        if txn_idx < self.block_size {
            let mut guard = self.txn_status[txn_idx].lock();
            if let TransactionStatus::ReadyToExecute(incarnation, condvar) = &*guard {
                let result = Some((*incarnation, condvar.clone()));
                *guard = TransactionStatus::Executing(*incarnation);
                return result;
            }
        }
        None
    }
    fn next_version_to_execute(&self) -> Option<(Version, Option<Condvar>, TaskGuard)> {
        let idx_to_execute = self.execution_idx.load();
        if idx_to_execute >= self.block_size {
            if !self.check_done() {
                hint::spin_loop();
            }
            return None;
        }
        let guard = TaskGuard::new(&self.num_active_tasks);
        let idx_to_execute = self.execution_idx.increment();
        self.try_incarnate(idx_to_execute)
            .map(|(incarnation, condvar)| ((idx_to_execute, incarnation), condvar, guard))
    }
    fn next_version_to_validate(&self) -> Option<(Version, TaskGuard)> {
        let idx_to_validate = self.validation_idx.load();
        if idx_to_validate >= self.block_size {
            if !self.check_done() {
                hint::spin_loop();
            }
            return None;
        }
        let guard = TaskGuard::new(&self.num_active_tasks);
        let idx_to_validate = self.validation_idx.increment();
        self.is_executed(idx_to_validate)
            .map(|incarnation| ((idx_to_validate, incarnation), guard))
    }
    fn resume_dependencies(&self, dependent_txn_indices: Vec<TxnIndex>) {
        let min_dep = dependent_txn_indices
            .into_iter()
            .map(|dep| {
                self.resume(dep);
                dep
            })
            .min();
        if let Some(min_dep) = min_dep {
            self.decrease_execution_idx(min_dep);
        }
    }
}
/// private methods used by scheduler itself to change transaction status
impl Scheduler {
    fn is_executed(&self, txn_idx: TxnIndex) -> Option<Incarnation> {
        if txn_idx >= self.block_size {
            return None;
        }
        let guard = self.txn_status[txn_idx].lock();
        if let TransactionStatus::Executed(incarnation) = *guard {
            Some(incarnation)
        } else {
            None
        }
    }
    fn suspend(&self, txn_idx: TxnIndex, condvar: Condvar) {
        let mut guard = self.txn_status[txn_idx].lock();
        if let TransactionStatus::Executing(incarnation) = *guard {
            *guard = TransactionStatus::Suspended(incarnation, condvar);
        } else {
            unreachable!()
        }
    }
    fn resume(&self, txn_idx: TxnIndex) {
        let mut guard = self.txn_status[txn_idx].lock();
        if let TransactionStatus::Suspended(incarnation, condvar) = &*guard {
            *guard = TransactionStatus::ReadyToExecute(*incarnation, Some(condvar.clone()));
        } else {
            unreachable!()
        }
    }
    fn set_executed_status(&self, txn_idx: TxnIndex) {
        let mut guard = self.txn_status[txn_idx].lock();
        if let TransactionStatus::Executing(incarnation) = *guard {
            *guard = TransactionStatus::Executed(incarnation);
        } else {
            unreachable!()
        }
    }
    fn set_ready_status(&self, txn_idx: TxnIndex) {
        let mut guard = self.txn_status[txn_idx].lock();
        if let TransactionStatus::Aborting(incarnation) = *guard {
            *guard = TransactionStatus::ReadyToExecute(incarnation + 1, None);
        } else {
            unreachable!()
        }
    }
}
/// transaction status,`Suspended` and `Condvar` are used to suspend and resume transaction(due to dependency)
enum TransactionStatus {
    ReadyToExecute(Incarnation, Option<Condvar>),
    Executing(Incarnation),
    Suspended(Incarnation, Condvar),
    Executed(Incarnation),
    Aborting(Incarnation),
}
impl PartialEq for TransactionStatus {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ReadyToExecute(l0, _), Self::ReadyToExecute(r0, _)) => l0 == r0,
            (Self::Executing(l0), Self::Executing(r0)) => l0 == r0,
            (Self::Suspended(l0, _), Self::Suspended(r0, _)) => l0 == r0,
            (Self::Executed(l0), Self::Executed(r0)) => l0 == r0,
            (Self::Aborting(l0), Self::Aborting(r0)) => l0 == r0,
            _ => false,
        }
    }
}
/// scheduler task guard,used to track the number of active scheduler tasks
pub struct TaskGuard<'a> {
    inner: &'a AtomicUsize,
}
impl<'a> TaskGuard<'a> {
    pub fn new(atomic: &'a AtomicUsize) -> Self {
        atomic.increment();
        Self { inner: atomic }
    }
}
impl<'a> Drop for TaskGuard<'a> {
    fn drop(&mut self) {
        self.inner.decrement();
    }
}
/// scheduler task type
pub enum SchedulerTask<'a> {
    Execution(Version, Option<Condvar>, TaskGuard<'a>),
    Validation(Version, TaskGuard<'a>),
    NoTask,
    Done,
}
impl<'a> std::fmt::Debug for SchedulerTask<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Execution(arg0, None, _) => write!(f, "Execution({:?})", arg0),
            Self::Execution(arg0, Some(_), _) => write!(f, "Hook to resume Execution({:?})", arg0),
            Self::Validation(arg0, _) => write!(f, "Validation({:?})", arg0),
            Self::NoTask => write!(f, "NoTask"),
            Self::Done => write!(f, "Done"),
        }
    }
}
