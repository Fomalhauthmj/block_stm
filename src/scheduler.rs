use std::{cmp::min, sync::atomic::Ordering};

use crate::{
    rayon_debug, rayon_error, rayon_trace, rayon_warn,
    sync::{AtomicBool, AtomicUsize, Mutex},
    types::{IncarnationNumber, TransactionIndex, Version},
};
use tracing::instrument;

pub struct Scheduler {
    execution_idx: AtomicUsize,
    validation_idx: AtomicUsize,
    decrease_cnt: AtomicUsize,
    num_active_tasks: AtomicUsize,
    done_marker: AtomicBool,
    txn_dependency: Vec<Mutex<Vec<TransactionIndex>>>,
    txn_status: Vec<Mutex<TransactionStatus>>,
    block_size: usize,
}
#[derive(PartialEq, Eq, Debug)]
enum TransactionStatus {
    ReadyToExecute(IncarnationNumber),
    Executing(IncarnationNumber),
    Executed(IncarnationNumber),
    Aborting(IncarnationNumber),
}
impl TransactionStatus {
    fn incarnation_number(&self) -> IncarnationNumber {
        match self {
            TransactionStatus::ReadyToExecute(i) => *i,
            TransactionStatus::Executing(i) => *i,
            TransactionStatus::Executed(i) => *i,
            TransactionStatus::Aborting(i) => *i,
        }
    }
    #[instrument(skip(self))]
    pub fn ready_to_execute(&mut self) {
        rayon_trace!("prev TransactionStatus = {:?}", self);
        *self = Self::ReadyToExecute(self.incarnation_number())
    }
    #[instrument(skip(self))]
    pub fn executing(&mut self) {
        rayon_trace!("prev TransactionStatus = {:?}", self);
        *self = Self::Executing(self.incarnation_number())
    }
    #[instrument(skip(self))]
    pub fn executed(&mut self) {
        rayon_trace!("prev TransactionStatus = {:?}", self);
        *self = Self::Executed(self.incarnation_number())
    }
    #[instrument(skip(self))]
    pub fn aborting(&mut self) {
        rayon_trace!("prev TransactionStatus = {:?}", self);
        *self = Self::Aborting(self.incarnation_number())
    }
}
#[derive(PartialEq, Eq, Debug)]
pub enum Task {
    Execution(Version),
    Validation(Version),
    None,
}
impl Scheduler {
    #[instrument(skip(self))]
    fn decrease_execution_idx(&self, target_idx: usize) {
        let min_val = self
            .execution_idx
            .fetch_min(target_idx, Ordering::SeqCst)
            .min(target_idx);
        self.decrease_cnt.increment();
        rayon_warn!(
            "execution_idx should = {}, current execution_idx = {}",
            min_val,
            self.execution_idx.load()
        );
    }
    #[instrument(skip(self))]
    fn decrease_validation_idx(&self, target_idx: usize) {
        let min_val = self
            .validation_idx
            .fetch_min(target_idx, Ordering::SeqCst)
            .min(target_idx);
        self.decrease_cnt.increment();
        rayon_warn!(
            "validation_idx should = {}, current validation_idx = {}",
            min_val,
            self.validation_idx.load()
        );
    }
    #[instrument(skip(self))]
    fn check_done(&self) -> bool {
        // from aptos-core
        let observed_cnt = self.decrease_cnt.load();

        let validation_idx = self.validation_idx.load();
        let execution_idx = self.execution_idx.load();
        let num_tasks = self.num_active_tasks.load();
        if min(execution_idx, validation_idx) < self.block_size || num_tasks > 0 {
            // There is work remaining.
            return false;
        }

        // Re-read and make sure decrease_cnt hasn't changed.
        if observed_cnt == self.decrease_cnt.load() {
            // why release order
            self.done_marker.store(true);
            rayon_error!("check done");
            true
        } else {
            false
        }
    }
    #[instrument(skip(self))]
    fn try_incarnate(&self, txn_idx: TransactionIndex) -> Task {
        if txn_idx < self.block_size {
            let mut guard = self.txn_status[txn_idx].lock();
            if let TransactionStatus::ReadyToExecute(incarnation_number) = *guard {
                guard.executing();
                return Task::Execution(Some((txn_idx, incarnation_number)));
            }
        }
        self.num_active_tasks.decrement();
        Task::None
    }
    #[instrument(skip(self))]
    fn next_version_to_execute(&self) -> Task {
        if self.execution_idx.load() >= self.block_size {
            self.check_done();
            return Task::None;
        }
        self.num_active_tasks.increment();
        let idx_to_execute = self.execution_idx.increment();
        self.try_incarnate(idx_to_execute)
    }
    #[instrument(skip(self))]
    fn next_version_to_validate(&self) -> Task {
        if self.validation_idx.load() >= self.block_size {
            self.check_done();
            return Task::None;
        }
        self.num_active_tasks.increment();
        let idx_to_validate = self.validation_idx.increment();
        if idx_to_validate < self.block_size {
            let guard = self.txn_status[idx_to_validate].lock();
            if let TransactionStatus::Executed(incarnation_number) = *guard {
                return Task::Validation(Some((idx_to_validate, incarnation_number)));
            }
        }
        self.num_active_tasks.decrement();
        Task::None
    }
    #[instrument(skip(self))]
    fn set_ready_status(&self, txn_idx: TransactionIndex) {
        let mut guard = self.txn_status[txn_idx].lock();
        if let TransactionStatus::Aborting(incarnation_number) = *guard {
            // Note: this transaction status change won't be logged.
            *guard = TransactionStatus::ReadyToExecute(incarnation_number + 1);
        } else {
            unreachable!()
        }
    }
    #[instrument(skip(self))]
    fn resume_dependencies(&self, dependent_txn_indices: Vec<TransactionIndex>) {
        for dep_txn_idx in &dependent_txn_indices {
            self.set_ready_status(*dep_txn_idx);
        }
        let min_dependency_idx = dependent_txn_indices.iter().min();
        if let Some(min_dependency_idx) = min_dependency_idx {
            self.decrease_execution_idx(*min_dependency_idx);
        }
    }
}
impl Scheduler {
    pub fn new(block_size: usize) -> Self {
        let mut txn_dependency = Vec::new();
        let mut txn_status = Vec::new();
        for i in 0..block_size {
            txn_dependency.insert(i, Mutex::new(vec![]));
            txn_status.insert(i, Mutex::new(TransactionStatus::ReadyToExecute(0)));
        }
        Self {
            execution_idx: AtomicUsize::new("execution_idx", 0),
            validation_idx: AtomicUsize::new("validation_idx", 0),
            decrease_cnt: AtomicUsize::new("decrease_cnt", 0),
            num_active_tasks: AtomicUsize::new("num_active_tasks", 0),
            done_marker: AtomicBool::new(false),
            txn_dependency,
            txn_status,
            block_size,
        }
    }
    #[instrument(skip(self))]
    pub fn done(&self) -> bool {
        self.done_marker.load()
    }
    #[instrument(skip(self))]
    pub fn next_task(&self) -> Task {
        if self.validation_idx.load() < self.execution_idx.load() {
            let version_to_validate = self.next_version_to_validate();
            if version_to_validate != Task::None {
                return version_to_validate;
            }
        } else {
            let version_to_execute = self.next_version_to_execute();
            if version_to_execute != Task::None {
                return version_to_execute;
            }
        }
        Task::None
    }
    #[instrument(skip(self))]
    pub fn add_dependency(
        &self,
        txn_idx: TransactionIndex,
        blocking_txn_idx: TransactionIndex,
    ) -> bool {
        {
            let mut dependency_guard = self.txn_dependency[blocking_txn_idx].lock();
            {
                let status_guard = self.txn_status[blocking_txn_idx].lock();
                if let TransactionStatus::Executed(_) = *status_guard {
                    return false;
                }
            }
            let mut status_guard = self.txn_status[txn_idx].lock();
            if let TransactionStatus::Executing(_) = *status_guard {
                status_guard.aborting();
                dependency_guard.push(txn_idx);
            } else {
                unreachable!()
            }
        }
        self.num_active_tasks.decrement();
        true
    }
    #[instrument(skip(self))]
    pub fn finish_execution(
        &self,
        txn_idx: TransactionIndex,
        incarnation_number: IncarnationNumber,
        wrote_new_path: bool,
    ) -> Task {
        {
            let mut status_guard = self.txn_status[txn_idx].lock();
            if let TransactionStatus::Executing(_) = *status_guard {
                status_guard.executed();
            } else {
                unreachable!()
            }
        }
        let deps = {
            let mut guard = self.txn_dependency[txn_idx].lock();
            std::mem::take(&mut *guard)
        };
        self.resume_dependencies(deps);
        if self.validation_idx.load() > txn_idx {
            if wrote_new_path {
                rayon_debug!("wrote_new_path");
                self.decrease_validation_idx(txn_idx);
            } else {
                rayon_debug!("execution finished and return validation task directly");
                return Task::Validation(Some((txn_idx, incarnation_number)));
            }
        }
        self.num_active_tasks.decrement();
        rayon_debug!("execution finished");
        Task::None
    }
    #[instrument(skip(self))]
    pub fn try_validation_abort(
        &self,
        txn_idx: TransactionIndex,
        incarnation_number: IncarnationNumber,
    ) -> bool {
        let mut guard = self.txn_status[txn_idx].lock();
        if let TransactionStatus::Executed(_) = *guard {
            guard.aborting();
            return true;
        }
        false
    }
    #[instrument(skip(self))]
    pub fn finish_validation(&self, txn_idx: TransactionIndex, aborted: bool) -> Task {
        if aborted {
            rayon_debug!("aborted");
            self.set_ready_status(txn_idx);
            self.decrease_validation_idx(txn_idx + 1);
            if self.execution_idx.load() > txn_idx {
                // avoid double num_active_tasks decrement (when new_version = None)
                return self.try_incarnate(txn_idx);
            }
        }
        self.num_active_tasks.decrement();
        rayon_debug!("validation finished");
        Task::None
    }
}
