use std::{hash::Hash, sync::Arc};

use crate::{
    core::Mergeable,
    executor::deg::DEGHandle,
    scheduler::Scheduler,
    types::{Mutex, TxnIndex, Version},
};

use super::map::{MVMap, MVMapReadError, MVMapReadOutput};

/// read type captured by mvmap view
#[derive(Debug, PartialEq, Eq)]
enum ReadType<Value>
where
    Value: Mergeable,
{
    Version(Version),
    Storage,
    Merged(Arc<Value>),
    PartialMerged(Value::Delta),
    Unmerged(Vec<Value::Delta>),
}
/// read descriptor captured by mvmap view
pub struct ReadDescriptor<Key, Value>
where
    Value: Mergeable,
{
    key: Key,
    read_type: ReadType<Value>,
}
impl<Key, Value> ReadDescriptor<Key, Value>
where
    Value: Mergeable,
{
    pub fn new_version(key: Key, version: Version) -> Self {
        Self {
            key,
            read_type: ReadType::Version(version),
        }
    }
    pub fn new_storage(key: Key) -> Self {
        Self {
            key,
            read_type: ReadType::Storage,
        }
    }
    pub fn new_merged(key: Key, value: Arc<Value>) -> Self {
        Self {
            key,
            read_type: ReadType::Merged(value),
        }
    }
    pub fn new_partial_merged(key: Key, delta: Value::Delta) -> Self {
        Self {
            key,
            read_type: ReadType::PartialMerged(delta),
        }
    }
    pub fn new_unmerged(key: Key, deltas: Vec<Value::Delta>) -> Self {
        Self {
            key,
            read_type: ReadType::Unmerged(deltas),
        }
    }
    pub fn key(&self) -> &Key {
        &self.key
    }
    pub fn validate_version(&self, version: Version) -> bool {
        self.read_type == ReadType::Version(version)
    }
    pub fn validate_storage(&self) -> bool {
        self.read_type == ReadType::Storage
    }
    pub fn validate_merged(&self, value: Value) -> bool {
        self.read_type == ReadType::Merged(Arc::new(value))
    }
    pub fn validate_partial_merged(&self, delta: Value::Delta) -> bool {
        self.read_type == ReadType::PartialMerged(delta)
    }
    pub fn validate_unmerged(&self, deltas: Vec<Value::Delta>) -> bool {
        self.read_type == ReadType::Unmerged(deltas)
    }
}
/// read result return by mvmap view
pub enum ReadResult<Value>
where
    Value: Mergeable,
{
    /// normal value
    Value(Arc<Value>),
    /// merged normal value
    Merged(Arc<Value>),
    /// delta
    PartialMerged(Value::Delta),
    /// deltas
    Unmerged(Vec<Value::Delta>),
    /// value not found
    NotFound,
}
pub type ReadSet<K, V> = Vec<ReadDescriptor<K, V>>;
/// mvmap view,mvmap used to read,scheduler used to add/wait dependency
pub struct MVMapView<Key, Value>
where
    Value: Mergeable,
{
    txn_idx: TxnIndex,
    mvmap: Arc<MVMap<Key, Value>>,
    scheduler: Arc<Scheduler>,
    deg: DEGHandle,
    /// Mutex used to be `Sync`
    captured_reads: Mutex<ReadSet<Key, Value>>,
}
/// public methods provided by mvmap view
impl<Key, Value> MVMapView<Key, Value>
where
    Key: Eq + Hash + Clone,
    Value: Mergeable,
{
    pub(crate) fn new(
        txn_idx: TxnIndex,
        mvmap: Arc<MVMap<Key, Value>>,
        scheduler: Arc<Scheduler>,
        deg: DEGHandle,
    ) -> Self {
        Self {
            txn_idx,
            mvmap,
            scheduler,
            deg,
            captured_reads: Mutex::new(Vec::new()),
        }
    }
    /// read value from mvmap view
    pub fn read(&self, k: &Key) -> ReadResult<Value> {
        loop {
            match self.mvmap.read(k, self.txn_idx) {
                Ok(MVMapReadOutput::Version(version, v)) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_version(k.clone(), version));
                    return ReadResult::Value(v);
                }
                Ok(MVMapReadOutput::Merged(v)) => {
                    let v = Arc::new(v);
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_merged(k.clone(), v.clone()));
                    return ReadResult::Merged(v);
                }
                Ok(MVMapReadOutput::PartialMerged(delta)) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_partial_merged(k.clone(), delta));
                    return ReadResult::PartialMerged(delta);
                }
                Ok(MVMapReadOutput::Unmerged(deltas)) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_unmerged(k.clone(), deltas.clone()));
                    return ReadResult::Unmerged(deltas);
                }
                Err(MVMapReadError::NotFound) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_storage(k.clone()));
                    return ReadResult::NotFound;
                }
                Err(MVMapReadError::Dependency(blocking_txn_idx)) => {
                    match self
                        .scheduler
                        .wait_for_dependency(self.txn_idx, blocking_txn_idx)
                    {
                        Some(condvar) => {
                            // TODO resume DEG
                            // self.deg.order();
                            #[cfg(feature = "tracing")]
                            let wait = std::time::Instant::now();
                            condvar.wait();
                            #[cfg(feature = "tracing")]
                            tracing::trace!("wait = {:?}", wait.elapsed().as_nanos());
                            // self.deg.cancel();
                        }
                        None => continue,
                    }
                }
            }
        }
    }
    pub(crate) fn take_read_set(&mut self) -> ReadSet<Key, Value> {
        let mut read_set = self.captured_reads.lock();
        std::mem::take(&mut read_set)
    }
    /// return txn idx of current mvmap view
    pub fn txn_idx(&self) -> TxnIndex {
        self.txn_idx
    }
}
