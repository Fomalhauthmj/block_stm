use std::{fmt::Debug, hash::Hash, sync::Arc};

use crate::{
    core::ValueBytes,
    scheduler::Scheduler,
    types::Mutex,
    types::{Incarnation, TxnIndex, Version},
};
use arc_swap::ArcSwapOption;
use crossbeam::utils::CachePadded;
use rayon::prelude::*;

use self::mvmap::{EntryCell, MVMap, MVMapError, MVMapOutput};

/// mvmap
mod mvmap;
/// read set captured from mvmemory view
type ResdSet<Key> = Vec<ReadDescriptor<Key>>;
/// mvmemory
pub struct MVMemory<Key, Value> {
    block_size: usize,
    data: MVMap<Key, Value>,
    /// ArcSwapOption used to read/write atomically and efficiently (RCU)
    last_written_locations: Vec<CachePadded<ArcSwapOption<Vec<Key>>>>,
    /// ArcSwapOption used to read/write atomically and efficiently (RCU)
    last_read_set: Vec<CachePadded<ArcSwapOption<ResdSet<Key>>>>,
}
/// public methods used by other components
impl<Key, Value> MVMemory<Key, Value>
where
    Key: Eq + Hash + Send + Sync + Clone + Debug,
    Value: Send + Sync + ValueBytes,
{
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            data: MVMap::new(),
            last_written_locations: (0..block_size)
                .map(|_| CachePadded::new(ArcSwapOption::empty()))
                .collect(),
            last_read_set: (0..block_size)
                .map(|_| CachePadded::new(ArcSwapOption::empty()))
                .collect(),
        }
    }
    pub fn read(&self, k: &Key, txn_idx: TxnIndex) -> Result<MVMapOutput<Value>, MVMapError> {
        self.data.read(k, txn_idx)
    }
    pub fn record(
        &self,
        version: Version,
        read_set: Vec<ReadDescriptor<Key>>,
        write_set: Vec<(Key, Value)>,
    ) -> bool {
        let (txn_idx, incarnation) = version;
        let new_locations = write_set.iter().map(|(key, _)| key).cloned().collect();
        self.apply_write_set(txn_idx, incarnation, write_set);
        let wrote_new_location = self.rcu_update_written_locations(txn_idx, new_locations);
        self.last_read_set[txn_idx].store(Some(Arc::new(read_set)));
        wrote_new_location
    }
    pub fn convert_writes_to_estimates(&self, txn_idx: TxnIndex) {
        let prev_locations = self.last_written_locations[txn_idx].load_full();
        if let Some(prev_locations) = prev_locations {
            for location in &*prev_locations {
                self.data.mark_estimate(location, txn_idx);
            }
        }
    }
    pub fn validate_read_set(&self, txn_idx: TxnIndex) -> bool {
        let prior_reads = self.last_read_set[txn_idx].load_full();
        if let Some(prior_reads) = prior_reads {
            return prior_reads
                .iter()
                .all(|r| match self.read(r.key(), txn_idx) {
                    Ok(MVMapOutput::Version(version, _)) => r.validate_version(version),
                    Err(MVMapError::ReadError(_)) => false,
                    Err(MVMapError::NotFound) => r.validate_storage(),
                });
        }
        true
    }
    pub fn snapshot(self) -> Vec<(Key, Option<Value>)> {
        let map = self.data.inner;
        map.into_par_iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(location, versions)| {
                if let Some((_, enrty)) = versions.range(0..self.block_size).next_back() {
                    match &enrty.cell {
                        // TODO: make sense?
                        EntryCell::Write(_, v) => match v.serialize() {
                            Some(bytes) => (location, Some(Value::deserialize(&bytes))),
                            None => (location, None),
                        },
                    }
                } else {
                    unreachable!()
                }
            })
            .collect()
    }
}
/// private methods used by mvmemory itself
impl<Key, Value> MVMemory<Key, Value>
where
    Key: Eq + Hash,
{
    fn apply_write_set(
        &self,
        txn_idx: TxnIndex,
        incarnation: Incarnation,
        write_set: Vec<(Key, Value)>,
    ) {
        for (key, value) in write_set {
            self.data.write(key, value, (txn_idx, incarnation));
        }
    }
    fn rcu_update_written_locations(&self, txn_idx: TxnIndex, new_locations: Vec<Key>) -> bool {
        let prev_locations = self.last_written_locations[txn_idx].load_full();
        let wrote_new_location = if let Some(prev_locations) = prev_locations {
            for location in &*prev_locations {
                if !new_locations.contains(location) {
                    self.data.delete(location, txn_idx);
                }
            }
            new_locations
                .iter()
                .any(|location| !prev_locations.contains(location))
        } else {
            !new_locations.is_empty()
        };
        self.last_written_locations[txn_idx].store(Some(Arc::new(new_locations)));
        wrote_new_location
    }
}
/// read type in mvmap
#[derive(Debug, PartialEq, Eq)]
enum ReadType {
    Version(Version),
    Storage,
}
/// read descriptor in mvmap
#[derive(Debug)]
pub struct ReadDescriptor<K> {
    key: K,
    read_type: ReadType,
}
impl<K> ReadDescriptor<K>
where
    K: Debug,
{
    pub fn new_version(key: K, version: Version) -> Self {
        Self {
            key,
            read_type: ReadType::Version(version),
        }
    }
    pub fn new_storage(key: K) -> Self {
        Self {
            key,
            read_type: ReadType::Storage,
        }
    }
    pub fn key(&self) -> &K {
        &self.key
    }
    pub fn validate_version(&self, version: Version) -> bool {
        self.read_type == ReadType::Version(version)
    }
    pub fn validate_storage(&self) -> bool {
        self.read_type == ReadType::Storage
    }
}
/// read result from mvmemory view
pub enum ReadResult<V> {
    Value(Arc<V>),
    NotFound,
}
/// mvmemory view,mvmemory used to read,scheduler used to add dependency
pub struct MVMemoryView<'a, K, V> {
    txn_idx: TxnIndex,
    mvmemory: &'a MVMemory<K, V>,
    scheduler: &'a Scheduler,
    /// Mutex used to be `Sync`
    captured_reads: Mutex<Vec<ReadDescriptor<K>>>,
}
/// public methods used by executor
impl<'a, K, V> MVMemoryView<'a, K, V>
where
    K: Eq + Hash + Send + Sync + Clone + Debug,
    V: Send + Sync + ValueBytes,
{
    pub fn new(txn_idx: TxnIndex, mvmemory: &'a MVMemory<K, V>, scheduler: &'a Scheduler) -> Self {
        Self {
            txn_idx,
            mvmemory,
            scheduler,
            captured_reads: Mutex::new(Vec::new()),
        }
    }
    pub fn read(&self, k: &K) -> ReadResult<V> {
        loop {
            match self.mvmemory.read(k, self.txn_idx) {
                Ok(MVMapOutput::Version(version, v)) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_version(k.clone(), version));
                    return ReadResult::Value(v);
                }
                Err(MVMapError::NotFound) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_storage(k.clone()));
                    return ReadResult::NotFound;
                }
                Err(MVMapError::ReadError(blocking_txn_idx)) => {
                    match self
                        .scheduler
                        .wait_for_dependency(self.txn_idx, blocking_txn_idx)
                    {
                        Some(condvar) => {
                            condvar.wait();
                        }
                        None => continue,
                    }
                }
            }
        }
    }
    pub fn take_read_set(&mut self) -> Vec<ReadDescriptor<K>> {
        let mut read_set = self.captured_reads.lock();
        std::mem::take(&mut read_set)
    }
    pub fn txn_idx(&self) -> TxnIndex {
        self.txn_idx
    }
}
