use std::{
    collections::{BTreeMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

use crate::{
    core::{Mergeable, Transaction, TransactionOutput, ValueBytes},
    scheduler::Scheduler,
    types::Mutex,
    types::{TxnIndex, Version},
};
use arc_swap::ArcSwapOption;
use crossbeam::utils::CachePadded;
use rayon::prelude::*;

use self::mvmap::{Entry, EntryCell, MVMap, MVMapError, MVMapOutput};

/// mvmap
pub mod mvmap;
/// ArcSwapOption used to record/load atomically and efficiently (RCU)
pub struct LastTxnIO<K, V, O>
where
    V: Mergeable,
    O: TransactionOutput,
{
    /// read set
    input: Vec<CachePadded<ArcSwapOption<Vec<ReadDescriptor<K, V>>>>>,
    /// write/delta set
    output: Vec<CachePadded<ArcSwapOption<O>>>,
}
impl<K, V, O> LastTxnIO<K, V, O>
where
    V: Mergeable,
    O: TransactionOutput,
{
    pub fn new(txns_num: usize) -> Self {
        Self {
            input: (0..txns_num)
                .map(|_| CachePadded::new(ArcSwapOption::empty()))
                .collect(),
            output: (0..txns_num)
                .map(|_| CachePadded::new(ArcSwapOption::empty()))
                .collect(),
        }
    }
    pub fn load_write_set(&self, txn_idx: TxnIndex) -> HashSet<<O::T as Transaction>::Key> {
        let output = self.output[txn_idx].load_full();
        if let Some(output) = output {
            output
                .get_write_set()
                .into_iter()
                .map(|(k, _)| k)
                .chain(output.get_delta_set().into_iter().map(|(k, _)| k))
                .collect()
        } else {
            HashSet::default()
        }
    }
    pub fn load_read_set(&self, txn_idx: TxnIndex) -> Option<Arc<Vec<ReadDescriptor<K, V>>>> {
        self.input[txn_idx].load_full()
    }
    pub fn record(&self, txn_idx: TxnIndex, input: Vec<ReadDescriptor<K, V>>, output: O) {
        self.input[txn_idx].store(Some(Arc::new(input)));
        self.output[txn_idx].store(Some(Arc::new(output)));
    }
    pub fn take_output(&self, txn_idx: TxnIndex) -> O {
        let output = self.output[txn_idx].swap(None).expect("swap error");
        if let Ok(output) = Arc::try_unwrap(output) {
            output
        } else {
            unreachable!()
        }
    }
}
/// mvmemory
pub struct MVMemory<Key, Value>
where
    Value: Mergeable,
{
    block_size: usize,
    data: MVMap<Key, Value>,
}
/// public methods used by other components
impl<Key, Value> MVMemory<Key, Value>
where
    Key: Eq + Hash + Send + Sync + Clone + Debug,
    Value: Send + Sync + ValueBytes + Mergeable,
{
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            data: MVMap::new(),
        }
    }
    pub fn read(&self, k: &Key, txn_idx: TxnIndex) -> Result<MVMapOutput<Value>, MVMapError> {
        self.data.read(k, txn_idx)
    }
    pub fn entry_map_for_key(
        &self,
        k: &Key,
    ) -> Option<BTreeMap<TxnIndex, CachePadded<Entry<Value>>>> {
        self.data.entry_map_for_key(k)
    }
    pub fn apply(
        &self,
        version: Version,
        mut last_write_set: HashSet<Key>,
        write_set: Vec<(Key, Value)>,
        delta_set: Vec<(Key, <Value as Mergeable>::DeltaOp)>,
    ) -> bool {
        let (txn_idx, _) = version;
        let mut wrote_new_location = false;
        for (k, v) in write_set {
            if !last_write_set.remove(&k) {
                wrote_new_location = true;
            }
            self.data.write(k, v, version);
        }
        for (k, d) in delta_set {
            if !last_write_set.remove(&k) {
                wrote_new_location = true;
            }
            self.data.delta(k, d, version);
        }
        for k in last_write_set {
            self.data.delete(&k, txn_idx);
        }
        wrote_new_location
    }
    pub fn convert_writes_to_estimates(&self, txn_idx: TxnIndex, write_set: &HashSet<Key>) {
        for key in write_set {
            self.data.mark_estimate(key, txn_idx);
        }
    }
    pub fn validate_read_set(
        &self,
        txn_idx: TxnIndex,
        read_set: &Vec<ReadDescriptor<Key, Value>>,
    ) -> bool {
        read_set.iter().all(|r| match self.read(r.key(), txn_idx) {
            Ok(MVMapOutput::Version(version, _)) => r.validate_version(version),
            Ok(MVMapOutput::Merged(v)) => r.validate_merged(v),
            Ok(MVMapOutput::PartialMerged(d)) => r.validate_partial_merged(d),
            Ok(MVMapOutput::Unmerged(d)) => r.validate_unmerged(d),
            Err(MVMapError::ReadError(_)) => false,
            Err(MVMapError::NotFound) => r.validate_storage(),
        })
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
                        _ => todo!(),
                    }
                } else {
                    unreachable!()
                }
            })
            .collect()
    }
}
/// read type in mvmap
#[derive(Debug, PartialEq, Eq)]
enum ReadType<V>
where
    V: Mergeable,
{
    Version(Version),
    Storage,
    Merged(Arc<V>),
    PartialMerged(V::DeltaOp),
    Unmerged(Vec<V::DeltaOp>),
}
/// read descriptor in mvmap
pub struct ReadDescriptor<K, V>
where
    V: Mergeable,
{
    key: K,
    read_type: ReadType<V>,
}
impl<K, V> ReadDescriptor<K, V>
where
    V: Mergeable,
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
    pub fn new_merged(key: K, value: Arc<V>) -> Self {
        Self {
            key,
            read_type: ReadType::Merged(value),
        }
    }
    pub fn new_partial_merged(key: K, delta: V::DeltaOp) -> Self {
        Self {
            key,
            read_type: ReadType::PartialMerged(delta),
        }
    }
    pub fn new_unmerged(key: K, deltas: Vec<V::DeltaOp>) -> Self {
        Self {
            key,
            read_type: ReadType::Unmerged(deltas),
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
    pub fn validate_merged(&self, value: V) -> bool {
        self.read_type == ReadType::Merged(Arc::new(value))
    }
    pub fn validate_partial_merged(&self, delta: V::DeltaOp) -> bool {
        self.read_type == ReadType::PartialMerged(delta)
    }
    pub fn validate_unmerged(&self, deltas: Vec<V::DeltaOp>) -> bool {
        self.read_type == ReadType::Unmerged(deltas)
    }
}
/// read result from mvmemory view
pub enum ReadResult<V>
where
    V: Mergeable,
{
    Value(Arc<V>),
    Merged(Arc<V>),
    PartialMerged(V::DeltaOp),
    Unmerged(Vec<V::DeltaOp>),
    NotFound,
}
/// mvmemory view,mvmemory used to read,scheduler used to add dependency
pub struct MVMemoryView<'a, K, V>
where
    V: Mergeable,
{
    txn_idx: TxnIndex,
    mvmemory: &'a MVMemory<K, V>,
    scheduler: &'a Scheduler,
    /// Mutex used to be `Sync`
    captured_reads: Mutex<Vec<ReadDescriptor<K, V>>>,
}
/// public methods used by executor
impl<'a, K, V> MVMemoryView<'a, K, V>
where
    K: Eq + Hash + Send + Sync + Clone + Debug,
    V: Send + Sync + ValueBytes + Mergeable,
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
                Ok(MVMapOutput::Unmerged(deltas)) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_unmerged(k.clone(), deltas.clone()));
                    return ReadResult::Unmerged(deltas);
                }
                Ok(MVMapOutput::Merged(v)) => {
                    let v = Arc::new(v);
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_merged(k.clone(), v.clone()));
                    return ReadResult::Merged(v);
                }
                Ok(MVMapOutput::PartialMerged(delta)) => {
                    self.captured_reads
                        .lock()
                        .push(ReadDescriptor::new_partial_merged(k.clone(), delta));
                    return ReadResult::PartialMerged(delta);
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
    pub fn take_read_set(&mut self) -> Vec<ReadDescriptor<K, V>> {
        let mut read_set = self.captured_reads.lock();
        std::mem::take(&mut read_set)
    }
    pub fn txn_idx(&self) -> TxnIndex {
        self.txn_idx
    }
}
