use std::{collections::BTreeMap, fmt::Debug, hash::Hash, sync::Arc};

use crate::{
    sync::AtomicBool,
    traits::ValueBytes,
    types::{IncarnationNumber, TransactionIndex, Version},
};
use arc_swap::ArcSwapOption;
use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use rayon::prelude::*;
use thiserror::Error;
use tracing::instrument;
/// design comes from `aptos-core`
pub struct Entry<Value> {
    /// estimate flag
    flag: AtomicBool,
    /// content
    pub cell: EntryCell<Value>,
}
/// entry cell uses Arc for shared ownership and avoids unnecessary data clones.
pub enum EntryCell<Value> {
    /// write record
    Write(IncarnationNumber, Arc<Value>),
}
impl<Value> Entry<Value> {
    pub fn new(incarnation: IncarnationNumber, v: Arc<Value>) -> Self {
        Self {
            flag: AtomicBool::new(false),
            cell: EntryCell::Write(incarnation, v),
        }
    }
    pub fn is_estimate(&self) -> bool {
        self.flag.load()
    }
    pub fn mark_estimate(&self) {
        self.flag.store(true);
    }
}
pub enum MVMemoryReadOutput<Value> {
    Version(Version, Arc<Value>),
}
type ResdSet<Key> = Vec<(Key, Option<Version>)>;
struct MultiVersionMap<Key, Value> {
    inner: DashMap<Key, BTreeMap<TransactionIndex, CachePadded<Entry<Value>>>>,
}
pub struct MVMemory<Key, Value> {
    /// BTreeMap used to `read` faster,CachePadded used to mitigate false sharing.
    data: MultiVersionMap<Key, Value>,
    /// ArcSwapOption used to read/write atomically and efficiently (RCU)
    last_written_locations: Vec<CachePadded<ArcSwapOption<Vec<Key>>>>,
    /// ArcSwapOption used to read/write atomically and efficiently (RCU)
    last_read_set: Vec<CachePadded<ArcSwapOption<ResdSet<Key>>>>,
    block_size: usize,
}
impl<Key, Value> MVMemory<Key, Value>
where
    Key: Eq + Hash + Debug,
    Value: Debug,
{
    #[instrument(skip(self))]
    fn apply_write_set(
        &self,
        txn_idx: TransactionIndex,
        incarnation: IncarnationNumber,
        write_set: Vec<(Key, Arc<Value>)>,
    ) {
        for (key, value) in write_set {
            self.data.write(key, (txn_idx, incarnation), value);
        }
    }
    #[instrument(skip(self))]
    fn rcu_update_written_locations(
        &self,
        txn_idx: TransactionIndex,
        new_locations: Vec<Key>,
    ) -> bool {
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
/// error from mvmemory
#[derive(Debug, Error)]
pub enum MVMemoryError {
    #[error("NotFound")]
    NotFound,
    #[error("ReadError:{0}")]
    ReadError(TransactionIndex),
}
impl<Key, Value> MVMemory<Key, Value>
where
    Key: Eq + Hash + Clone + Send + Sync + Debug,
    Value: Send + Sync + ValueBytes + Debug,
{
    pub fn new(block_size: usize) -> Self {
        Self {
            data: MultiVersionMap {
                inner: DashMap::new(),
            },
            last_written_locations: (0..block_size)
                .map(|_| CachePadded::new(ArcSwapOption::empty()))
                .collect(),
            last_read_set: (0..block_size)
                .map(|_| CachePadded::new(ArcSwapOption::empty()))
                .collect(),
            block_size,
        }
    }
    #[instrument(skip(self))]
    pub fn read(
        &self,
        k: &Key,
        txn_idx: usize,
    ) -> Result<MVMemoryReadOutput<Value>, MVMemoryError> {
        self.data.read(k, txn_idx)
    }
    #[instrument(skip(self))]
    pub fn record(
        &self,
        version: Version,
        read_set: Vec<(Key, Option<Version>)>,
        write_set: Vec<(Key, Arc<Value>)>,
    ) -> bool {
        let (txn_idx, incarnation_number) = version;
        let new_locations = write_set.iter().map(|(key, _)| key).cloned().collect();
        self.apply_write_set(txn_idx, incarnation_number, write_set);
        let wrote_new_location = self.rcu_update_written_locations(txn_idx, new_locations);
        self.last_read_set[txn_idx].store(Some(Arc::new(read_set)));
        wrote_new_location
    }
    pub fn convert_writes_to_estimates(&self, txn_idx: TransactionIndex) {
        let prev_locations = self.last_written_locations[txn_idx].load_full();
        if let Some(prev_locations) = prev_locations {
            for location in &*prev_locations {
                self.data.mark_estimate(location, txn_idx);
            }
        }
    }
    #[instrument(skip(self))]
    pub fn snapshot(self) -> Vec<(Key, Value)> {
        let map = self.data.inner;
        map.into_par_iter()
            .filter(|(_, v)| !v.is_empty())
            .map(|(location, versions)| {
                if let Some((_, enrty)) = versions.range(0..self.block_size).next_back() {
                    match &enrty.cell {
                        EntryCell::Write(_, v) => {
                            (location, Value::from_raw_bytes(v.to_raw_bytes()))
                        }
                    }
                } else {
                    unreachable!()
                }
            })
            .collect()
    }
    #[instrument(skip(self))]
    pub fn validate_read_set(&self, txn_idx: TransactionIndex) -> bool {
        let prior_reads = self.last_read_set[txn_idx].load_full();
        if let Some(prior_reads) = prior_reads {
            for (location, version) in &*prior_reads {
                match self.read(location, txn_idx) {
                    Ok(MVMemoryReadOutput::Version(cur_version, _))
                        if Some(cur_version) != *version =>
                    {
                        return false
                    }
                    Err(MVMemoryError::ReadError(_)) => return false,
                    Err(MVMemoryError::NotFound) if version.is_some() => return false,
                    _ => {}
                }
            }
        }
        true
    }
}

impl<Key, Value> MultiVersionMap<Key, Value>
where
    Key: Eq + Hash + Debug,
    Value: Debug,
{
    #[instrument(skip(self))]
    pub fn read(
        &self,
        k: &Key,
        txn_idx: TransactionIndex,
    ) -> Result<MVMemoryReadOutput<Value>, MVMemoryError> {
        match self.inner.get(k) {
            Some(map) => {
                let mut iter = map.range(0..txn_idx);
                match iter.next_back() {
                    Some((idx, entry)) => {
                        if entry.is_estimate() {
                            Err(MVMemoryError::ReadError(*idx))
                        } else {
                            match &entry.cell {
                                EntryCell::Write(incarnation, v) => {
                                    Ok(MVMemoryReadOutput::Version((*idx, *incarnation), v.clone()))
                                }
                            }
                        }
                    }
                    None => Err(MVMemoryError::NotFound),
                }
            }
            None => Err(MVMemoryError::NotFound),
        }
    }
    #[allow(unused_variables)]
    #[instrument(skip(self))]
    pub fn write(&self, k: Key, version: Version, v: Arc<Value>) {
        let (txn_idx, incarnation) = version;
        let mut map = self.inner.entry(k).or_insert(BTreeMap::new());
        let prev = map.insert(txn_idx, CachePadded::new(Entry::new(incarnation, v)));
        #[cfg(feature = "correctness")]
        // Assert that the previous entry for txn_idx, if present, had lower incarnation.
        assert!(prev.map_or(true, |enrty| -> bool {
            match enrty.cell {
                EntryCell::Write(i, _) => i < incarnation,
            }
        }));
    }
    #[instrument(skip(self))]
    pub fn mark_estimate(&self, k: &Key, txn_idx: TransactionIndex) {
        let map = self.inner.get(k).expect("key must exist");
        map.get(&txn_idx)
            .expect("entry by txn idx must exist")
            .mark_estimate();
    }
    #[instrument(skip(self))]
    pub fn delete(&self, k: &Key, txn_idx: TransactionIndex) {
        let mut map = self.inner.get_mut(k).expect("key must exist");
        map.remove(&txn_idx);
    }
}
