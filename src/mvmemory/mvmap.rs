use std::{collections::BTreeMap, hash::Hash, sync::Arc};

use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use thiserror::Error;

use crate::types::{AtomicBool, Incarnation, TxnIndex, Version};

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
    Write(Incarnation, Arc<Value>),
}
impl<Value> Entry<Value> {
    pub fn new(incarnation: Incarnation, v: Arc<Value>) -> Self {
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
/// read output from mvmap
pub enum MVMapOutput<Value> {
    Version(Version, Arc<Value>),
}
/// error from mvmap
#[derive(Debug, Error)]
pub enum MVMapError {
    #[error("NotFound")]
    NotFound,
    #[error("ReadError:{0}")]
    ReadError(TxnIndex),
}
/// multi-version hashmap used by block-stm
pub struct MVMap<Key, Value> {
    /// `BTreeMap` used to `read` faster,`CachePadded` used to mitigate false sharing.
    pub inner: DashMap<Key, BTreeMap<TxnIndex, CachePadded<Entry<Value>>>>,
}
/// public methods used by mvmemory
impl<Key, Value> MVMap<Key, Value>
where
    Key: Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }
    pub fn read(&self, k: &Key, txn_idx: TxnIndex) -> Result<MVMapOutput<Value>, MVMapError> {
        match self.inner.get(k) {
            Some(map) => {
                let mut iter = map.range(0..txn_idx);
                match iter.next_back() {
                    Some((idx, entry)) => {
                        if entry.is_estimate() {
                            Err(MVMapError::ReadError(*idx))
                        } else {
                            match &entry.cell {
                                EntryCell::Write(incarnation, v) => {
                                    Ok(MVMapOutput::Version((*idx, *incarnation), v.clone()))
                                }
                            }
                        }
                    }
                    None => Err(MVMapError::NotFound),
                }
            }
            None => Err(MVMapError::NotFound),
        }
    }
    pub fn write(&self, k: Key, v: Value, version: Version) {
        let (txn_idx, incarnation) = version;
        let mut map = self.inner.entry(k).or_insert(BTreeMap::new());
        map.insert(
            txn_idx,
            CachePadded::new(Entry::new(incarnation, Arc::new(v))),
        );
    }
    pub fn mark_estimate(&self, k: &Key, txn_idx: TxnIndex) {
        let map = self.inner.get(k).expect("key must exist");
        map.get(&txn_idx)
            .expect("entry by txn idx must exist")
            .mark_estimate();
    }
    pub fn delete(&self, k: &Key, txn_idx: TxnIndex) {
        let mut map = self.inner.get_mut(k).expect("key must exist");
        map.remove(&txn_idx);
    }
}
