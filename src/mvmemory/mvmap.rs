use std::{collections::BTreeMap, hash::Hash, sync::Arc};

use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use thiserror::Error;

use crate::{
    core::Mergeable,
    types::{AtomicBool, Incarnation, TxnIndex, Version},
};

/// design comes from `aptos-core`
///
/// entry cell uses Arc for shared ownership and avoids unnecessary data clones.
pub enum EntryCell<Value>
where
    Value: Mergeable,
{
    /// write record
    Write(Incarnation, Arc<Value>),
    /// delta record
    Delta(Value::DeltaOp),
}
///
pub struct Entry<Value>
where
    Value: Mergeable,
{
    /// estimate flag
    flag: AtomicBool,
    /// content
    pub cell: EntryCell<Value>,
}
impl<Value> Entry<Value>
where
    Value: Mergeable,
{
    pub fn new_write(incarnation: Incarnation, v: Arc<Value>) -> Self {
        Self {
            flag: AtomicBool::new(false),
            cell: EntryCell::Write(incarnation, v),
        }
    }
    pub fn new_delta(d: Value::DeltaOp) -> Self {
        Self {
            flag: AtomicBool::new(false),
            cell: EntryCell::Delta(d),
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
pub enum MVMapOutput<Value>
where
    Value: Mergeable,
{
    Version(Version, Arc<Value>),
    PartialMerged(Value::DeltaOp),
    Unmerged(Vec<Value::DeltaOp>),
    Merged(Value),
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
pub struct MVMap<Key, Value>
where
    Value: Mergeable,
{
    /// `BTreeMap` used to `read` faster,`CachePadded` used to mitigate false sharing.
    pub inner: DashMap<Key, BTreeMap<TxnIndex, CachePadded<Entry<Value>>>>,
}
/// public methods used by mvmemory
impl<Key, Value> MVMap<Key, Value>
where
    Key: Eq + Hash,
    Value: Mergeable,
{
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }
    pub fn entry_map_for_key(
        &self,
        k: &Key,
    ) -> Option<BTreeMap<TxnIndex, CachePadded<Entry<Value>>>> {
        self.inner.remove(k).map(|(_, tree)| tree)
    }
    pub fn read(&self, k: &Key, txn_idx: TxnIndex) -> Result<MVMapOutput<Value>, MVMapError> {
        match self.inner.get(k) {
            Some(map) => {
                let mut iter = map.range(0..txn_idx);
                if Value::partial_mergeable() {
                    let mut delta: Option<Value::DeltaOp> = None;
                    while let Some((idx, entry)) = iter.next_back() {
                        if entry.is_estimate() {
                            return Err(MVMapError::ReadError(*idx));
                        }
                        match (&entry.cell, delta.as_mut()) {
                            (EntryCell::Write(i, v), None) => {
                                return Ok(MVMapOutput::Version((*idx, *i), v.clone()));
                            }
                            (EntryCell::Write(_i, v), Some(delta)) => {
                                return Ok(MVMapOutput::Merged(v.apply_delta(delta)));
                            }
                            (EntryCell::Delta(d), None) => {
                                delta = Some(*d);
                            }
                            (EntryCell::Delta(d), Some(delta)) => {
                                *delta = Value::partial_merge(d, delta);
                            }
                        }
                    }
                    match delta {
                        Some(delta) => Ok(MVMapOutput::PartialMerged(delta)),
                        None => Err(MVMapError::NotFound),
                    }
                } else {
                    let mut deltas: Option<Vec<Value::DeltaOp>> = None;
                    while let Some((idx, entry)) = iter.next_back() {
                        if entry.is_estimate() {
                            return Err(MVMapError::ReadError(*idx));
                        }
                        match (&entry.cell, deltas.as_mut()) {
                            (EntryCell::Write(i, v), None) => {
                                return Ok(MVMapOutput::Version((*idx, *i), v.clone()));
                            }
                            (EntryCell::Write(_i, v), Some(deltas)) => {
                                let mut result: Option<Value> = None;
                                deltas.iter().rev().for_each(|delta| match &mut result {
                                    Some(result) => *result = Value::apply_delta(result, delta),
                                    None => result = Some(Value::apply_delta(v, delta)),
                                });
                                return Ok(MVMapOutput::Merged(result.unwrap()));
                            }
                            (EntryCell::Delta(d), None) => deltas = Some(vec![*d]),
                            (EntryCell::Delta(d), Some(deltas)) => {
                                deltas.push(*d);
                            }
                        }
                    }
                    match deltas {
                        Some(mut deltas) => Ok({
                            deltas.reverse();
                            MVMapOutput::Unmerged(deltas)
                        }),
                        None => Err(MVMapError::NotFound),
                    }
                }
            }
            None => Err(MVMapError::NotFound),
        }
    }
    pub fn write(&self, k: Key, v: Value, version: Version) {
        let (txn_idx, incarnation) = version;
        let mut map = self.inner.entry(k).or_default();
        map.insert(
            txn_idx,
            CachePadded::new(Entry::new_write(incarnation, Arc::new(v))),
        );
    }
    pub fn delta(&self, k: Key, d: Value::DeltaOp, version: Version) {
        let (txn_idx, _) = version;
        let mut map = self.inner.entry(k).or_default();
        map.insert(txn_idx, CachePadded::new(Entry::new_delta(d)));
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
