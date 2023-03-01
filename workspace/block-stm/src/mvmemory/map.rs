use std::{
    collections::{BTreeMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use thiserror::Error;

use crate::{
    core::{DeltaSet, Mergeable, WriteSet},
    types::{AtomicBool, Incarnation, TxnIndex, Version},
};

use super::view::ReadSet;

/// design from `aptos-core`,entry cell uses Arc for shared ownership and avoids unnecessary data clones.
pub enum EntryCell<Value>
where
    Value: Mergeable,
{
    /// write record
    Write(Incarnation, Arc<Value>),
    /// delta record
    Delta(Value::Delta),
}
/// entry consists with estimate flag and entry cell
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
    fn new_write_record(i: Incarnation, v: Arc<Value>) -> Self {
        Self {
            flag: AtomicBool::new(false),
            cell: EntryCell::Write(i, v),
        }
    }
    fn new_delta_record(d: Value::Delta) -> Self {
        Self {
            flag: AtomicBool::new(false),
            cell: EntryCell::Delta(d),
        }
    }
    fn is_estimate(&self) -> bool {
        self.flag.load()
    }
    fn mark_estimate(&self) {
        self.flag.store(true);
    }
}
/// mvmap read output
pub enum MVMapReadOutput<Value>
where
    Value: Mergeable,
{
    Version(Version, Arc<Value>),
    Unmerged(Vec<Value::Delta>),
    PartialMerged(Value::Delta),
    Merged(Value),
}
/// mvmap read error
#[derive(Debug, Error)]
pub enum MVMapReadError {
    #[error("NotFound")]
    NotFound,
    #[error("Dependency:{0}")]
    Dependency(TxnIndex),
}
/// multi-version concurrent hashmap (mvmemory)
pub struct MVMap<Key, Value>
where
    Value: Mergeable,
{
    /// `BTreeMap` used to `read` faster(ordered)
    /// `CachePadded` used to mitigate false sharing.
    inner: DashMap<Key, BTreeMap<TxnIndex, CachePadded<Entry<Value>>>>,
}
/// public methods provided by mvmap
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

    pub fn entry_map(&self, k: &Key) -> Option<BTreeMap<TxnIndex, CachePadded<Entry<Value>>>> {
        self.inner.remove(k).map(|(_, tree)| tree)
    }

    pub fn write(&self, k: Key, v: Value, version: Version) {
        let (txn_idx, i) = version;
        let mut map = self.inner.entry(k).or_default();
        map.insert(
            txn_idx,
            CachePadded::new(Entry::new_write_record(i, Arc::new(v))),
        );
    }
    pub fn delta(&self, k: Key, d: Value::Delta, version: Version) {
        let (txn_idx, _) = version;
        let mut map = self.inner.entry(k).or_default();
        map.insert(txn_idx, CachePadded::new(Entry::new_delta_record(d)));
    }
    pub fn mark_estimate(&self, k: &Key, txn_idx: TxnIndex) {
        let map = self.inner.get(k).expect("dashmap entry must exist");
        map.get(&txn_idx)
            .expect("entry by txn idx must exist")
            .mark_estimate();
    }
    pub fn delete(&self, k: &Key, txn_idx: TxnIndex) {
        let mut map = self.inner.get_mut(k).expect("dashmap entry must exist");
        map.remove(&txn_idx);
    }

    pub fn read(
        &self,
        k: &Key,
        txn_idx: TxnIndex,
    ) -> Result<MVMapReadOutput<Value>, MVMapReadError> {
        match self.inner.get(k) {
            Some(map) => {
                let mut iter = map.range(0..txn_idx);
                if Value::partial_mergeable() {
                    let mut delta: Option<Value::Delta> = None;
                    while let Some((idx, entry)) = iter.next_back() {
                        if entry.is_estimate() {
                            return Err(MVMapReadError::Dependency(*idx));
                        }
                        match (&entry.cell, delta.as_mut()) {
                            (EntryCell::Write(i, v), None) => {
                                return Ok(MVMapReadOutput::Version((*idx, *i), v.clone()));
                            }
                            (EntryCell::Write(_i, v), Some(delta)) => {
                                return Ok(MVMapReadOutput::Merged(v.apply_delta(delta)));
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
                        Some(delta) => Ok(MVMapReadOutput::PartialMerged(delta)),
                        None => Err(MVMapReadError::NotFound),
                    }
                } else {
                    // TODO: maybe meaningless
                    let mut deltas: Option<Vec<Value::Delta>> = None;
                    while let Some((idx, entry)) = iter.next_back() {
                        if entry.is_estimate() {
                            return Err(MVMapReadError::Dependency(*idx));
                        }
                        match (&entry.cell, deltas.as_mut()) {
                            (EntryCell::Write(i, v), None) => {
                                return Ok(MVMapReadOutput::Version((*idx, *i), v.clone()));
                            }
                            (EntryCell::Write(_i, v), Some(deltas)) => {
                                let mut result: Option<Value> = None;
                                deltas.iter().rev().for_each(|delta| match result.as_mut() {
                                    Some(result) => *result = Value::apply_delta(result, delta),
                                    None => result = Some(Value::apply_delta(v, delta)),
                                });
                                return Ok(MVMapReadOutput::Merged(result.unwrap()));
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
                            MVMapReadOutput::Unmerged(deltas)
                        }),
                        None => Err(MVMapReadError::NotFound),
                    }
                }
            }
            None => Err(MVMapReadError::NotFound),
        }
    }

    pub fn apply(
        &self,
        version: Version,
        mut last_modified_keys: HashSet<Key>,
        write_set: WriteSet<Key, Value>,
        delta_set: DeltaSet<Key, <Value as Mergeable>::Delta>,
    ) -> bool {
        let (txn_idx, _) = version;
        let mut modify_new_key = false;
        for (k, v) in write_set {
            if !last_modified_keys.remove(&k) {
                modify_new_key = true;
            }
            self.write(k, v, version);
        }
        for (k, d) in delta_set {
            if !last_modified_keys.remove(&k) {
                modify_new_key = true;
            }
            self.delta(k, d, version);
        }
        for k in last_modified_keys {
            self.delete(&k, txn_idx);
        }
        modify_new_key
    }

    pub fn validate_read_set(&self, txn_idx: TxnIndex, read_set: &ReadSet<Key, Value>) -> bool {
        read_set.iter().all(|r| match self.read(r.key(), txn_idx) {
            Ok(MVMapReadOutput::Version(version, _)) => r.validate_version(version),
            Ok(MVMapReadOutput::Merged(v)) => r.validate_merged(v),
            Ok(MVMapReadOutput::PartialMerged(d)) => r.validate_partial_merged(d),
            Ok(MVMapReadOutput::Unmerged(d)) => r.validate_unmerged(d),
            Err(MVMapReadError::Dependency(_)) => false,
            Err(MVMapReadError::NotFound) => r.validate_storage(),
        })
    }
}
