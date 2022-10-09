/// multiple version memory
use std::{collections::HashSet, fmt::Debug, hash::Hash};

use dashmap::DashMap;
use thiserror::Error;
use tracing::instrument;

use crate::{
    rayon_info,
    types::{IncarnationNumber, TransactionIndex, Version},
};
#[derive(Debug)]
pub enum Entry<Value> {
    Estimate,
    Write(IncarnationNumber, Value),
}
pub struct MVMemory<Key, Value>
where
    Key: Eq + Hash + Copy,
    Value: Clone,
{
    // TODO: more appropriate concurrent data structure
    data: DashMap<(Key, TransactionIndex), Entry<Value>>,
    last_written_locations: DashMap<TransactionIndex, Vec<Key>>,
    last_read_set: DashMap<TransactionIndex, Vec<(Key, Version)>>,
    block_size: usize,
}
impl<Key, Value> MVMemory<Key, Value>
where
    Key: Eq + Hash + Copy + Debug,
    Value: Clone + Debug,
{
    #[instrument(skip_all)]
    fn apply_write_set(
        &self,
        txn_idx: TransactionIndex,
        incarnation_number: IncarnationNumber,
        write_set: Vec<(Key, Value)>,
    ) {
        for (key, value) in write_set {
            let prev = self
                .data
                .insert((key, txn_idx), Entry::Write(incarnation_number, value));
            if prev.is_some() {
                if let Entry::Estimate = prev.unwrap() {
                    rayon_info!("overwritten estimate in txn {}", txn_idx);
                }
            }
        }
    }
    #[instrument(skip_all)]
    fn rcu_update_written_locations(
        &self,
        txn_idx: TransactionIndex,
        new_locations: Vec<Key>,
    ) -> bool {
        let wrote_new_location;
        {
            let prev_locations = &*self
                .last_written_locations
                .get(&txn_idx)
                .expect("prev_locations should exist.");
            for unwritten_location in prev_locations {
                if !new_locations.contains(unwritten_location) {
                    // TODO: key should be copy?
                    self.data.remove(&(*unwritten_location, txn_idx));
                }
            }
            wrote_new_location = new_locations
                .iter()
                .any(|location| !prev_locations.contains(location));
        }
        self.last_written_locations.insert(txn_idx, new_locations);
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
    Key: Eq + Hash + Copy + Debug,
    Value: Clone + Debug,
{
    pub fn new(block_size: usize) -> Self {
        let last_written_locations = DashMap::new();
        let last_read_set = DashMap::new();

        for i in 0..block_size {
            last_read_set.insert(i, vec![]);
            last_written_locations.insert(i, vec![]);
        }
        Self {
            data: DashMap::new(),
            last_written_locations,
            last_read_set,
            block_size,
        }
    }
    #[instrument(skip(self))]
    pub fn record(
        &self,
        version: Version,
        read_set: Vec<(Key, Version)>,
        write_set: Vec<(Key, Value)>,
    ) -> bool {
        let (txn_idx, incarnation_number) = version.expect("version should be some.");
        let new_locations = write_set.iter().map(|(location, _)| *location).collect();
        self.apply_write_set(txn_idx, incarnation_number, write_set);
        let wrote_new_location = self.rcu_update_written_locations(txn_idx, new_locations);
        self.last_read_set.insert(txn_idx, read_set);
        wrote_new_location
    }
    pub fn convert_writes_to_estimates(&self, txn_idx: TransactionIndex) {
        let prev_locations = &*self
            .last_written_locations
            .get(&txn_idx)
            .expect("prev_locations should exist.");
        for location in prev_locations {
            self.data.insert((*location, txn_idx), Entry::Estimate);
        }
    }
    #[instrument(skip_all)]
    pub fn read(
        &self,
        location: &Key,
        txn_idx: TransactionIndex,
    ) -> Result<(Version, Value), MVMemoryError> {
        for idx in (0..txn_idx).rev() {
            if let Some(entry) = self.data.get(&(*location, idx)) {
                return match &*entry {
                    Entry::Estimate => Err(MVMemoryError::ReadError(idx)),
                    // TODO: value should be clone? use Arc instead of clone value;
                    Entry::Write(incarnation_number, value) => {
                        Ok((Some((idx, *incarnation_number)), value.clone()))
                    }
                };
            }
        }
        Err(MVMemoryError::NotFound)
    }
    pub fn snapshot(&self) -> Vec<(Key, Value)> {
        // TODO: need more efficient implement, maybe need change data index
        let mut ret = Vec::new();
        let mut has_read = HashSet::new();
        self.data.iter().for_each(|item| {
            let (location, _) = item.key();
            if !has_read.contains(location) {
                rayon_info!(
                    "read location:{:?}->{:?}",
                    location,
                    self.read(location, self.block_size)
                );
                if let Ok((_, value)) = self.read(location, self.block_size) {
                    ret.push((*location, value));
                    has_read.insert(*location);
                }
            }
        });
        rayon_info!("{:?}", self.data);
        ret
    }
    #[instrument(skip_all)]
    pub fn validate_read_set(&self, txn_idx: TransactionIndex) -> bool {
        let prior_reads = &*self
            .last_read_set
            .get(&txn_idx)
            .expect("prior_reads should exist.");
        for (location, version) in prior_reads {
            match self.read(location, txn_idx) {
                Err(MVMemoryError::ReadError(_)) => return false,
                Err(MVMemoryError::NotFound) if version.is_some() => return false,
                Ok((cur_version, _)) if cur_version != *version => return false,
                _ => {}
            }
        }
        true
    }
}
