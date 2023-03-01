use std::{collections::HashSet, sync::Arc};

use arc_swap::ArcSwapOption;
use crossbeam::utils::CachePadded;

use crate::{
    core::{Mergeable, Transaction, TransactionOutput},
    mvmemory::ReadSet,
    types::TxnIndex,
};

/// latest transaction input/output
///
/// ArcSwapOption used to record/load atomically and efficiently (RCU)
pub(crate) struct LastTxnsIO<Key, Value, Output>
where
    Value: Mergeable,
    Output: TransactionOutput,
{
    /// input(read set)
    input: Vec<CachePadded<ArcSwapOption<ReadSet<Key, Value>>>>,
    /// output(write set + delta set)
    output: Vec<CachePadded<ArcSwapOption<Output>>>,
}
impl<Key, Value, Output> LastTxnsIO<Key, Value, Output>
where
    Value: Mergeable,
    Output: TransactionOutput,
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
    pub fn last_modified_keys(
        &self,
        txn_idx: TxnIndex,
    ) -> HashSet<<<Output as TransactionOutput>::T as Transaction>::Key> {
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
    pub fn last_read_set(&self, txn_idx: TxnIndex) -> Option<Arc<ReadSet<Key, Value>>> {
        self.input[txn_idx].load_full()
    }
    pub fn record(&self, txn_idx: TxnIndex, input: ReadSet<Key, Value>, output: Output) {
        self.input[txn_idx].store(Some(Arc::new(input)));
        self.output[txn_idx].store(Some(Arc::new(output)));
    }
    pub fn take_output(&self, txn_idx: TxnIndex) -> Output {
        let output = self.output[txn_idx].swap(None).expect("swap error");
        if let Ok(output) = Arc::try_unwrap(output) {
            output
        } else {
            unreachable!()
        }
    }
}
