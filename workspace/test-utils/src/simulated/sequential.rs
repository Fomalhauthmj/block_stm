use super::types::{Ledger, TransferTxn, TransferTxnOutput};
use anyhow::anyhow;

struct SequentialExecutor(Ledger);
impl SequentialExecutor {
    fn new(ledger: Ledger) -> Self {
        Self(ledger)
    }
    fn apply_output(&mut self, output: &TransferTxnOutput) {
        for (k, v) in &output.0 {
            self.0.insert(*k, *v);
        }
    }
    fn execute_transaction(&self, txn: &TransferTxn) -> anyhow::Result<TransferTxnOutput> {
        let read = |k| match self.0.get(k) {
            Some(v) => Ok(*v),
            _ => Err(anyhow!("balance not found")),
        };
        let from_balance = read(&txn.from)?;
        if from_balance >= txn.money {
            let to_balance = read(&txn.to)?;
            let output = TransferTxnOutput(vec![
                (txn.from, from_balance - txn.money),
                (txn.to, to_balance + txn.money),
            ]);
            Ok(output)
        } else {
            Err(anyhow!("balance not enough"))
        }
    }
}
/// sequential execute simulated txns
pub fn simulated_sequential_execute(
    txns: Vec<TransferTxn>,
    ledger: &Ledger,
) -> Vec<TransferTxnOutput> {
    let mut executor = SequentialExecutor::new(ledger.clone());
    txns.iter()
        .map(|txn| {
            let output = executor.execute_transaction(txn).expect("execute error");
            executor.apply_output(&output);
            output
        })
        .collect()
}
