use block_stm::test_utils::generate_ledger_and_txns;
use criterion::{criterion_group, criterion_main, Criterion};

pub fn criterion_benchmark(_c: &mut Criterion) {
    static ACCOUNTS_NUM: [usize; 5] = [3, 10, 100, 1000, 10000];
    static TXNS_NUM: [usize; 2] = [1000, 10000];
    for txns_num in TXNS_NUM {
        for accounts_num in ACCOUNTS_NUM {
            let (_ledger, _txns) =
                generate_ledger_and_txns(accounts_num, 1_000_000, txns_num, 1, 1000);
            // TODO
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
