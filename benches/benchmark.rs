use std::time::Instant;

use block_stm::test_utils::{
    generate_ledger_and_txns, parallel_execute, sequential_execute, Ledger,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const TXNS_NUM: usize = 1_000;

fn conflicting_level(c: &mut Criterion) {
    let mut group = c.benchmark_group("conflicting_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    group.sample_size(10);
    // accounts num bigger,conflicting level lower
    for accounts_num in [3, 10, 100, 1000] {
        let (txns, ledger) = generate_ledger_and_txns(accounts_num, 1_000_000, TXNS_NUM, 1, 1000);
        group.bench_with_input(
            BenchmarkId::new("sequential execute", accounts_num),
            &accounts_num,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut ledgers: Vec<Ledger> = (0..iters).map(|_| ledger.clone()).collect();
                    let start = Instant::now();
                    for i in 0..iters {
                        sequential_execute(&txns, &mut ledgers[i as usize]);
                    }
                    start.elapsed()
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("parallel execute", accounts_num),
            &accounts_num,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut ledgers: Vec<Ledger> = (0..iters).map(|_| ledger.clone()).collect();
                    let start = Instant::now();
                    for i in 0..iters {
                        parallel_execute(&txns, &mut ledgers[i as usize], num_cpus::get());
                    }
                    start.elapsed()
                })
            },
        );
    }
    group.finish();
}

fn concurrency_level(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrency_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    group.sample_size(10);
    static ACCOUNTS_NUM: usize = 1000;
    for concurrency_level in 1..=num_cpus::get() {
        let (txns, ledger) = generate_ledger_and_txns(ACCOUNTS_NUM, 1_000_000, TXNS_NUM, 1, 1000);
        group.bench_with_input(
            BenchmarkId::new("parallel execute", concurrency_level),
            &concurrency_level,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut ledgers: Vec<Ledger> = (0..iters).map(|_| ledger.clone()).collect();
                    let start = Instant::now();
                    for i in 0..iters {
                        parallel_execute(&txns, &mut ledgers[i as usize], concurrency_level);
                    }
                    start.elapsed()
                })
            },
        );
    }
    group.finish();
}
criterion_group!(benches, conflicting_level, concurrency_level);
criterion_main!(benches);
