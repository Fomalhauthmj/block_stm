use std::time::{Duration, Instant};

#[cfg(feature = "aptos_test_utils")]
use block_stm::test_utils::{
    aptos_parallel_execute, aptos_sequential_execute, generate_aptos_txns_and_state,
};
#[cfg(feature = "simulated_test_utils")]
use block_stm::test_utils::{
    generate_txns_and_ledger, parallel_execute, sequential_execute, Ledger,
};
use block_stm::{rayon_info, test_utils::aptos_official_parallel_execute};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
const TXNS_NUM: usize = 10_000;
fn install_logger() {
    let file_appender = tracing_appender::rolling::hourly("./logs", "benchmark.log");
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(file_appender)
        .with_max_level(tracing::Level::INFO)
        .try_init();
}
fn conflicting_level(c: &mut Criterion) {
    #[cfg(feature = "tracing")]
    let _ = install_logger();
    let mut group = c.benchmark_group("conflicting_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    // accounts num bigger,conflicting level lower
    for accounts_num in [3, 10, 100, 1000] {
        #[cfg(feature = "aptos_test_utils")]
        {
            let (txns, state) = generate_aptos_txns_and_state(accounts_num, TXNS_NUM);
            group.bench_with_input(
                BenchmarkId::new("aptos sequential execute", accounts_num),
                &accounts_num,
                |b, accounts_nums| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let info = aptos_sequential_execute(txns.clone(), &state);
                            rayon_info!(
                                "aptos sequential execute(accs={}) {:?}",
                                accounts_nums,
                                info
                            );
                            total += info.total_time;
                        }
                        total
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("aptos official parallel execute", accounts_num),
                &accounts_num,
                |b, accounts_num| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let info = aptos_official_parallel_execute(
                                txns.clone(),
                                &state,
                                num_cpus::get(),
                            );
                            rayon_info!(
                                "aptos official parallel execute(accs={},cpus={}) {:?}",
                                accounts_num,
                                num_cpus::get(),
                                info
                            );
                            total += info.total_time;
                        }
                        total
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("aptos parallel execute", accounts_num),
                &accounts_num,
                |b, accounts_num| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let info =
                                aptos_parallel_execute(txns.clone(), &state, num_cpus::get());
                            rayon_info!(
                                "aptos parallel execute(accs={},cpus={}) {:?}",
                                accounts_num,
                                num_cpus::get(),
                                info
                            );
                            total += info.total_time;
                        }
                        total
                    })
                },
            );
        }
        #[cfg(feature = "simulated_test_utils")]
        {
            let (txns, ledger) =
                generate_txns_and_ledger(accounts_num, 1_000_000, TXNS_NUM, 1, 1000);
            group.bench_with_input(
                BenchmarkId::new("simulated sequential execute", accounts_num),
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
                BenchmarkId::new("simulated parallel execute", accounts_num),
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
    }

    group.finish();
}

fn concurrency_level(c: &mut Criterion) {
    #[cfg(feature = "tracing")]
    let _ = install_logger();
    let mut group = c.benchmark_group("concurrency_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    static ACCOUNTS_NUM: usize = 1_000;
    for concurrency_level in 2..=num_cpus::get() {
        #[cfg(feature = "aptos_test_utils")]
        {
            let (txns, state) = generate_aptos_txns_and_state(ACCOUNTS_NUM, TXNS_NUM);

            group.bench_with_input(
                BenchmarkId::new("aptos official parallel execute", concurrency_level),
                &concurrency_level,
                |b, _| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let info = aptos_official_parallel_execute(
                                txns.clone(),
                                &state,
                                concurrency_level,
                            );
                            rayon_info!(
                                "aptos official parallel execute(accs={},cpus={}) {:?}",
                                ACCOUNTS_NUM,
                                concurrency_level,
                                info
                            );
                            total += info.total_time;
                        }
                        total
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("aptos parallel execute", concurrency_level),
                &concurrency_level,
                |b, _| {
                    b.iter_custom(|iters| {
                        let mut total = Duration::ZERO;
                        for _ in 0..iters {
                            let info =
                                aptos_parallel_execute(txns.clone(), &state, concurrency_level);
                            rayon_info!(
                                "aptos parallel execute(accs={},cpus={}) {:?}",
                                ACCOUNTS_NUM,
                                concurrency_level,
                                info
                            );
                            total += info.total_time;
                        }
                        total
                    })
                },
            );
        }
        #[cfg(feature = "simulated_test_utils")]
        {
            let (txns, ledger) =
                generate_txns_and_ledger(ACCOUNTS_NUM, 1_000_000, TXNS_NUM, 1, 1000);
            group.bench_with_input(
                BenchmarkId::new("simulated parallel execute", concurrency_level),
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
    }
    group.finish();
}
criterion_group!(
    name = benches;
    config=Criterion::default().with_profiler(PProfProfiler::new(100,Output::Flamegraph(None))).sample_size(10);
    targets=conflicting_level, concurrency_level);
criterion_main!(benches);
