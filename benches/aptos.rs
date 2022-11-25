use block_stm::test_utils::{aptos::*, try_init_global_subscriber, BenchmarkInfos};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};
use std::time::Duration;
const TXNS_NUM: usize = 10_000;

fn log_benchmark_info(
    name: &str,
    accs: usize,
    txns: usize,
    cpus: usize,
    infos: &mut BenchmarkInfos,
) {
    #[cfg(feature = "bench_info")]
    {
        block_stm::rayon_info!("{} (accs={},txns={},cpus={})", name, accs, txns, cpus);
        block_stm::rayon_info!("{}", infos.mean());
        infos.clear_infos();
    }
}
fn bench(c: &mut Criterion) {
    #[cfg(feature = "bench_info")]
    let _guard = try_init_global_subscriber("./logs", "aptos_bench", tracing::Level::TRACE);
    let mut group = c.benchmark_group("conflicting_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    let mut infos = BenchmarkInfos::default();
    // accounts num bigger,conflicting level lower
    for accounts_num in [3, 10, 100, 1000] {
        let (txns, state) = generate_txns_and_state(accounts_num, TXNS_NUM);
        group.bench_with_input(
            BenchmarkId::new("aptos sequential execute", accounts_num),
            &accounts_num,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (_, info) = sequential_execute(&txns, &state);
                        total += info.total_time;
                        infos.add_info(info);
                    }
                    total
                })
            },
        );
        log_benchmark_info(
            "aptos sequential execute",
            accounts_num,
            TXNS_NUM,
            1,
            &mut infos,
        );
        group.bench_with_input(
            BenchmarkId::new("aptos parallel execute", accounts_num),
            &accounts_num,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (_, info) = aptos_parallel_execute(&txns, &state, num_cpus::get());
                        total += info.total_time;
                        infos.add_info(info);
                    }
                    total
                })
            },
        );
        log_benchmark_info(
            "aptos parallel execute",
            accounts_num,
            TXNS_NUM,
            num_cpus::get(),
            &mut infos,
        );
        group.bench_with_input(
            BenchmarkId::new("my parallel execute", accounts_num),
            &accounts_num,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (_, info) = my_parallel_execute(&txns, &state, num_cpus::get());
                        total += info.total_time;
                        infos.add_info(info);
                    }
                    total
                })
            },
        );
        log_benchmark_info(
            "my parallel execute",
            accounts_num,
            TXNS_NUM,
            num_cpus::get(),
            &mut infos,
        );
    }
    group.finish();

    let mut group = c.benchmark_group("concurrency_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    static ACCOUNTS_NUM: usize = 1_000;
    let mut infos = BenchmarkInfos::default();
    for concurrency_level in 2..=num_cpus::get() {
        let (txns, state) = generate_txns_and_state(ACCOUNTS_NUM, TXNS_NUM);
        group.bench_with_input(
            BenchmarkId::new("aptos parallel execute", concurrency_level),
            &concurrency_level,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (_, info) = aptos_parallel_execute(&txns, &state, concurrency_level);
                        total += info.total_time;
                        infos.add_info(info);
                    }
                    total
                })
            },
        );
        log_benchmark_info(
            "aptos parallel execute",
            ACCOUNTS_NUM,
            TXNS_NUM,
            concurrency_level,
            &mut infos,
        );
        group.bench_with_input(
            BenchmarkId::new("my parallel execute", concurrency_level),
            &concurrency_level,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let (_, info) = my_parallel_execute(&txns, &state, concurrency_level);
                        total += info.total_time;
                        infos.add_info(info);
                    }
                    total
                })
            },
        );
        log_benchmark_info(
            "my parallel execute",
            ACCOUNTS_NUM,
            TXNS_NUM,
            concurrency_level,
            &mut infos,
        );
    }
    group.finish();
}
criterion_group!(
    name = benches;
    config=Criterion::default().with_profiler(PProfProfiler::new(100,Output::Flamegraph(None))).sample_size(10);
    targets=bench);
criterion_main!(benches);
