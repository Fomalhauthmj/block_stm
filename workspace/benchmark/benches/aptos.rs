use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BatchSize, BenchmarkGroup, BenchmarkId,
    Criterion, Throughput,
};
use test_utils::{
    generate_txns_and_state, set_global_subscriber, AVAILABLE_PARALLEL_EXECUTION,
    AVAILABLE_SEQUENTIAL_EXECUTION,
};
use tracing::info;

static TXNS_NUM: usize = 10_000;
static ACCOUNTS_NUM: usize = 1_000;

fn bench(c: &mut Criterion) {
    let _guard = set_global_subscriber("./logs", "aptos_benchmark", tracing::Level::TRACE);
    info!("TXNS_NUM = {TXNS_NUM},ACCOUNTS_NUM= {ACCOUNTS_NUM}");
    let sequential_execute_parameterized_by_accs = |group: &mut BenchmarkGroup<WallTime>, accs| {
        let (txns, state) = generate_txns_and_state(accs, TXNS_NUM);
        for (name, func) in AVAILABLE_SEQUENTIAL_EXECUTION.iter() {
            info!("func = {name}, accs = {accs}, txns = {TXNS_NUM}, cpus = 1");
            group.bench_with_input(BenchmarkId::new(name, accs), &accs, |b, _| {
                b.iter_batched(
                    || txns.clone(),
                    |txns| func(txns, &state),
                    BatchSize::SmallInput,
                );
            });
        }
    };

    let parallel_execute_parameterized_by_accs = |group: &mut BenchmarkGroup<WallTime>, accs| {
        let (txns, state) = generate_txns_and_state(accs, TXNS_NUM);
        for (name, func) in AVAILABLE_PARALLEL_EXECUTION.iter() {
            info!(
                "func = {name}, accs = {accs}, txns = {TXNS_NUM}, cpus = {}",
                num_cpus::get()
            );
            group.bench_with_input(BenchmarkId::new(name, accs), &accs, |b, _| {
                b.iter_batched(
                    || txns.clone(),
                    |txns| func(txns, &state, num_cpus::get()),
                    BatchSize::SmallInput,
                );
            });
        }
    };

    let parallel_execute_parameterized_by_cpus = |group: &mut BenchmarkGroup<WallTime>, cpus| {
        let (txns, state) = generate_txns_and_state(ACCOUNTS_NUM, TXNS_NUM);
        for (name, func) in AVAILABLE_PARALLEL_EXECUTION.iter() {
            info!("func = {name}, accs = {ACCOUNTS_NUM}, txns = {TXNS_NUM}, cpus = {cpus}");
            group.bench_with_input(BenchmarkId::new(name, cpus), &cpus, |b, _| {
                b.iter_batched(
                    || txns.clone(),
                    |txns| func(txns, &state, cpus),
                    BatchSize::SmallInput,
                );
            });
        }
    };

    let mut group = c.benchmark_group("conflicting_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    // accounts num bigger,conflicting level lower
    for accs in [10_000] {
        sequential_execute_parameterized_by_accs(&mut group, accs);
        parallel_execute_parameterized_by_accs(&mut group, accs);
    }
    group.finish();

    let mut group = c.benchmark_group("concurrency_level");
    group.throughput(Throughput::Elements(TXNS_NUM as u64));
    for cpus in 2..=num_cpus::get() {
        parallel_execute_parameterized_by_cpus(&mut group, cpus);
    }
    group.finish();
}
criterion_group!(
    name = benches;
    config=Criterion::default().sample_size(10);
    targets=bench);
criterion_main!(benches);
