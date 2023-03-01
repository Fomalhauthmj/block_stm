use clap::Parser;
use test_utils::{generate_txns_and_state, set_global_subscriber, AVAILABLE_PARALLEL_EXECUTION};
use tracing::info;
#[derive(Parser)]
struct Cli {
    accs: usize,
    txns: usize,
    cpus: usize,
}
fn main() {
    let cli = Cli::parse();
    let _guard = set_global_subscriber(
        "./logs",
        &format!("mpe-accs-{}-txns-{}-cpus-{}", cli.accs, cli.txns, cli.cpus),
        tracing::Level::TRACE,
    );
    info!(
        "accs = {}, txns = {}, cpus = {}",
        cli.accs, cli.txns, cli.cpus
    );
    for (name, func) in AVAILABLE_PARALLEL_EXECUTION.iter() {
        if name.starts_with("my") {
            let (txns, state) = generate_txns_and_state(cli.accs, cli.txns);
            func(txns, &state, cli.cpus);
        }
    }
}
