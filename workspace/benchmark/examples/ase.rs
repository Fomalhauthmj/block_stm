use clap::Parser;
use test_utils::{generate_txns_and_state, set_global_subscriber, AVAILABLE_SEQUENTIAL_EXECUTION};
use tracing::info;
#[derive(Parser)]
struct Cli {
    accs: usize,
    txns: usize,
}
fn main() {
    let cli = Cli::parse();
    let _guard = set_global_subscriber(
        "./logs",
        &format!("ase-accs-{}-txns-{}", cli.accs, cli.txns),
        tracing::Level::TRACE,
    );
    info!("accs = {}, txns = {}", cli.accs, cli.txns);
    for (name, func) in AVAILABLE_SEQUENTIAL_EXECUTION.iter() {
        if name.starts_with("aptos") {
            let (txns, state) = generate_txns_and_state(cli.accs, cli.txns);
            func(txns, &state);
        }
    }
}
