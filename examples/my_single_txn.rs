fn main() {
    let _guard = block_stm::test_utils::try_init_global_subscriber(
        "./logs",
        "my_single_txn",
        tracing::Level::TRACE,
    );
    let (txns, state) = block_stm::test_utils::aptos::generate_txns_and_state(1_000, 10_000);
    let _ = block_stm::test_utils::aptos::my_parallel_execute(txns, state, num_cpus::get());
}
