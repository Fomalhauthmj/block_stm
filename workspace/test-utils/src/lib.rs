#[cfg(feature = "aptos")]
mod aptos;
#[cfg(feature = "aptos")]
pub use aptos::{
    aptos_parallel_execute, aptos_sequential_execute, generate_txns_and_state, my_parallel_execute,
    my_sequential_execute, AVAILABLE_PARALLEL_EXECUTION, AVAILABLE_SEQUENTIAL_EXECUTION,
};
mod simulated;
pub use simulated::{
    generate_txns_and_ledger, simulated_parallel_execute, simulated_sequential_execute,
};
mod log;
pub use log::set_global_subscriber;
#[cfg(test)]
mod tests {
    use indicatif::ProgressBar;

    use crate::{
        set_global_subscriber,
        simulated::{
            generate_txns_and_ledger, simulated_parallel_execute, simulated_sequential_execute,
        },
    };
    static CORRECTNESS_TESTS_NUM: u64 = 1_000;
    #[test]
    fn correctness() {
        let _guard = set_global_subscriber("./logs", "correctness", tracing::Level::TRACE);
        let pb = ProgressBar::new(CORRECTNESS_TESTS_NUM);
        let mut count = 0;
        while count < CORRECTNESS_TESTS_NUM {
            let (txns, ledger) = generate_txns_and_ledger(5, 10_000);
            let s_output = simulated_sequential_execute(txns.clone(), &ledger);
            let mp_output = simulated_parallel_execute(txns, &ledger, num_cpus::get());
            assert_eq!(s_output, mp_output);
            count += 1;
            pb.inc(1);
        }
        pb.finish_with_message("correctness test passed");
    }
}
