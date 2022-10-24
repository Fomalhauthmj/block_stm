#[cfg(test)]
mod tests {
    #[test]
    fn correctness() {
        #[cfg(feature = "tracing")]
        let _guard = {
            use tracing_subscriber::fmt::format;
            let file_appender = tracing_appender::rolling::minutely("./logs", "correctness.log");
            let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(non_blocking)
                .with_max_level(tracing::Level::INFO)
                .event_format(format().pretty().with_source_location(true))
                .init();
            guard
        };
        let mut loop_cnt = 0;
        loop {
            #[cfg(feature = "tracing")]
            block_stm::rayon_info!("correctness test will start");
            #[cfg(feature = "simulated_test_utils")]
            {
                use block_stm::test_utils::{
                    generate_txns_and_ledger, parallel_execute, sequential_execute,
                };
                let (txns, mut sequential_ledger) =
                    generate_txns_and_ledger(4, 1_000_000, 1_000, 100, 10_000);
                let mut parallel_ledger = sequential_ledger.clone();
                sequential_execute(&txns, &mut sequential_ledger);
                parallel_execute(&txns, &mut parallel_ledger, num_cpus::get());
                // ensure the outcomes of parallel and sequential execute are consistent
                assert_eq!(sequential_ledger, parallel_ledger);
            }
            #[cfg(feature = "aptos_test_utils")]
            {
                use block_stm::test_utils::{
                    aptos_parallel_execute_and_apply, aptos_sequential_execute_and_apply,
                    generate_aptos_txns_and_state,
                };
                let (txns, sequential_state) = generate_aptos_txns_and_state(3, 10);
                let parallel_state = sequential_state.clone();
                let sequential_state =
                    aptos_sequential_execute_and_apply(txns.clone(), sequential_state);
                let parallel_state =
                    aptos_parallel_execute_and_apply(txns, parallel_state, num_cpus::get());
                // ensure the outcomes of parallel and sequential execute are consistent
                if sequential_state.state_data != parallel_state.state_data {
                    panic!()
                }
            }
            #[cfg(feature = "tracing")]
            block_stm::rayon_info!("correctness test passed");
            loop_cnt += 1;
            println!("{} correctness tests passed", loop_cnt);
        }
    }
}
