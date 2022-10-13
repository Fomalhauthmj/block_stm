#[cfg(test)]
mod tests {
    use block_stm::test_utils::{generate_ledger_and_txns, parallel_execute, sequential_execute};

    #[test]
    fn correctness() {
        #[cfg(feature = "tracing")]
        {
            use tracing_subscriber::fmt::format;
            let file_appender = tracing_appender::rolling::minutely("./logs", "correctness.log");
            let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(non_blocking)
                .with_max_level(tracing::Level::TRACE)
                .event_format(format().pretty().with_source_location(true))
                .init();
        }
        let mut loop_cnt = 0;
        loop {
            let (txns, mut sequential_ledger) =
                generate_ledger_and_txns(5, 1_000_000, 1000, 1, 1000);
            let mut parallel_ledger = sequential_ledger.clone();
            sequential_execute(&txns, &mut sequential_ledger);
            parallel_execute(&txns, &mut parallel_ledger, num_cpus::get());
            // ensure the outcomes of parallel and sequential execute are consistent
            assert_eq!(sequential_ledger, parallel_ledger);
            loop_cnt += 1;
            println!("{} correctness tests passed", loop_cnt);
        }
    }
}
