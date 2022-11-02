#[cfg(test)]
mod tests {
    use block_stm::test_utils::simulated::{
        generate_txns_and_ledger, my_parallel_execute, sequential_execute,
    };
    use either::Either;

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
            let (txns, ledger) = generate_txns_and_ledger(5, 1_000_000, 10_000, 1, 1_000);
            let (s_output, _) = sequential_execute(&txns, &ledger);
            let (mp_output, _) = my_parallel_execute(&txns, &ledger, num_cpus::get());
            let cloned = ledger.clone();
            assert_eq!(
                ledger.apply(Either::Left(s_output)),
                cloned.apply(Either::Right(mp_output))
            );
            #[cfg(feature = "tracing")]
            block_stm::rayon_info!("correctness test passed");
            loop_cnt += 1;
            println!("{} correctness tests passed", loop_cnt);
        }
    }
}
