#[cfg(test)]
mod tests {
    use block_stm::{
        rayon_info,
        test_utils::{generate_ledger_and_txns, parallel_execute, sequential_execute},
    };
    use tracing_subscriber::fmt::format;

    #[test]
    fn correctness() {
        let file_appender = tracing_appender::rolling::minutely("./logs", "test.log");
        let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(non_blocking)
            .with_max_level(tracing::Level::TRACE)
            .event_format(format().pretty().with_source_location(true))
            .init();
        let mut loop_cnt = 0;
        while loop_cnt < 5 {
            let (ledger, txns) = generate_ledger_and_txns(5, 1_000_000, 1000, 1, 1000);
            rayon_info!("Txns:{:?}", txns);
            assert_eq!(
                sequential_execute(txns.clone(), ledger.clone()),
                parallel_execute(txns, ledger)
            );
            rayon_info!("correctness test passed");
            loop_cnt += 1;
            println!("have passed {} correctness tests", loop_cnt);
        }
    }
}
