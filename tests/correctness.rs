#[cfg(test)]
mod tests {
    use block_stm::test_utils::simulated::{
        generate_txns_and_ledger, my_parallel_execute, sequential_execute,
    };
    use either::Either;

    #[test]
    fn correctness() {
        let _guard = block_stm::test_utils::try_init_global_subscriber(
            "./logs",
            "correctness",
            tracing::Level::TRACE,
        );
        let mut loop_cnt = 0;
        loop {
            let (txns, ledger) = generate_txns_and_ledger(5, 1_000, 1_000_000, 1, 1_000);
            let s_output = sequential_execute(&txns, &ledger);
            let c1 = ledger.clone();
            let c2 = ledger.clone();
            let mp_output = my_parallel_execute(txns, ledger, num_cpus::get());
            assert_eq!(
                c1.apply(Either::Left(s_output)),
                c2.apply(Either::Right(mp_output))
            );
            loop_cnt += 1;
            println!("{} correctness tests passed", loop_cnt);
        }
    }
}
