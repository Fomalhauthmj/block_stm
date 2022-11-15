#[cfg(test)]
mod tests {
    use block_stm::test_utils::simulated::{
        generate_txns_and_ledger, my_parallel_execute, sequential_execute,
    };
    use either::Either;

    #[test]
    fn correctness() {
        let mut loop_cnt = 0;
        loop {
            let (txns, ledger) = generate_txns_and_ledger(5, 1_000_000, 10_000, 1, 1_000);
            let (s_output, _) = sequential_execute(&txns, &ledger);
            let (mp_output, _) = my_parallel_execute(&txns, &ledger, num_cpus::get());
            let cloned = ledger.clone();
            assert_eq!(
                ledger.apply(Either::Left(s_output)),
                cloned.apply(Either::Right(mp_output))
            );
            loop_cnt += 1;
            println!("{} correctness tests passed", loop_cnt);
        }
    }
}
