#[cfg(test)]
mod tests {
    use block_stm::test_utils::simulated::{
        generate_txns_and_ledger, my_parallel_execute, sequential_execute,
    };

    #[test]
    fn correctness() {
        let mut loop_cnt = 0;
        loop {
            let (txns, ledger) = generate_txns_and_ledger(5, 1_000_000, 1_000, 1, 1_000);
            let s_output = sequential_execute(&txns, &ledger);
            let mp_output = my_parallel_execute(&txns, &ledger, num_cpus::get());
            assert_eq!(s_output, mp_output);
            loop_cnt += 1;
            println!("{} correctness tests passed", loop_cnt);
        }
    }
}
