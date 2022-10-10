An implement of [Block-STM](http://arxiv.org/abs/2203.06871),which still under development.

# Test
use `cargo test -- --nocapture` to test the correctness of parallel execute,whose outcome should be consistent with sequential execute.

**Note**:
Currently,we generate random txns for each correctness test,and parallel execute only once.Idealy,we should parallel execute multiple times to ensure deterministic of parallel execute in each test.
Additionally,correctness test is a dead loop,which will test forever until test failure.

# Benchmark
use `cargo bench --features benchmark` to benchmark the performace (throughput) of sequential/parallel execute.

**Note**:
Currently,for benchmark,we add
`std::thread::sleep(std::time::Duration::from_millis(1))`
before each execution of transaction to simulate real VM execution.

# Known Problems
In conflicting level benchmark,the performance of conflicting level(100) is outperform than conflicting level(1000),which is counterintuitive because later is less conflicting.

# Acknowledgment
[aptos-core](https://github.com/aptos-labs/aptos-core)