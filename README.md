An implement of [Block-STM](http://arxiv.org/abs/2203.06871),which still under development.

# Design
We want to reach a balance between theory and practice.
So,we have tried our best to make the design of this library corresponds to the Block-STM paper.
Besides,for better re-produceable performace,we have applied many implement techs from `aptos-core` to boost the performance of this library.

# Test
use `cargo test -- --nocapture` to test the correctness of parallel execute,whose outcome should be consistent with sequential execute.

**Note**:
Currently,we generate random txns for each correctness test,and parallel execute only once.Idealy,we should parallel execute multiple times to ensure deterministic of parallel execute in each test.
Additionally,correctness test is a dead loop,which will test forever until test failure/blocking.

# Benchmark
use `cargo bench --features benchmark` to benchmark the performace (throughput) of sequential/parallel execute.

**Note**:
Currently,for benchmark,we add
`std::thread::sleep(std::time::Duration::from_millis(1))`
before each execution of transaction to simulate real VM execution.

## Profiling
use `cargo bench --bench benchmark --features benchmark -- --profile-time <profile-time>` to profiling.

# Limitations and Known Problems
Currently, please try to search all `TODO` or `FIXME` in source code to check the problems.

# Acknowledgment
[aptos-core](https://github.com/aptos-labs/aptos-core)