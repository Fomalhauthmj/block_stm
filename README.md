An implement of [Block-STM](http://arxiv.org/abs/2203.06871),which still under development.

# Design
We want to reach a balance between theory and practice.
So,we have tried our best to make the design of this library corresponds to the Block-STM paper.
Besides,for better re-produceable performace,we have applied many implement techs from `aptos-core` to boost the performance of this library.

# Test
use `cargo test --all-features -- --nocapture` to test the correctness of parallel execute,whose outcome should be consistent with sequential execute.

**Note**:
Currently,we generate random txns for each correctness test,and parallel execute only once.Idealy,we should parallel execute multiple times to ensure deterministic of parallel execute in each test.
Additionally,correctness test is a dead loop,which will test forever until test failure/blocking.

# Benchmark
use `cargo bench --bench aptos` to benchmark the performace (throughput) of sequential/parallel execute.

## Profiling
use `cargo bench --bench aptos -- --profile-time <profile-time>` to profiling.

# Limitations and Known Problems
Sadly, our implement of block-stm currently doesn't support `DeltaOp` and any other features in `aptos-core`,which make the performace of this library exactly poor.
Besides, please try to search all `TODO` or `FIXME` in source code to check the problems.

# Acknowledgment
[aptos-core](https://github.com/aptos-labs/aptos-core)