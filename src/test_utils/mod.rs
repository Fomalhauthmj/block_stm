use std::time::Duration;

/// smart contract (move-based) transfer transaction
#[cfg(feature = "aptos_transfer")]
pub mod aptos;
// log utils
mod log;
pub use log::try_init_global_subscriber;
/// simulated transfer transaction
#[cfg(feature = "simulated_transfer")]
pub mod simulated;

/// info during execution process
#[derive(Debug, Default)]
pub struct BenchmarkInfo {
    /// total walltime
    pub total_time: Duration,
    /// execute walltime
    pub execute_time: Option<Duration>,
    /// collect walltime
    pub collect_time: Option<Duration>,
}
///
#[derive(Debug, Default)]
pub struct BenchmarkInfos {
    data: Vec<BenchmarkInfo>,
    mean: BenchmarkInfo,
}
impl BenchmarkInfos {
    ///
    pub fn add_info(&mut self, info: BenchmarkInfo) {
        self.data.push(info);
        let mut t = Duration::ZERO;
        let mut e = Duration::ZERO;
        let mut c = Duration::ZERO;
        self.data.iter().for_each(
            |BenchmarkInfo {
                 total_time,
                 execute_time,
                 collect_time,
             }| {
                t += *total_time;
                if let Some(execute_time) = execute_time {
                    e += *execute_time;
                }
                if let Some(collect_time) = collect_time {
                    c += *collect_time;
                }
            },
        );
        self.mean = BenchmarkInfo {
            total_time: t / self.data.len() as u32,
            execute_time: if e == Duration::ZERO {
                None
            } else {
                Some(e / self.data.len() as u32)
            },
            collect_time: if c == Duration::ZERO {
                None
            } else {
                Some(c / self.data.len() as u32)
            },
        }
    }
    ///
    pub fn clear_infos(&mut self) {
        self.data.clear();
    }
    ///
    pub fn mean(&self) -> String {
        format!(
            "Mean total time:{:?},Mean execute time:{:?},Mean collect time:{:?}",
            self.mean.total_time, self.mean.execute_time, self.mean.collect_time
        )
    }
}
