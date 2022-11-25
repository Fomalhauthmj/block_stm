///
pub fn try_init_global_subscriber(
    directory: &str,
    file_name_prefix: &str,
    filter: tracing::Level,
) -> anyhow::Result<tracing_appender::non_blocking::WorkerGuard> {
    use anyhow::anyhow;
    use tracing_subscriber::fmt::format;
    let file_appender = tracing_appender::rolling::hourly(directory, file_name_prefix);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(non_blocking)
        .with_max_level(filter)
        .event_format(format().pretty().with_source_location(true))
        .try_init()
        .map_or_else(|e| Err(anyhow!("{:?}", e)), |_| Ok(guard))
}
/// convenient `tracing::trace!` macro with current rayon thread index
#[macro_export]
macro_rules! rayon_trace {
    ($fmt:expr) => {
        tracing::trace!(
            "{} at Rayon Thread:{:?}",
            $fmt,
            rayon::current_thread_index()
        );
    };
    ($fmt:expr, $($args:tt)*) => {
        let log = format!($fmt,$($args)*);
        tracing::trace!(
            "{} at Rayon Thread:{:?}",
            log,
            rayon::current_thread_index()
        );
    };
}
/// convenient `tracing::debug!` macro with current rayon thread index
#[macro_export]
macro_rules! rayon_debug {
    ($fmt:expr) => {
        tracing::debug!(
            "{} at Rayon Thread:{:?}",
            $fmt,
            rayon::current_thread_index()
        );
    };
    ($fmt:expr, $($args:tt)*) => {
        let log = format!($fmt,$($args)*);
        tracing::debug!(
            "{} at Rayon Thread:{:?}",
            log,
            rayon::current_thread_index()
        );
    };
}
/// convenient `tracing::info!` macro with current rayon thread index
#[macro_export]
macro_rules! rayon_info {
    ($fmt:expr) => {
        tracing::info!(
            "{} at Rayon Thread:{:?}",
            $fmt,
            rayon::current_thread_index()
        );
    };
    ($fmt:expr, $($args:tt)*) => {
        let log = format!($fmt,$($args)*);
        tracing::info!(
            "{} at Rayon Thread:{:?}",
            log,
            rayon::current_thread_index()
        );
    };
}
/// convenient `tracing::warn!` macro with current rayon thread index
#[macro_export]
macro_rules! rayon_warn {
    ($fmt:expr) => {
        tracing::warn!(
            "{} at Rayon Thread:{:?}",
            $fmt,
            rayon::current_thread_index()
        );
    };
    ($fmt:expr, $($args:tt)*) => {
        let log = format!($fmt,$($args)*);
        tracing::warn!(
            "{} at Rayon Thread:{:?}",
            log,
            rayon::current_thread_index()
        );
    };
}
/// convenient `tracing::error!` macro with current rayon thread index
#[macro_export]
macro_rules! rayon_error {
    ($fmt:expr) => {
        tracing::error!(
            "{} at Rayon Thread:{:?}",
            $fmt,
            rayon::current_thread_index()
        );
    };
    ($fmt:expr, $($args:tt)*) => {
        let log = format!($fmt,$($args)*);
        tracing::error!(
            "{} at Rayon Thread:{:?}",
            log,
            rayon::current_thread_index()
        );
    };
}
