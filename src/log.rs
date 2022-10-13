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
