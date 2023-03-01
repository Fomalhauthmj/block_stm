/// logging in tests and benchmark
pub fn set_global_subscriber(
    directory: &str,
    file_name_prefix: &str,
    filter: tracing::Level,
) -> anyhow::Result<tracing_appender::non_blocking::WorkerGuard> {
    let file_appender = tracing_appender::rolling::hourly(directory, file_name_prefix);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .pretty()
        .with_ansi(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_max_level(filter)
        .with_writer(non_blocking)
        .init();
    Ok(guard)
}
