use std::io::Write;
use std::sync::{Arc, Mutex};

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn logs() -> (Arc<Mutex<Vec<u8>>>, tracing::Dispatch) {
    logs_at(tracing::Level::WARN)
}

pub fn logs_at(level: tracing::Level) -> (Arc<Mutex<Vec<u8>>>, tracing::Dispatch) {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_ansi(false)
        .without_time()
        .with_writer(move || LogWriter(writer.clone()))
        .finish();
    (logs, tracing::Dispatch::new(subscriber))
}
