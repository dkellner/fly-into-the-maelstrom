use std::time::Instant;

/// A logger that writes to STDERR and prepends relative timestamps.
#[derive(Debug)]
pub struct Logger {
    start_time: Instant,
}

impl Default for Logger {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
        }
    }
}

impl Logger {
    pub fn log(&self, message: &str) {
        let elapsed = Instant::now() - self.start_time;
        eprintln!("[{:.1}] {}", elapsed.as_millis() as f32 / 1000.0, message);
    }
}
