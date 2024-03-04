use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
pub struct LogsCapture {
    pub logs: Arc<RwLock<Vec<u8>>>,
}

#[derive(Clone)]
pub struct CaptureWriter {
    logs: Weak<RwLock<Vec<u8>>>,
}

impl From<&LogsCapture> for CaptureWriter {
    fn from(capture_logs: &LogsCapture) -> Self {
        Self {
            logs: Arc::downgrade(&capture_logs.logs),
        }
    }
}

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(logs) = self.logs.upgrade() {
            let mut logs_write = logs.write();
            logs_write.extend_from_slice(buf);
            drop(logs_write);
            drop(logs)
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CaptureWriter {
    type Writer = Self;
    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}
