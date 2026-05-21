use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use cc_lb_observability::{RedactingMakeWriter, RedactionLayer, RedactionPolicy};
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Default)]
pub struct CapturedWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl CapturedWriter {
    pub fn output(&self) -> String {
        let guard = self.buffer.lock().unwrap();
        String::from_utf8_lossy(&guard).into_owned()
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedWriter {
    type Writer = CapturedWrite;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWrite {
            buffer: self.buffer.clone(),
        }
    }
}

pub struct CapturedWrite {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl Write for CapturedWrite {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut guard = self.buffer.lock().unwrap();
        guard.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn capture_event(policy: RedactionPolicy, emit: impl FnOnce()) -> String {
    let captured = CapturedWriter::default();
    let subscriber = tracing_subscriber::registry()
        .with(RedactionLayer::new(policy))
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .with_writer(RedactingMakeWriter::new(captured.clone(), policy)),
        );

    tracing::subscriber::with_default(subscriber, emit);
    captured.output()
}
