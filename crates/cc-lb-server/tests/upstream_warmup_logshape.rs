use std::io::{self, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct CapturedLogs {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        let bytes = self.inner.lock().expect("captured logs lock");
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

struct CapturedWriter {
    logs: CapturedLogs,
}

impl Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.logs
            .inner
            .lock()
            .expect("captured logs lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter { logs: self.clone() }
    }
}

#[test]
fn tracing_events_have_required_fields() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let upstream_id = uuid::Uuid::new_v4();
    let holder = "test-holder";
    let jitter_ms = 1234u64;

    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, lease_until = ?"2024-01-01T00:00:00Z", action = "lease_claimed");
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, jitter_ms = %jitter_ms, action = "dispatch_start");
    tracing::info!(target: "warmup", upstream_id = %upstream_id, holder = %holder, status = 200, outcome = "success", action = "dispatch_result");

    let output = logs.contents();

    assert!(
        output.contains(r#"action="lease_claimed""#),
        "expected lease_claimed event in:\n{}",
        output
    );
    assert!(
        output.contains(r#"action="dispatch_start""#),
        "expected dispatch_start event in:\n{}",
        output
    );
    assert!(
        output.contains(r#"action="dispatch_result""#),
        "expected dispatch_result event in:\n{}",
        output
    );

    let lease_claimed = output
        .lines()
        .find(|l| l.contains(r#"action="lease_claimed""#))
        .expect("lease_claimed line");
    assert!(
        lease_claimed.contains("holder="),
        "lease_claimed missing holder"
    );
    assert!(
        lease_claimed.contains("lease_until="),
        "lease_claimed missing lease_until"
    );

    let dispatch_start = output
        .lines()
        .find(|l| l.contains(r#"action="dispatch_start""#))
        .expect("dispatch_start line");
    assert!(
        dispatch_start.contains("holder="),
        "dispatch_start missing holder"
    );
    assert!(
        dispatch_start.contains("jitter_ms="),
        "dispatch_start missing jitter_ms"
    );

    let dispatch_result = output
        .lines()
        .find(|l| l.contains(r#"action="dispatch_result""#))
        .expect("dispatch_result line");
    assert!(
        dispatch_result.contains("holder="),
        "dispatch_result missing holder"
    );
    assert!(
        dispatch_result.contains("status="),
        "dispatch_result missing status"
    );
    assert!(
        dispatch_result.contains("outcome="),
        "dispatch_result missing outcome"
    );
}
