mod common;

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use cc_lb_plugin_api::sign_request;
use cc_lb_signer_gcp::StaticGcpTokenProvider;

#[tokio::test(flavor = "current_thread")]
async fn no_token_leak() {
    let raw_token = "ya29.no-token-leak";
    let provider = Arc::new(StaticGcpTokenProvider::new(common::token(raw_token, 3600)));
    let signer = common::signer(provider);
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let writer_buffer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(move || BufferWriter(writer_buffer.clone()))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let _signed = sign_request(&signer, common::shaped_request())
        .await
        .expect("request signs");

    let logs = String::from_utf8(buffer.lock().expect("buffer lock").clone()).expect("utf8 logs");
    assert!(logs.contains("gcp oauth bearer applied"));
    assert!(!logs.contains(raw_token));
}

#[derive(Clone)]
struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut output = self
            .0
            .lock()
            .map_err(|_| io::Error::other("buffer lock poisoned"))?;
        output.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
