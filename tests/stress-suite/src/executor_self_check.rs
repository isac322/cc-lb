use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde::Serialize;

use crate::executor::{ExecutorConfig, execute_open_loop, planned_request};
use crate::executor_http::HttpRequestSender;
use crate::sse_timing::SseTiming;
use crate::verdict::Verdict;

pub fn run_executor(output: &Path) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("create executor runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let evidence = runtime.block_on(run_executor_check());
    match evidence.and_then(|evidence| write_json(output, &evidence)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("executor self-check failed: {error}");
            ExitCode::FAILURE
        }
    }
}

pub fn run_sse(input: &Path, output: &Path) -> ExitCode {
    let bytes = match std::fs::read(input) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("read SSE fixture {}: {error}", input.display());
            return ExitCode::FAILURE;
        }
    };
    let mut timing = SseTiming::new(64 * 1024);
    for (index, chunk) in bytes.chunks(7).enumerate() {
        timing.push(
            chunk,
            Duration::from_millis((index as u64).saturating_mul(5)),
        );
    }
    let summary = timing.finish();
    let evidence = TruncatedSseEvidence {
        unexpected_stream_truncation_count: u64::from(summary.unexpected_truncation),
        malformed_sse_count: u64::from(summary.malformed_sse),
        malformed_json_count: u64::from(summary.malformed_json),
        verdict_contribution: if summary.unexpected_truncation {
            Verdict::Fail
        } else {
            Verdict::Pass
        },
    };
    match write_json(output, &evidence) {
        Ok(()) if summary.unexpected_truncation => ExitCode::FAILURE,
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("write SSE self-check {}: {error}", output.display());
            ExitCode::FAILURE
        }
    }
}

async fn run_executor_check() -> Result<crate::executor_metrics::ExecutorEvidence, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let server = tokio::spawn(async move {
        let app = Router::new().route("/v1/messages", post(fake_messages));
        let _ = axum::serve(listener, app).await;
    });
    let endpoint = format!("http://{address}/v1/messages")
        .parse()
        .map_err(|error: http::uri::InvalidUri| error.to_string())?;
    let sender = Arc::new(HttpRequestSender::new(endpoint, Duration::from_secs(2)));
    let requests = vec![
        planned_request("json", 0, false),
        planned_request("stream", 1, true),
    ];
    let report = execute_open_loop(requests, sender, ExecutorConfig::default()).await;
    server.abort();
    Ok(report.evidence)
}

async fn fake_messages(headers: HeaderMap) -> Response {
    let stream = headers.get("x-stress-stream").is_some();
    let body = if stream {
        "event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"self-check\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
    } else {
        "{\"type\":\"message\",\"content\":[]}"
    };
    (
        StatusCode::OK,
        [(
            "content-type",
            if stream {
                "text/event-stream"
            } else {
                "application/json"
            },
        )],
        Body::from(body),
    )
        .into_response()
}

#[derive(Serialize)]
struct TruncatedSseEvidence {
    unexpected_stream_truncation_count: u64,
    malformed_sse_count: u64,
    malformed_json_count: u64,
    verdict_contribution: Verdict,
}

fn write_json(path: &Path, evidence: &impl Serialize) -> Result<(), String> {
    crate::evidence_redaction::write_redacted_json(path, evidence)
}
