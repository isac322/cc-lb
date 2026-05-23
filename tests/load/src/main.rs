use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use cc_lb_load_tests::{
    BASELINE_PATH, Baseline, EVIDENCE_PATH, EndpointSummary, Evidence, ModeSummary, ToolInfo,
    evaluate_summary, mode_key, round3,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use url::Url;

#[derive(Clone, Debug)]
struct Options {
    mode: String,
    direct_url: String,
    proxy_url: String,
    body_path: PathBuf,
    requests: Option<usize>,
    soak_duration_secs: Option<u64>,
    concurrency: usize,
    warmup: usize,
    output_path: PathBuf,
    evidence_path: PathBuf,
    baseline_path: PathBuf,
    tool: String,
    oha_available: bool,
    fallback: String,
}

#[derive(Clone, Debug)]
struct HttpTarget {
    host: String,
    port: u16,
    path: String,
    host_header: String,
}

#[derive(Debug)]
struct Sample {
    latency_ms: f64,
    sse_events: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::parse()?;
    let baseline = Baseline::read(&options.baseline_path).map_err(anyhow::Error::msg)?;
    let body = fs::read(&options.body_path)
        .with_context(|| format!("read body fixture {}", options.body_path.display()))?;
    let proxy = parse_http_url(&options.proxy_url)?;
    let streaming = options.mode == "streaming";

    if let Some(duration_secs) = options.soak_duration_secs {
        let stats = run_soak_endpoint(proxy, body, options.concurrency, duration_secs).await?;
        println!(
            "mode=soak duration_secs={} concurrency={} success_count={} failure_count={}",
            duration_secs, options.concurrency, stats.success_count, stats.failure_count
        );
        return Ok(());
    }

    let direct = parse_http_url(&options.direct_url)?;
    let requests = options
        .requests
        .ok_or_else(|| anyhow!("--requests is required unless --soak-duration-secs is set"))?;
    let direct_summary = run_endpoint(
        direct,
        body.clone(),
        requests,
        options.concurrency,
        options.warmup,
        streaming,
    )
    .await
    .context("run direct fake Anthropic load")?;
    let proxy_summary = run_endpoint(
        proxy,
        body,
        requests,
        options.concurrency,
        options.warmup,
        streaming,
    )
    .await
    .context("run cc-lb proxy load")?;

    let summary = build_summary(&options, requests, direct_summary, proxy_summary)?;
    let summary = evaluate_summary(summary, &baseline).map_err(anyhow::Error::msg)?;
    write_json(&options.output_path, &summary)?;
    upsert_evidence(&options, summary.clone())?;

    if !summary.passed {
        bail!("{} budget failed: {:?}", summary.mode, summary.passes);
    }

    println!(
        "mode={} direct_p50_ms={} proxy_p50_ms={} p50_overhead_ms={} direct_p99_ms={} proxy_p99_ms={} p99_overhead_ms={}",
        summary.mode,
        summary.direct_p50_ms,
        summary.proxy_p50_ms,
        summary.p50_overhead_ms,
        summary.direct_p99_ms,
        summary.proxy_p99_ms,
        summary.p99_overhead_ms
    );
    if let Some(event_overhead) = summary.streaming_p50_event_overhead_ms {
        println!(
            "mode={} streaming_p50_event_overhead_ms={event_overhead}",
            summary.mode
        );
    }
    Ok(())
}

impl Options {
    fn parse() -> Result<Self> {
        let mut mode = None;
        let mut direct_url = None;
        let mut proxy_url = None;
        let mut body_path = None;
        let mut requests = None;
        let mut soak_duration_secs = None;
        let mut concurrency = None;
        let mut warmup = Some(16_usize);
        let mut output_path = None;
        let mut evidence_path = Some(PathBuf::from(EVIDENCE_PATH));
        let mut baseline_path = Some(PathBuf::from(BASELINE_PATH));
        let mut tool = Some("cc-lb-loadgen".to_owned());
        let mut oha_available = Some(false);
        let mut fallback = Some("deterministic raw TCP load generator".to_owned());

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--mode" => mode = Some(next_arg(&mut args, &arg)?),
                "--direct-url" => direct_url = Some(next_arg(&mut args, &arg)?),
                "--proxy-url" => proxy_url = Some(next_arg(&mut args, &arg)?),
                "--body" => body_path = Some(PathBuf::from(next_arg(&mut args, &arg)?)),
                "--requests" => requests = Some(parse_usize(next_arg(&mut args, &arg)?, &arg)?),
                "--soak-duration-secs" => {
                    soak_duration_secs = Some(parse_u64(next_arg(&mut args, &arg)?, &arg)?)
                }
                "--concurrency" => {
                    concurrency = Some(parse_usize(next_arg(&mut args, &arg)?, &arg)?)
                }
                "--warmup" => warmup = Some(parse_usize(next_arg(&mut args, &arg)?, &arg)?),
                "--output" => output_path = Some(PathBuf::from(next_arg(&mut args, &arg)?)),
                "--evidence" => evidence_path = Some(PathBuf::from(next_arg(&mut args, &arg)?)),
                "--baseline" => baseline_path = Some(PathBuf::from(next_arg(&mut args, &arg)?)),
                "--tool" => tool = Some(next_arg(&mut args, &arg)?),
                "--oha-available" => {
                    oha_available = Some(parse_bool(next_arg(&mut args, &arg)?, &arg)?)
                }
                "--fallback" => fallback = Some(next_arg(&mut args, &arg)?),
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => bail!("unknown argument {other}"),
            }
        }

        let mode = mode.ok_or_else(|| anyhow!("--mode is required"))?;
        mode_key(&mode).map_err(anyhow::Error::msg)?;
        let concurrency = concurrency.ok_or_else(|| anyhow!("--concurrency is required"))?;
        if concurrency == 0 {
            bail!("--concurrency must be positive");
        }
        if let Some(requests) = requests {
            if requests == 0 {
                bail!("--requests must be positive");
            }
        }
        if let Some(duration_secs) = soak_duration_secs {
            if duration_secs == 0 {
                bail!("--soak-duration-secs must be positive");
            }
            if mode != "non-streaming" {
                bail!("--soak-duration-secs supports only --mode non-streaming");
            }
        } else if requests.is_none() {
            bail!("--requests is required unless --soak-duration-secs is set");
        }

        Ok(Self {
            mode,
            direct_url: direct_url.ok_or_else(|| anyhow!("--direct-url is required"))?,
            proxy_url: proxy_url.ok_or_else(|| anyhow!("--proxy-url is required"))?,
            body_path: body_path.ok_or_else(|| anyhow!("--body is required"))?,
            requests,
            soak_duration_secs,
            concurrency,
            warmup: warmup.unwrap_or(16),
            output_path: output_path.ok_or_else(|| anyhow!("--output is required"))?,
            evidence_path: evidence_path.unwrap_or_else(|| PathBuf::from(EVIDENCE_PATH)),
            baseline_path: baseline_path.unwrap_or_else(|| PathBuf::from(BASELINE_PATH)),
            tool: tool.unwrap_or_else(|| "cc-lb-loadgen".to_owned()),
            oha_available: oha_available.unwrap_or(false),
            fallback: fallback.unwrap_or_else(|| "deterministic raw TCP load generator".to_owned()),
        })
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow!("{name} requires a value"))
}

fn parse_usize(value: String, name: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("parse {name}={value}"))
}

fn parse_u64(value: String, name: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("parse {name}={value}"))
}

fn parse_bool(value: String, name: &str) -> Result<bool> {
    match value.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("{name} must be true or false"),
    }
}

fn print_usage() {
    println!(
        "usage: cc-lb-loadgen --mode <non-streaming|streaming> --direct-url URL --proxy-url URL --body PATH (--requests N --output PATH | --soak-duration-secs N) --concurrency N"
    );
}

fn parse_http_url(value: &str) -> Result<HttpTarget> {
    let url = Url::parse(value).with_context(|| format!("parse url {value}"))?;
    if url.scheme() != "http" {
        bail!("only http loopback URLs are supported: {value}");
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("URL missing host: {value}"))?
        .to_owned();
    if host != "127.0.0.1" && host != "localhost" {
        bail!("load harness only targets loopback hosts, got {host}");
    }
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("URL missing port: {value}"))?;
    let mut path = url.path().to_owned();
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    let host_header = format!("{host}:{port}");
    Ok(HttpTarget {
        host,
        port,
        path,
        host_header,
    })
}

async fn run_endpoint(
    target: HttpTarget,
    body: Vec<u8>,
    requests: usize,
    concurrency: usize,
    warmup: usize,
    streaming: bool,
) -> Result<EndpointSummary> {
    let request = Arc::new(build_request(&target, &body, streaming));
    for _ in 0..warmup {
        single_request(&target, request.clone(), streaming).await?;
    }

    let target = Arc::new(target);
    let next = Arc::new(AtomicUsize::new(0));
    let (sender, mut receiver) = mpsc::channel(requests);
    let worker_count = concurrency.min(requests);
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let sender = sender.clone();
        let target = target.clone();
        let request = request.clone();
        let next = next.clone();
        let handle = tokio::spawn(async move {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= requests {
                    return Ok::<(), anyhow::Error>(());
                }
                let sample = single_request(&target, request.clone(), streaming).await?;
                if sender.send(sample).await.is_err() {
                    return Ok(());
                }
            }
        });
        handles.push(handle);
    }
    drop(sender);

    let mut samples = Vec::with_capacity(requests);
    while let Some(sample) = receiver.recv().await {
        samples.push(sample);
    }
    for handle in handles {
        handle.await.context("join load worker")??;
    }
    if samples.len() != requests {
        bail!("expected {requests} samples, got {}", samples.len());
    }

    Ok(endpoint_summary(samples, requests, concurrency))
}

#[derive(Debug)]
struct SoakStats {
    success_count: usize,
    failure_count: usize,
}

async fn run_soak_endpoint(
    target: HttpTarget,
    body: Vec<u8>,
    concurrency: usize,
    duration_secs: u64,
) -> Result<SoakStats> {
    let request = Arc::new(build_request(&target, &body, false));
    let target = Arc::new(target);
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let (sender, mut receiver) = mpsc::channel(concurrency * 2);
    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let sender = sender.clone();
        let target = target.clone();
        let request = request.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if Instant::now() >= deadline {
                    return;
                }
                let ok = single_request(&target, request.clone(), false)
                    .await
                    .is_ok();
                if sender.send(ok).await.is_err() {
                    return;
                }
            }
        });
        handles.push(handle);
    }
    drop(sender);

    let mut success_count = 0;
    let mut failure_count = 0;
    while let Some(ok) = receiver.recv().await {
        if ok {
            success_count += 1;
        } else {
            failure_count += 1;
        }
    }
    for handle in handles {
        handle.await.context("join soak load worker")?;
    }

    Ok(SoakStats {
        success_count,
        failure_count,
    })
}

fn build_request(target: &HttpTarget, body: &[u8], streaming: bool) -> Vec<u8> {
    let accept = if streaming {
        "Accept: text/event-stream\r\n"
    } else {
        ""
    };
    let mut request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nx-api-key: sk-ant-test\r\nanthropic-version: 2023-06-01\r\ncontent-type: application/json\r\n{}content-length: {}\r\nConnection: close\r\n\r\n",
        target.path,
        target.host_header,
        accept,
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(body);
    request
}

async fn single_request(
    target: &HttpTarget,
    request: Arc<Vec<u8>>,
    streaming: bool,
) -> Result<Sample> {
    let start = Instant::now();
    let bytes = tokio::time::timeout(Duration::from_secs(15), async {
        let mut stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
        stream.write_all(&request).await?;
        let mut bytes = Vec::with_capacity(4096);
        stream.read_to_end(&mut bytes).await?;
        Ok::<Vec<u8>, std::io::Error>(bytes)
    })
    .await
    .context("request timed out")?
    .context("request I/O")?;
    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = status_code(&bytes)?;
    if status != 200 {
        bail!("unexpected status {status}: {}", preview_response(&bytes));
    }
    let sse_events = if streaming {
        let count = count_sse_events(&bytes);
        if count == 0 {
            bail!(
                "streaming response had no SSE events: {}",
                preview_response(&bytes)
            );
        }
        count
    } else {
        0
    };
    Ok(Sample {
        latency_ms,
        sse_events,
    })
}

fn status_code(bytes: &[u8]) -> Result<u16> {
    let text = String::from_utf8_lossy(bytes);
    text.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| anyhow!("missing HTTP status line"))
}

fn count_sse_events(bytes: &[u8]) -> usize {
    let body = match bytes.windows(4).position(|window| window == b"\r\n\r\n") {
        Some(index) => &bytes[index + 4..],
        None => bytes,
    };
    String::from_utf8_lossy(body)
        .lines()
        .filter(|line| line.trim_start().starts_with("event:"))
        .count()
}

fn preview_response(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.chars().take(240).collect()
}

fn endpoint_summary(samples: Vec<Sample>, requests: usize, concurrency: usize) -> EndpointSummary {
    let mut latencies = samples
        .iter()
        .map(|sample| sample.latency_ms)
        .collect::<Vec<_>>();
    latencies.sort_by(f64::total_cmp);
    let total_sse_events = samples
        .iter()
        .map(|sample| sample.sse_events)
        .sum::<usize>();
    let mut events = samples
        .iter()
        .map(|sample| sample.sse_events as f64)
        .collect::<Vec<_>>();
    events.sort_by(f64::total_cmp);
    let mean_ms = latencies.iter().sum::<f64>() / latencies.len() as f64;
    EndpointSummary {
        requests,
        concurrency,
        success_count: samples.len(),
        p50_ms: round3(percentile(&latencies, 0.50)),
        p99_ms: round3(percentile(&latencies, 0.99)),
        min_ms: round3(*latencies.first().unwrap_or(&0.0)),
        max_ms: round3(*latencies.last().unwrap_or(&0.0)),
        mean_ms: round3(mean_ms),
        total_sse_events,
        p50_sse_events_per_response: round3(percentile(&events, 0.50)),
    }
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((sorted.len() - 1) as f64 * quantile).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn build_summary(
    options: &Options,
    requests: usize,
    direct: EndpointSummary,
    proxy: EndpointSummary,
) -> Result<ModeSummary> {
    let p50_overhead_ms = round3((proxy.p50_ms - direct.p50_ms).max(0.0));
    let p99_overhead_ms = round3((proxy.p99_ms - direct.p99_ms).max(0.0));
    let streaming_events_per_response = if options.mode == "streaming" {
        Some(
            direct
                .p50_sse_events_per_response
                .min(proxy.p50_sse_events_per_response),
        )
    } else {
        None
    };
    let (direct_per_event, proxy_per_event, event_overhead) = match streaming_events_per_response {
        Some(events) if events > 0.0 => {
            let direct_per_event = round3(direct.p50_ms / events);
            let proxy_per_event = round3(proxy.p50_ms / events);
            let event_overhead = round3((proxy_per_event - direct_per_event).max(0.0));
            (
                Some(direct_per_event),
                Some(proxy_per_event),
                Some(event_overhead),
            )
        }
        Some(_) => bail!("streaming summary has zero events per response"),
        None => (None, None, None),
    };

    Ok(ModeSummary {
        mode: options.mode.clone(),
        tool: options.tool.clone(),
        requests,
        concurrency: options.concurrency,
        direct_p50_ms: direct.p50_ms,
        proxy_p50_ms: proxy.p50_ms,
        p50_overhead_ms,
        direct_p99_ms: direct.p99_ms,
        proxy_p99_ms: proxy.p99_ms,
        p99_overhead_ms,
        direct,
        proxy,
        streaming_events_per_response,
        streaming_direct_p50_ms_per_event: direct_per_event,
        streaming_proxy_p50_ms_per_event: proxy_per_event,
        streaming_p50_event_overhead_ms: event_overhead,
        thresholds: json!({}),
        passes: BTreeMap::new(),
        passed: false,
    })
}

fn upsert_evidence(options: &Options, summary: ModeSummary) -> Result<()> {
    let generated_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let tool = ToolInfo {
        preferred: "oha".to_owned(),
        selected: options.tool.clone(),
        oha_available: options.oha_available,
        fallback: options.fallback.clone(),
    };
    let mut evidence = if options.evidence_path.exists() {
        match Evidence::read(&options.evidence_path) {
            Ok(mut existing) => {
                existing.generated_at_unix_ms = generated_at_unix_ms;
                existing.benchmark_tool = tool;
                existing
            }
            Err(_) => Evidence::empty(tool, generated_at_unix_ms),
        }
    } else {
        Evidence::empty(tool, generated_at_unix_ms)
    };

    evidence.modes.insert(
        mode_key(&summary.mode)
            .map_err(anyhow::Error::msg)?
            .to_owned(),
        summary,
    );
    evidence.complete =
        evidence.modes.contains_key("non_streaming") && evidence.modes.contains_key("streaming");
    evidence.passed = evidence.complete && evidence.modes.values().all(|summary| summary.passed);
    write_json(&options.evidence_path, &evidence)
}

fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent directory {}", parent.display()))?;
    }
    let text = serde_json::to_string_pretty(value).context("serialize JSON")?;
    fs::write(path, format!("{text}\n")).with_context(|| format!("write {}", path.display()))
}
