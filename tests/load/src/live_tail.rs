use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use cc_lb_load_tests::{
    LIVE_TAIL_EVIDENCE_SCHEMA_VERSION, LiveTailSoakEvidence, MetricSeriesSummary, SoakProfile,
    evaluate_live_tail_soak, round3,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

use super::{Options, build_request, parse_http_url, single_request, write_json};

#[derive(Debug)]
struct ProxyStats {
    success: AtomicU64,
    failure: AtomicU64,
    streaming: AtomicU64,
    non_streaming: AtomicU64,
}

#[derive(Clone, Debug, Default)]
struct SubscriberStats {
    events: u64,
    messages: u64,
    resets: u64,
    reconnects: u64,
    last_event_id: Option<String>,
}

#[derive(Clone, Debug)]
struct MetricSample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

#[derive(Debug)]
struct ScrapeResult {
    series: BTreeMap<String, Vec<f64>>,
    last_values: BTreeMap<String, f64>,
}

#[derive(Clone, Copy, Debug)]
struct RssSample {
    elapsed_secs: f64,
    mib: f64,
}

#[derive(Clone)]
struct ProxyLoadConfig {
    target: Arc<super::HttpTarget>,
    non_stream_request: Arc<Vec<u8>>,
    stream_request: Arc<Vec<u8>>,
    rps: u64,
    duration_secs: u64,
    stream_ratio: u8,
    max_in_flight: usize,
}

pub async fn run(options: Options) -> Result<()> {
    let profile = std::env::var("CC_LB_LOAD_PROFILE")
        .unwrap_or_else(|_| "smoke".to_owned())
        .parse::<SoakProfile>()
        .map_err(anyhow::Error::msg)?;
    let rps = options.rps.context("--rps is required")?;
    let duration_secs = options
        .duration_secs
        .context("--duration-secs is required")?;
    let stream_ratio = options.stream_ratio.unwrap_or(0);
    let max_in_flight = options.max_in_flight.unwrap_or(rps as usize).max(1);
    let sse_subscribers = options.sse_subscribers.unwrap_or(0);
    let reconnect_churn_secs = options.reconnect_churn_secs.unwrap_or(0);
    let metrics_interval = options.metrics_scrape_interval_secs.unwrap_or(5).max(1);
    let output_path = options.output_path;
    let metrics_output = options
        .metrics_output
        .context("--metrics-output is required")?;
    let proxy_url = options.proxy_url;
    let api_key = options.api_key;
    let body_path = options.body_path;
    let stream_body_path = options
        .stream_body_path
        .clone()
        .unwrap_or_else(|| body_path.clone());
    let sse_stream_url = options
        .sse_stream_url
        .context("--sse-stream-url is required")?;
    let admin_token = options.admin_token.context("--admin-token is required")?;
    let metrics_scrape_url = options
        .metrics_scrape_url
        .context("--metrics-scrape-url is required")?;

    let target = Arc::new(parse_http_url(&proxy_url)?);
    let non_stream_body = fs::read(&body_path)
        .with_context(|| format!("read non-stream body {}", body_path.display()))?;
    let stream_body = fs::read(&stream_body_path)
        .with_context(|| format!("read stream body {}", stream_body_path.display()))?;
    let non_stream_request = Arc::new(build_request(&target, &non_stream_body, false, &api_key));
    let stream_request = Arc::new(build_request(&target, &stream_body, true, &api_key));
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let started_at_unix_ms = unix_ms();
    let rss_pid = options.rss_pid;
    let proxy_stats = Arc::new(ProxyStats {
        success: AtomicU64::new(0),
        failure: AtomicU64::new(0),
        streaming: AtomicU64::new(0),
        non_streaming: AtomicU64::new(0),
    });

    let proxy_task = tokio::spawn(run_proxy_load(
        ProxyLoadConfig {
            target,
            non_stream_request,
            stream_request,
            rps,
            duration_secs,
            stream_ratio,
            max_in_flight,
        },
        proxy_stats.clone(),
    ));
    let sse_task = tokio::spawn(run_subscribers(
        sse_stream_url,
        admin_token,
        sse_subscribers,
        reconnect_churn_secs,
        deadline,
    ));
    let metrics_task = tokio::spawn(scrape_metrics_until(
        metrics_scrape_url,
        metrics_interval,
        deadline,
        metrics_output,
    ));
    let rss_task = tokio::spawn(sample_rss_until(rss_pid, metrics_interval, deadline));

    proxy_task.await.context("join proxy load")??;
    let subscribers = sse_task.await.context("join SSE subscribers")??;
    let scrape = metrics_task.await.context("join metrics scraper")??;
    let rss_samples = rss_task.await.context("join RSS sampler")?;

    let completed_at_unix_ms = unix_ms();
    let actual_duration_secs =
        (completed_at_unix_ms.saturating_sub(started_at_unix_ms) as f64 / 1000.0).max(0.001);
    let success = proxy_stats.success.load(Ordering::Relaxed);
    let failure = proxy_stats.failure.load(Ordering::Relaxed);
    let (rss_initial_mib, rss_final_mib, rss_growth_mib, rss_slope_mib_per_min) =
        summarize_rss(&rss_samples, duration_secs);
    let mut metric_series = scrape
        .series
        .iter()
        .map(|(name, samples)| (name.clone(), MetricSeriesSummary::from_samples(samples)))
        .collect::<BTreeMap<_, _>>();
    add_histogram_quantile_series(&mut metric_series, &scrape.last_values);

    let evidence = LiveTailSoakEvidence {
        schema_version: LIVE_TAIL_EVIDENCE_SCHEMA_VERSION,
        profile,
        rps,
        duration_secs,
        stream_ratio,
        max_in_flight,
        sse_subscribers_count: sse_subscribers,
        reconnect_churn_secs,
        started_at_unix_ms,
        completed_at_unix_ms,
        actual_duration_secs: round3(actual_duration_secs),
        actual_rps: round3((success + failure) as f64 / actual_duration_secs),
        proxy_success_count: success,
        proxy_failure_count: failure,
        streaming_request_count: proxy_stats.streaming.load(Ordering::Relaxed),
        non_streaming_request_count: proxy_stats.non_streaming.load(Ordering::Relaxed),
        sse_events_received_per_subscriber: subscribers.iter().map(|stats| stats.events).collect(),
        sse_messages_received_per_subscriber: subscribers
            .iter()
            .map(|stats| stats.messages)
            .collect(),
        sse_resets_received_per_subscriber: subscribers.iter().map(|stats| stats.resets).collect(),
        sse_reconnects_per_subscriber: subscribers.iter().map(|stats| stats.reconnects).collect(),
        sse_last_event_id_per_subscriber: subscribers
            .iter()
            .map(|stats| stats.last_event_id.clone())
            .collect(),
        sse_final_events_observed: subscribers.iter().map(|stats| stats.messages).sum(),
        storage_final_events_estimate: success,
        metric_series,
        metric_last_values: scrape.last_values,
        rss_initial_mib: round3(rss_initial_mib),
        rss_final_mib: round3(rss_final_mib),
        rss_growth_mib: round3(rss_growth_mib),
        rss_slope_mib_per_min: round3(rss_slope_mib_per_min),
        thresholds: profile.thresholds(),
    };

    write_json(&output_path, &evidence)?;
    if let Err(failures) = evaluate_live_tail_soak(&evidence, profile) {
        bail!(
            "live-tail soak assertions failed: {}",
            serde_json::to_string_pretty(&failures)?
        );
    }
    println!(
        "profile={} rps_target={} actual_rps={} success={} failure={} sse_messages={}",
        profile.as_str(),
        rps,
        evidence.actual_rps,
        success,
        failure,
        evidence.sse_final_events_observed
    );
    Ok(())
}

async fn run_proxy_load(config: ProxyLoadConfig, stats: Arc<ProxyStats>) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(config.max_in_flight));
    let mut set = JoinSet::new();
    let mut interval = tokio::time::interval(Duration::from_micros(
        1_000_000_u64.saturating_div(config.rps).max(1),
    ));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let deadline = Instant::now() + Duration::from_secs(config.duration_secs);
    let mut index = 0_u64;
    while Instant::now() < deadline {
        interval.tick().await;
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .context("acquire in-flight permit")?;
        let target = config.target.clone();
        let stats = stats.clone();
        let streaming = (index % 100) < u64::from(config.stream_ratio);
        let request = if streaming {
            stats.streaming.fetch_add(1, Ordering::Relaxed);
            config.stream_request.clone()
        } else {
            stats.non_streaming.fetch_add(1, Ordering::Relaxed);
            config.non_stream_request.clone()
        };
        set.spawn(async move {
            let result = single_request(&target, request, streaming).await;
            drop(permit);
            if result.is_ok() {
                stats.success.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.failure.fetch_add(1, Ordering::Relaxed);
            }
        });
        index = index.saturating_add(1);
        while let Some(result) = set.try_join_next() {
            result.context("join request task")?;
        }
    }
    while let Some(result) = set.join_next().await {
        result.context("join request task")?;
    }
    Ok(())
}

async fn run_subscribers(
    url: String,
    token: String,
    count: usize,
    churn_secs: u64,
    deadline: Instant,
) -> Result<Vec<SubscriberStats>> {
    let mut set = JoinSet::new();
    for _ in 0..count {
        let url = url.clone();
        let token = token.clone();
        set.spawn(async move { run_subscriber(url, token, churn_secs, deadline).await });
    }
    let mut subscribers = Vec::with_capacity(count);
    while let Some(result) = set.join_next().await {
        subscribers.push(result.context("join subscriber")??);
    }
    Ok(subscribers)
}

async fn run_subscriber(
    url: String,
    token: String,
    churn_secs: u64,
    deadline: Instant,
) -> Result<SubscriberStats> {
    let target = parse_http_url(&url)?;
    let mut stats = SubscriberStats::default();
    while Instant::now() < deadline {
        stats.reconnects = stats.reconnects.saturating_add(1);
        let churn_deadline = if churn_secs == 0 {
            deadline
        } else {
            deadline.min(Instant::now() + Duration::from_secs(churn_secs))
        };
        stream_once(&target, &token, &mut stats, churn_deadline).await?;
    }
    Ok(stats)
}

async fn stream_once(
    target: &super::HttpTarget,
    token: &str,
    stats: &mut SubscriberStats,
    deadline: Instant,
) -> Result<()> {
    let last_id = stats
        .last_event_id
        .as_ref()
        .map(|id| format!("Last-Event-ID: {id}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nAccept: text/event-stream\r\n{}Connection: close\r\n\r\n",
        target.path, target.host_header, token, last_id
    );
    let mut stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut buffer = Vec::with_capacity(8192);
    let mut bytes = [0_u8; 2048];
    loop {
        if Instant::now() >= deadline {
            return Ok(());
        }
        let remaining = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(5));
        let read = match tokio::time::timeout(remaining, stream.read(&mut bytes)).await {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(read)) => read,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Ok(()),
        };
        buffer.extend_from_slice(&bytes[..read]);
        consume_sse_frames(&mut buffer, stats);
    }
}

fn consume_sse_frames(buffer: &mut Vec<u8>, stats: &mut SubscriberStats) {
    while let Some(index) = buffer.windows(2).position(|window| window == b"\n\n") {
        let frame = String::from_utf8_lossy(&buffer[..index]).to_string();
        buffer.drain(..index + 2);
        if frame.starts_with("HTTP/") || frame.starts_with(':') {
            continue;
        }
        let mut event_name = None;
        for line in frame.lines().map(str::trim_end) {
            if let Some(rest) = line.strip_prefix("event:") {
                event_name = Some(rest.trim().to_owned());
            } else if let Some(rest) = line.strip_prefix("id:") {
                stats.last_event_id = Some(rest.trim().to_owned());
            }
        }
        if let Some(name) = event_name {
            stats.events = stats.events.saturating_add(1);
            if name == "message" {
                stats.messages = stats.messages.saturating_add(1);
            } else if name == "reset" {
                stats.resets = stats.resets.saturating_add(1);
            }
        }
    }
}

async fn scrape_metrics_until(
    url: String,
    interval_secs: u64,
    deadline: Instant,
    output: impl AsRef<Path>,
) -> Result<ScrapeResult> {
    if let Some(parent) = output.as_ref().parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(output.as_ref())?;
    let target = parse_http_url(&url)?;
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut series = BTreeMap::<String, Vec<f64>>::new();
    let mut last_values = BTreeMap::new();
    while Instant::now() < deadline {
        interval.tick().await;
        let text = http_get_text(&target).await?;
        let samples = parse_metrics(&text);
        for sample in samples.into_iter().filter(track_sample) {
            let key = metric_key(&sample);
            series
                .entry(sample.name.clone())
                .or_default()
                .push(sample.value);
            last_values.insert(key, sample.value);
        }
        writeln!(
            file,
            "{}",
            json!({ "ts_unix_ms": unix_ms(), "metrics": last_values })
        )?;
    }
    Ok(ScrapeResult {
        series,
        last_values,
    })
}

async fn http_get_text(target: &super::HttpTarget) -> Result<String> {
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        target.path, target.host_header
    );
    let mut stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).await?;
    let status = super::status_code(&bytes)?;
    if status != 200 {
        bail!("metrics scrape returned HTTP {status}");
    }
    let body_start = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .unwrap_or(0);
    Ok(String::from_utf8_lossy(&bytes[body_start..]).to_string())
}

fn parse_metrics(text: &str) -> Vec<MetricSample> {
    text.lines().filter_map(parse_metric_line).collect()
}

fn parse_metric_line(line: &str) -> Option<MetricSample> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (head, value_text) = line.rsplit_once(char::is_whitespace)?;
    let value = match value_text {
        "+Inf" | "Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        other => other.parse::<f64>().ok()?,
    };
    let (name, labels) = if let Some(open) = head.find('{') {
        let close = head.rfind('}')?;
        (&head[..open], parse_labels(&head[open + 1..close]))
    } else {
        (head, BTreeMap::new())
    };
    Some(MetricSample {
        name: name.to_owned(),
        labels,
        value,
    })
}

fn parse_labels(text: &str) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    for part in text.split(',') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        labels.insert(key.to_owned(), value.trim_matches('"').to_owned());
    }
    labels
}

fn track_sample(sample: &MetricSample) -> bool {
    sample.name.starts_with("sse_")
        || sample.name == "cc_lb_dropped_events_total"
        || sample.name == "cc_lb_lifecycle_assembler_rows_total"
        || sample.name == "cc_lb_lifecycle_assembler_in_flight"
}

fn metric_key(sample: &MetricSample) -> String {
    if sample.labels.is_empty() {
        return sample.name.clone();
    }
    let labels = sample
        .labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}{{{labels}}}", sample.name)
}

fn add_histogram_quantile_series(
    series: &mut BTreeMap<String, MetricSeriesSummary>,
    last_values: &BTreeMap<String, f64>,
) {
    let mut buckets = HashMap::<String, f64>::new();
    for (key, value) in last_values {
        if let Some(le) = key
            .strip_prefix("sse_storage_tail_lag_ms_bucket{le=\"")
            .and_then(|rest| rest.strip_suffix("\"}"))
        {
            buckets.insert(le.to_owned(), *value);
        }
    }
    let p95 = histogram_quantile(&buckets, 0.95);
    if p95.is_finite() {
        series.insert(
            "sse_storage_tail_lag_ms".to_owned(),
            MetricSeriesSummary::from_samples(&[p95]),
        );
    }
}

fn histogram_quantile(buckets: &HashMap<String, f64>, quantile: f64) -> f64 {
    let mut parsed = buckets
        .iter()
        .filter_map(|(le, count)| {
            let upper = if le == "+Inf" {
                f64::INFINITY
            } else {
                le.parse::<f64>().ok()?
            };
            Some((upper, *count))
        })
        .collect::<Vec<_>>();
    parsed.sort_by(|left, right| left.0.total_cmp(&right.0));
    let count = parsed.last().map(|(_, count)| *count).unwrap_or(0.0);
    if count <= 0.0 {
        return 0.0;
    }
    let target = count * quantile;
    let mut lower_bound = 0.0;
    let mut lower_count = 0.0;
    for (upper_bound, upper_count) in parsed {
        if upper_count >= target {
            if upper_bound.is_infinite() {
                return lower_bound;
            }
            let span = upper_count - lower_count;
            if span <= 0.0 {
                return lower_bound;
            }
            let fraction = (target - lower_count) / span;
            return lower_bound + (upper_bound - lower_bound) * fraction;
        }
        lower_bound = upper_bound;
        lower_count = upper_count;
    }
    lower_bound
}

fn read_rss_mib(pid: u32) -> Option<f64> {
    let text = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    text.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        let kb = rest.split_whitespace().next()?.parse::<f64>().ok()?;
        Some(kb / 1024.0)
    })
}

async fn sample_rss_until(
    pid: Option<u32>,
    interval_secs: u64,
    deadline: Instant,
) -> Vec<RssSample> {
    let Some(pid) = pid else {
        return Vec::new();
    };
    let start = Instant::now();
    let mut samples = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    while Instant::now() < deadline {
        interval.tick().await;
        if let Some(mib) = read_rss_mib(pid) {
            samples.push(RssSample {
                elapsed_secs: start.elapsed().as_secs_f64(),
                mib,
            });
        }
    }
    samples
}

fn summarize_rss(samples: &[RssSample], duration_secs: u64) -> (f64, f64, f64, f64) {
    let initial = samples.first().map(|sample| sample.mib).unwrap_or(0.0);
    let final_mib = samples.last().map(|sample| sample.mib).unwrap_or(initial);
    let growth = (final_mib - initial).max(0.0);
    let warmup_secs = (duration_secs / 3).clamp(10, 60) as f64;
    let slope_base = samples
        .iter()
        .find(|sample| sample.elapsed_secs >= warmup_secs)
        .or_else(|| samples.first());
    let slope = match (slope_base, samples.last()) {
        (Some(first), Some(last)) if last.elapsed_secs > first.elapsed_secs => {
            (last.mib - first.mib).max(0.0) / ((last.elapsed_secs - first.elapsed_secs) / 60.0)
        }
        _ => 0.0,
    };
    (initial, final_mib, growth, slope)
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
}
