use std::time::Duration;

use http::HeaderMap;

pub const DEFAULT_DELTA_COUNT: u64 = 50;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeatherConfig {
    pub seed: u64,
    pub ttft_ms: u64,
    pub inter_token_ms: u64,
    pub jitter_ms: u64,
    pub long_tail_every: u64,
    pub long_tail_ms: u64,
    pub delta_count: u64,
    pub timeout_ms: u64,
    pub retry_after_secs: u64,
}

impl Default for WeatherConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            ttft_ms: 0,
            inter_token_ms: 0,
            jitter_ms: 0,
            long_tail_every: 0,
            long_tail_ms: 0,
            delta_count: DEFAULT_DELTA_COUNT,
            timeout_ms: 0,
            retry_after_secs: 1,
        }
    }
}

impl WeatherConfig {
    pub(crate) fn for_headers(&self, headers: &HeaderMap) -> RequestWeather {
        RequestWeather {
            seed: header_u64(headers, "x-fake-weather-seed", self.seed),
            ttft_ms: header_u64(headers, "x-fake-ttft-ms", self.ttft_ms),
            inter_token_ms: header_u64(headers, "x-fake-inter-token-ms", self.inter_token_ms),
            jitter_ms: header_u64(headers, "x-fake-jitter-ms", self.jitter_ms),
            long_tail_every: header_u64(headers, "x-fake-long-tail-every", self.long_tail_every),
            long_tail_ms: header_u64(headers, "x-fake-long-tail-ms", self.long_tail_ms),
            delta_count: header_u64(headers, "x-fake-delta-count", self.delta_count),
            timeout_ms: header_u64(headers, "x-fake-timeout-ms", self.timeout_ms),
            retry_after_secs: header_u64(headers, "x-fake-retry-after", self.retry_after_secs),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestWeather {
    seed: u64,
    ttft_ms: u64,
    inter_token_ms: u64,
    jitter_ms: u64,
    long_tail_every: u64,
    long_tail_ms: u64,
    delta_count: u64,
    timeout_ms: u64,
    retry_after_secs: u64,
}

impl RequestWeather {
    pub(crate) const fn delta_count(&self) -> u64 {
        self.delta_count
    }

    pub(crate) const fn retry_after(&self) -> u64 {
        self.retry_after_secs
    }

    pub(crate) fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    pub(crate) fn delay_for_event(
        &self,
        event_index: u64,
        event_name: &str,
        delta_index: u64,
    ) -> Duration {
        let ttft = if event_index == 0 { self.ttft_ms } else { 0 };
        let is_delta = event_name == "content_block_delta";
        let base = if is_delta { self.inter_token_ms } else { 0 };
        let jitter = if is_delta {
            jitter_ms(self.seed, delta_index, self.jitter_ms)
        } else {
            0
        };
        let long_tail = if is_delta
            && self.long_tail_every != 0
            && (delta_index + 1).is_multiple_of(self.long_tail_every)
        {
            self.long_tail_ms
        } else {
            0
        };
        Duration::from_millis(
            ttft.saturating_add(base)
                .saturating_add(jitter)
                .saturating_add(long_tail),
        )
    }
}

fn header_u64(headers: &HeaderMap, name: &str, fallback: u64) -> u64 {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(fallback)
}

const fn jitter_ms(seed: u64, delta_index: u64, ceiling: u64) -> u64 {
    if ceiling == 0 {
        return 0;
    }
    splitmix64(seed ^ delta_index.wrapping_mul(0x9e37_79b9_7f4a_7c15)) % ceiling.saturating_add(1)
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::WeatherConfig;

    #[test]
    fn seeded_jitter_schedule_is_deterministic() {
        let mut headers = HeaderMap::new();
        headers.insert("x-fake-weather-seed", HeaderValue::from_static("42"));
        headers.insert("x-fake-jitter-ms", HeaderValue::from_static("10"));
        headers.insert("x-fake-long-tail-every", HeaderValue::from_static("2"));
        headers.insert("x-fake-long-tail-ms", HeaderValue::from_static("50"));
        let weather = WeatherConfig::default().for_headers(&headers);

        let first = weather.delay_for_event(2, "content_block_delta", 1);
        let second = weather.delay_for_event(2, "content_block_delta", 1);

        assert_eq!(first, second);
        assert!(first.as_millis() >= 50);
    }
}
