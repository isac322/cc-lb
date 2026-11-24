use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http::Uri;
use tower::Service;

use crate::request_timing::{self, DnsResolutionOutcome};

#[derive(Clone)]
pub struct InstrumentedHttpsConnector<S> {
    inner: S,
}

impl<S> InstrumentedHttpsConnector<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<Uri> for InstrumentedHttpsConnector<S>
where
    S: Service<Uri> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        request_timing::mark_connector_called();
        let dns_before = request_timing::dns_snapshot();
        let host_is_literal_ip = uri
            .host()
            .map(|host| {
                host.trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<IpAddr>()
                    .is_ok()
            })
            .unwrap_or(false);
        let start = Instant::now();
        let inner_fut = self.inner.call(uri);

        Box::pin(async move {
            let result = inner_fut.await;
            let total_elapsed = start.elapsed();
            let dns_after = request_timing::dns_snapshot();
            // DNS ran during this call iff the attempt counter advanced; the
            // recorded dns_ms is then exactly this call's resolution time.
            let dns_ran_this_call = dns_after.attempts > dns_before.attempts;
            let dns_elapsed = if dns_ran_this_call {
                Duration::from_millis(dns_after.ms.unwrap_or(0))
            } else {
                Duration::ZERO
            };

            let record = match (&result, dns_ran_this_call, dns_after.outcome) {
                // DNS failed inside this call: TCP was never attempted, so the
                // connect stage did not run and stays unset.
                (Err(_), true, Some(DnsResolutionOutcome::Failed)) => None,
                // DNS resolved (or the call succeeded): the remainder of the
                // elapsed time is the TCP+TLS connect stage.
                (Ok(_), _, _) | (Err(_), true, _) => {
                    Some(total_elapsed.saturating_sub(dns_elapsed))
                }
                // No DNS ran: the connector only reaches TCP when the URI host
                // is a literal IP (hyper skips resolution for IP literals).
                // Other pre-connect rejections (bad scheme, missing host, SNI
                // resolution) never attempted a connection.
                (Err(_), false, _) => host_is_literal_ip.then_some(total_elapsed),
            };
            if let Some(connect_elapsed) = record {
                request_timing::record_connect(connect_elapsed);
            }

            result
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    use http::Uri;
    use tower::Service;

    use super::*;

    #[derive(Clone)]
    struct StubConnector {
        delay: Duration,
        succeed: bool,
    }

    impl Service<Uri> for StubConnector {
        type Response = ();
        type Error = std::io::Error;
        type Future = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            let delay = self.delay;
            let succeed = self.succeed;

            Box::pin(async move {
                tokio::time::sleep(delay).await;
                if succeed {
                    Ok(())
                } else {
                    Err(std::io::Error::other("boom"))
                }
            })
        }
    }

    #[tokio::test]
    async fn connector_call_marks_reused_false() {
        use crate::request_timing::with_timings;

        let stub = StubConnector {
            delay: Duration::from_millis(0),
            succeed: true,
        };
        let mut connector = InstrumentedHttpsConnector::new(stub);

        let (_, timings) = with_timings(async {
            let _ = connector.call("https://example.com".parse().unwrap()).await;
        })
        .await;

        assert_eq!(timings.connection_reused, Some(false));
    }

    #[tokio::test]
    async fn connector_not_called_keeps_reused_unset() {
        use crate::request_timing::with_timings;

        let (_, timings) = with_timings(async { 42 }).await;

        assert_eq!(timings.connection_reused, None);
    }

    #[tokio::test]
    async fn connector_records_connect_ms_when_call_succeeds() {
        use crate::request_timing::with_timings;

        let stub = StubConnector {
            delay: Duration::from_millis(50),
            succeed: true,
        };
        let mut connector = InstrumentedHttpsConnector::new(stub);

        let (_, timings) = with_timings(async {
            let _ = connector.call("https://example.com".parse().unwrap()).await;
        })
        .await;

        let connect_ms = timings.connect_ms.expect("connect_ms recorded");
        assert!(
            (45..=200).contains(&connect_ms),
            "expected ~50ms connect_ms, got {connect_ms}"
        );
    }

    #[tokio::test]
    async fn connector_subtracts_dns_when_dns_recorded_during_call() {
        use crate::request_timing::{record_dns, with_timings};

        #[derive(Clone)]
        struct DnsSimulator;

        impl Service<Uri> for DnsSimulator {
            type Response = ();
            type Error = std::io::Error;
            type Future = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _uri: Uri) -> Self::Future {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    record_dns(Duration::from_millis(30), DnsResolutionOutcome::Resolved);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(())
                })
            }
        }

        let mut connector = InstrumentedHttpsConnector::new(DnsSimulator);

        let (total_elapsed, timings) = with_timings(async {
            let start = Instant::now();
            let _ = connector.call("https://example.com".parse().unwrap()).await;
            start.elapsed()
        })
        .await;

        assert_eq!(timings.dns_ms, Some(30));
        let connect_ms = timings.connect_ms.expect("connect_ms recorded");
        let total_ms = total_elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        assert!(
            connect_ms < total_ms,
            "expected DNS subtraction from total {total_ms}ms, got {connect_ms}ms"
        );
        assert!(
            connect_ms.saturating_add(30) <= total_ms,
            "expected connect_ms {connect_ms}ms plus DNS 30ms to fit within total {total_ms}ms"
        );
    }

    #[tokio::test]
    async fn connector_records_connect_ms_on_failure_for_literal_ip() {
        use crate::request_timing::with_timings;

        let stub = StubConnector {
            delay: Duration::from_millis(10),
            succeed: false,
        };
        let mut connector = InstrumentedHttpsConnector::new(stub);

        let (_, timings) = with_timings(async {
            // Literal IP: no DNS stage, but TCP connect is attempted.
            let _ = connector.call("https://127.0.0.1/".parse().unwrap()).await;
        })
        .await;

        let connect_ms = timings.connect_ms.expect("connect_ms recorded on failure");
        assert!(
            (5..=200).contains(&connect_ms),
            "expected ~10ms connect_ms, got {connect_ms}"
        );
        assert_eq!(timings.dns_ms, None);
        assert_eq!(timings.connection_reused, Some(false));
    }

    #[tokio::test]
    async fn connector_failure_without_dns_and_non_ip_host_records_no_connect() {
        use crate::request_timing::with_timings;

        let stub = StubConnector {
            delay: Duration::from_millis(10),
            succeed: false,
        };
        let mut connector = InstrumentedHttpsConnector::new(stub);

        let (_, timings) = with_timings(async {
            // Non-literal host with no DNS attempt recorded: the connector
            // failed before TCP was attempted, so connect_ms stays unset.
            let _ = connector.call("https://example.com".parse().unwrap()).await;
        })
        .await;

        assert_eq!(timings.connect_ms, None);
        assert_eq!(timings.connection_reused, Some(false));
    }

    #[tokio::test]
    async fn connector_dns_failure_records_no_connect_ms() {
        use crate::request_timing::{record_dns, with_timings};

        #[derive(Clone)]
        struct FailingDnsSimulator;

        impl Service<Uri> for FailingDnsSimulator {
            type Response = ();
            type Error = std::io::Error;
            type Future = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _uri: Uri) -> Self::Future {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    record_dns(Duration::from_millis(30), DnsResolutionOutcome::Failed);
                    Err(std::io::Error::other("dns failed"))
                })
            }
        }

        let mut connector = InstrumentedHttpsConnector::new(FailingDnsSimulator);

        let (_, timings) = with_timings(async {
            let _ = connector.call("https://example.com".parse().unwrap()).await;
        })
        .await;

        assert_eq!(timings.dns_ms, Some(30));
        assert_eq!(timings.connect_ms, None);
        assert_eq!(timings.connection_reused, Some(false));
    }

    #[tokio::test]
    async fn connector_failure_subtracts_resolved_dns_elapsed() {
        use crate::request_timing::{record_dns, with_timings};

        #[derive(Clone)]
        struct DnsThenFailSimulator;

        impl Service<Uri> for DnsThenFailSimulator {
            type Response = ();
            type Error = std::io::Error;
            type Future = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _uri: Uri) -> Self::Future {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    record_dns(Duration::from_millis(30), DnsResolutionOutcome::Resolved);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "refused",
                    ))
                })
            }
        }

        let mut connector = InstrumentedHttpsConnector::new(DnsThenFailSimulator);

        let (total_elapsed, timings) = with_timings(async {
            let start = Instant::now();
            let _ = connector.call("https://example.com".parse().unwrap()).await;
            start.elapsed()
        })
        .await;

        assert_eq!(timings.dns_ms, Some(30));
        let connect_ms = timings.connect_ms.expect("connect_ms recorded on failure");
        let total_ms = total_elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        assert!(
            connect_ms < total_ms,
            "expected DNS subtraction from total {total_ms}ms, got {connect_ms}ms"
        );
        assert!(
            connect_ms.saturating_add(30) <= total_ms,
            "expected connect_ms {connect_ms}ms plus DNS 30ms to fit within total {total_ms}ms"
        );
    }
}
