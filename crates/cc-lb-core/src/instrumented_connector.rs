use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http::Uri;
use tower::Service;

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
        crate::request_timing::mark_connector_called();
        let dns_before = read_dns_ms_snapshot();
        let start = Instant::now();
        let inner_fut = self.inner.call(uri);

        Box::pin(async move {
            let result = inner_fut.await;
            if result.is_ok() {
                let total_elapsed = start.elapsed();
                let dns_after = read_dns_ms_snapshot();
                let dns_delta_ms = dns_after.saturating_sub(dns_before);
                let total_ms = duration_to_ms(total_elapsed);
                let connect_ms = total_ms.saturating_sub(dns_delta_ms);

                crate::request_timing::record_connect(Duration::from_millis(connect_ms));
            }

            result
        })
    }
}

fn duration_to_ms(elapsed: Duration) -> u64 {
    elapsed.as_millis().min(u128::from(u64::MAX)) as u64
}

fn read_dns_ms_snapshot() -> u64 {
    crate::request_timing::REQUEST_STAGE_TIMINGS
        .try_with(|ctx| ctx.lock().unwrap().dns_ms.unwrap_or(0))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

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
                    record_dns(Duration::from_millis(30));
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(())
                })
            }
        }

        let mut connector = InstrumentedHttpsConnector::new(DnsSimulator);

        let (_, timings) = with_timings(async {
            let _ = connector.call("https://example.com".parse().unwrap()).await;
        })
        .await;

        let connect_ms = timings.connect_ms.expect("connect_ms recorded");
        assert!(
            (40..=100).contains(&connect_ms),
            "expected ~50ms connect_ms, got {connect_ms}"
        );
    }

    #[tokio::test]
    async fn connector_does_not_record_on_inner_error() {
        use crate::request_timing::with_timings;

        let stub = StubConnector {
            delay: Duration::from_millis(10),
            succeed: false,
        };
        let mut connector = InstrumentedHttpsConnector::new(stub);

        let (_, timings) = with_timings(async {
            let _ = connector.call("https://example.com".parse().unwrap()).await;
        })
        .await;

        assert_eq!(timings.connect_ms, None);
        assert_eq!(timings.connection_reused, Some(false));
    }
}
