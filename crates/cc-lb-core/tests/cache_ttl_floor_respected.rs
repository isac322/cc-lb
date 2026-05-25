use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_lb_core::{DnsResolverConfig, make_resolver_with_factory};
use hickory_resolver::config::ResolverOpts;

#[test]
fn cache_ttl_floor_respected() {
    let captured = Arc::new(Mutex::new(None::<ResolverOpts>));
    let observed = Arc::clone(&captured);

    make_resolver_with_factory(
        &DnsResolverConfig {
            cache_ttl_floor: Duration::from_secs(30),
            cache_ttl_ceiling: Duration::from_secs(300),
            max_concurrent: 64,
        },
        move |_config, opts, _provider| {
            *observed.lock().expect("capture opts") = Some(opts);
        },
    );

    let opts = captured
        .lock()
        .expect("capture opts")
        .clone()
        .expect("opts");
    assert_eq!(opts.positive_min_ttl, Some(Duration::from_secs(30)));
}
