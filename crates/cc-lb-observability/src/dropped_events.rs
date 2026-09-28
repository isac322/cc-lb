pub fn increment_dropped_events_by(reason: &str, amount: u64) {
    metrics::counter!("cc_lb_dropped_events_total", "reason" => reason.to_owned())
        .increment(amount);
}
