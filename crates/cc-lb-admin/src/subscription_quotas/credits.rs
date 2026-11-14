//! Credit charts project recorded state, not historical polling freshness.
//! Only the current tail uses the balance field's own latest observation clock.

use super::*;
use cc_lb_domain::{CreditObservation, CreditSource, is_credit_balance};
use cc_lb_storage_api::{SubscriptionQuotaCheckpointRangeQuery, SubscriptionQuotaSlimCheckpoint};

#[derive(Debug, Deserialize)]
pub(super) struct CreditHistoryQuery {
    upstream_ids: Option<String>,
    since_unix_secs: Option<u64>,
    until_unix_secs: Option<u64>,
    relative_range_secs: Option<u64>,
    max_points_per_series: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct CreditPoint {
    pub at_unix_millis: u64,
    pub balance: Option<String>,
    pub observed_at_unix_millis: Option<u64>,
    pub contributing_upstreams: usize,
    pub eligible_upstreams: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct CreditLatest {
    pub balance: Option<String>,
    pub observed_at_unix_millis: Option<u64>,
    pub state: &'static str,
}

#[derive(Debug, Serialize)]
pub(super) struct CreditUpstreamHistory {
    pub upstream_id: Uuid,
    pub latest: CreditLatest,
    pub series: Vec<CreditPoint>,
}

#[derive(Debug, Serialize)]
pub(super) struct CreditPoolHistory {
    pub latest: CreditPoint,
    pub series: Vec<CreditPoint>,
}

#[derive(Debug, Serialize)]
pub(super) struct CreditHistoryResponse {
    pub now_unix_secs: u64,
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub max_staleness_secs: u64,
    pub upstreams: Vec<CreditUpstreamHistory>,
    pub pool: CreditPoolHistory,
}

pub(super) async fn handle_history(
    State(state): State<AdminState>,
    Query(query): Query<CreditHistoryQuery>,
) -> Response {
    let result = async {
        let now = now_unix_millis(&*state.clock);
        let until = query.until_unix_secs.unwrap_or(now / 1_000);
        if query.relative_range_secs.is_some() && query.since_unix_secs.is_some() {
            return Err(bad_request(
                "ambiguous_history_range",
                "relative and absolute credit ranges cannot be combined",
            ));
        }
        let range = query.relative_range_secs.unwrap_or(7 * 86_400);
        if range == 0 {
            return Err(bad_request(
                "invalid_relative_range",
                "credit history range must be positive",
            ));
        }
        let since = query
            .since_unix_secs
            .unwrap_or_else(|| until.saturating_sub(range));
        if since > until || until > u64::MAX / 1_000 {
            return Err(bad_request(
                "invalid_history_range",
                "invalid credit history bounds",
            ));
        }
        let max_points_per_series = query
            .max_points_per_series
            .unwrap_or(DEFAULT_SERIES_MAX_POINTS);
        if !(2..=MAX_SERIES_MAX_POINTS).contains(&max_points_per_series) {
            return Err(bad_request(
                "invalid_max_points_per_series",
                "max_points_per_series must be between 2 and 10000",
            ));
        }
        let upstreams =
            upstreams_for_optional_query(storage(&state)?, query.upstream_ids.as_deref())
                .await?
                .into_iter()
                .filter(|upstream| upstream.kind == UpstreamKind::OpenaiOauth)
                .collect();
        build_history(
            &state,
            upstreams,
            since,
            until,
            now,
            max_points_per_series as usize,
        )
        .await
    }
    .await;
    match result {
        Ok(response) => Json(response).into_response(),
        Err(response) => response,
    }
}

pub(super) async fn build_history(
    state: &AdminState,
    upstreams: Vec<UpstreamRecord>,
    since: u64,
    until: u64,
    now: u64,
    max_points_per_series: usize,
) -> Result<CreditHistoryResponse, Response> {
    let storage = storage(state)?;
    let ids = upstreams
        .iter()
        .map(|upstream| upstream.id)
        .collect::<Vec<_>>();
    let end = if until >= now / 1_000 {
        now
    } else {
        until.saturating_mul(1_000)
    };
    let checkpoints = storage
        .list_subscription_quota_slim_checkpoints(SubscriptionQuotaCheckpointRangeQuery {
            upstream_ids: ids.clone(),
            windows: vec![
                SubscriptionQuotaWindow::Unified,
                SubscriptionQuotaWindow::Overage,
            ],
            sources: vec![
                SubscriptionQuotaSource::Header,
                SubscriptionQuotaSource::Api,
            ],
            since_unix_millis: since.saturating_mul(1_000),
            until_unix_millis: end,
        })
        .await
        .map_err(storage_error)?;
    let latest = storage
        .list_latest_subscription_quota_for_upstreams(&ids)
        .await
        .map_err(storage_error)?;
    let mut checkpoints_by_upstream = HashMap::<Uuid, Vec<SubscriptionQuotaSlimCheckpoint>>::new();
    for checkpoint in checkpoints {
        checkpoints_by_upstream
            .entry(checkpoint.upstream_id)
            .or_default()
            .push(checkpoint);
    }
    let mut latest_by_upstream = HashMap::<Uuid, Vec<SubscriptionQuotaSample>>::new();
    for sample in latest {
        if matches!(
            sample.window,
            SubscriptionQuotaWindow::Unified | SubscriptionQuotaWindow::Overage
        ) {
            latest_by_upstream
                .entry(sample.upstream_id)
                .or_default()
                .push(sample);
        }
    }
    let view = state.dynamic_view.load();
    let max_staleness = view.subscription_quota_routing_max_staleness_secs;
    let mut histories = Vec::with_capacity(upstreams.len());
    let mut pool_indices = Vec::new();
    for upstream in upstreams {
        let cached =
            view.subscription_quota_cache
                .snapshot_for_upstream(upstream.id, now, max_staleness);
        let durable = latest_by_upstream.remove(&upstream.id).unwrap_or_default();
        let current = current_credit_state(&durable, &cached);
        let latest = latest_balance(current.as_ref(), now, max_staleness);
        let series = bound_points(
            account_history(
                checkpoints_by_upstream
                    .remove(&upstream.id)
                    .unwrap_or_default(),
                &latest,
                since.saturating_mul(1_000),
                end,
                now,
            ),
            max_points_per_series,
        );
        // Match shipped OpenAI Pool quota membership. Disabled accounts can be
        // inspected in lists but never contribute to the current pool lens.
        if upstream.enabled {
            pool_indices.push(histories.len());
        }
        histories.push(CreditUpstreamHistory {
            upstream_id: upstream.id,
            latest,
            series,
        });
    }
    let mut pool = pool_history(
        &histories,
        &pool_indices,
        since.saturating_mul(1_000),
        end,
        now,
    );
    pool.series = bound_points(pool.series, max_points_per_series);
    Ok(CreditHistoryResponse {
        now_unix_secs: now / 1_000,
        since_unix_secs: since,
        until_unix_secs: until,
        max_staleness_secs: max_staleness,
        upstreams: histories,
        pool,
    })
}

fn source(source: SubscriptionQuotaSource) -> CreditSource {
    match source {
        SubscriptionQuotaSource::Header => CreditSource::Header,
        SubscriptionQuotaSource::Api => CreditSource::Api,
    }
}

fn merge_credit(
    state: &mut Option<SubscriptionQuotaCredits>,
    mut incoming: SubscriptionQuotaCredits,
    at: u64,
    source: CreditSource,
) {
    incoming.stamp_observations(at, source);
    if let Some(current) = state.as_mut() {
        current.merge_observations(&incoming);
    } else {
        *state = Some(incoming);
    }
}

fn current_credit_state(
    durable: &[SubscriptionQuotaSample],
    cached: &[SubscriptionQuotaCandidateSnapshot],
) -> Option<SubscriptionQuotaCredits> {
    let mut state = None;
    for sample in durable {
        let superseded_by_absence = cached.iter().any(|snapshot| {
            snapshot.window == sample.window.as_str()
                && snapshot.source.as_deref() == Some(sample.source.as_str())
                && snapshot.state == SubscriptionQuotaDataState::Absent
                && snapshot
                    .observed_at_unix_millis
                    .is_some_and(|at| at >= sample.observed_at_unix_millis)
        });
        if !superseded_by_absence
            && sample.sample_kind == SubscriptionQuotaSampleKind::Sample
            && let Some(credits) = sample.credits.as_ref()
        {
            merge_credit(
                &mut state,
                credits.clone(),
                sample.observed_at_unix_millis,
                source(sample.source),
            );
        }
    }
    for snapshot in cached {
        if !matches!(snapshot.window.as_str(), "unified" | "overage")
            || matches!(
                snapshot.state,
                SubscriptionQuotaDataState::Absent | SubscriptionQuotaDataState::Unobserved
            )
        {
            continue;
        }
        let Some(mut credits) = snapshot.credits.clone() else {
            continue;
        };
        let at = snapshot.observed_at_unix_millis.unwrap_or(0);
        let fallback_source = if snapshot.source.as_deref() == Some("api") {
            CreditSource::Api
        } else {
            CreditSource::Header
        };
        credits.stamp_observations(at, fallback_source);
        let tombstoned = |observation: Option<CreditObservation>| {
            observation.is_some_and(|observation| {
                durable.iter().any(|sample| {
                    sample.window.as_str() == snapshot.window
                        && source(sample.source) == observation.source
                        && sample.sample_kind == SubscriptionQuotaSampleKind::Absent
                        && sample.observed_at_unix_millis >= observation.observed_at_unix_millis
                })
            })
        };
        if tombstoned(credits.observations.has_credits) {
            credits.has_credits = None;
            credits.observations.has_credits = None;
        }
        if tombstoned(credits.observations.unlimited) {
            credits.unlimited = None;
            credits.observations.unlimited = None;
        }
        if tombstoned(credits.observations.limit_reached) {
            credits.limit_reached = None;
            credits.observations.limit_reached = None;
        }
        if tombstoned(credits.observations.balance) {
            credits.balance = None;
            credits.observations.balance = None;
        }
        merge_credit(&mut state, credits, at, fallback_source);
    }
    state
}

fn latest_balance(
    state: Option<&SubscriptionQuotaCredits>,
    now: u64,
    max_staleness: u64,
) -> CreditLatest {
    let balance = state
        .and_then(|credits| credits.balance.as_deref())
        .filter(|value| is_credit_balance(value))
        .map(str::to_owned);
    let observation = state.and_then(|credits| credits.observations.balance);
    let state = if state.is_some_and(|credits| credits.unlimited == Some(true)) {
        "unlimited"
    } else if balance.is_none() || observation.is_none() {
        "unknown"
    } else if observation.is_some_and(|observation| observation.is_fresh(now, max_staleness)) {
        "fresh"
    } else {
        "stale"
    };
    CreditLatest {
        balance,
        observed_at_unix_millis: observation.map(|observation| observation.observed_at_unix_millis),
        state,
    }
}

fn recorded_point(at: u64, state: Option<&SubscriptionQuotaCredits>) -> CreditPoint {
    let finite = state.filter(|credits| credits.unlimited != Some(true));
    let balance = finite
        .and_then(|credits| credits.balance.as_deref())
        .filter(|value| is_credit_balance(value))
        .map(str::to_owned);
    CreditPoint {
        at_unix_millis: at,
        contributing_upstreams: usize::from(balance.is_some()),
        eligible_upstreams: 1,
        observed_at_unix_millis: finite
            .and_then(|credits| credits.observations.balance)
            .map(|o| o.observed_at_unix_millis),
        balance,
    }
}

fn account_history(
    mut checkpoints: Vec<SubscriptionQuotaSlimCheckpoint>,
    latest: &CreditLatest,
    since: u64,
    until: u64,
    now: u64,
) -> Vec<CreditPoint> {
    checkpoints.sort_by_key(|row| (row.changed_at_unix_millis, row.sample_id, row.source));
    let mut states = BTreeMap::<
        (SubscriptionQuotaWindow, SubscriptionQuotaSource),
        SubscriptionQuotaCredits,
    >::new();
    let mut points = Vec::new();
    let mut cursor = 0;
    while cursor < checkpoints.len() {
        let at = checkpoints[cursor].changed_at_unix_millis;
        while cursor < checkpoints.len() && checkpoints[cursor].changed_at_unix_millis == at {
            let row = &mut checkpoints[cursor];
            let key = (row.window, row.source);
            if row.sample_kind == SubscriptionQuotaSampleKind::Absent {
                states.remove(&key);
            } else if let Some(mut credits) = row.credits.take() {
                credits.stamp_observations(at, source(row.source));
                if let Some(current) = states.get_mut(&key) {
                    current.merge_observations(&credits);
                } else {
                    states.insert(key, credits);
                }
            }
            cursor += 1;
        }
        let mut combined = None;
        for credits in states.values() {
            if let Some(current) = combined.as_mut() {
                SubscriptionQuotaCredits::merge_observations(current, credits);
            } else {
                combined = Some(credits.clone());
            }
        }
        let point = recorded_point(at, combined.as_ref());
        // Preserve a source's left anchor as recorded state. Multiple physical
        // sources at the same time are reconciled before producing one point.
        if at < since {
            points.clear();
        }
        push_state_point(&mut points, point);
    }
    if latest.state == "unlimited" {
        if until >= now {
            if points.last().is_some_and(|point| point.balance.is_some()) {
                points.push(CreditPoint {
                    at_unix_millis: now,
                    balance: None,
                    observed_at_unix_millis: None,
                    contributing_upstreams: 0,
                    eligible_upstreams: 1,
                });
            }
        } else if let Some(last) = points.last().cloned() {
            push_state_point(
                &mut points,
                CreditPoint {
                    at_unix_millis: until,
                    ..last
                },
            );
        }
        return points;
    }
    if until < now {
        // A fully historical range holds its recorded state to its bound.
        // Today's sidecar must never rewrite a past balance or polling age.
        if let Some(last) = points.last().cloned() {
            push_state_point(
                &mut points,
                CreditPoint {
                    at_unix_millis: until,
                    ..last
                },
            );
        }
        return points;
    }
    if let Some(observed_at) = latest.observed_at_unix_millis {
        let end = if latest.state == "fresh" {
            until
        } else {
            observed_at.min(until)
        };
        if latest.state == "fresh" || latest.state == "stale" {
            // The live sidecar can contain an unchanged successful observation
            // with no checkpoint. It proves this tail only, never past liveness.
            points.retain(|point| point.at_unix_millis <= end);
            if observed_at <= end
                && points
                    .last()
                    .is_none_or(|point| point.at_unix_millis <= observed_at)
            {
                push_state_point(
                    &mut points,
                    CreditPoint {
                        at_unix_millis: observed_at,
                        balance: latest.balance.clone(),
                        observed_at_unix_millis: Some(observed_at),
                        contributing_upstreams: usize::from(latest.balance.is_some()),
                        eligible_upstreams: 1,
                    },
                );
            }
            if end >= since {
                push_state_point(
                    &mut points,
                    CreditPoint {
                        at_unix_millis: end,
                        balance: latest.balance.clone(),
                        observed_at_unix_millis: Some(observed_at),
                        contributing_upstreams: usize::from(latest.balance.is_some()),
                        eligible_upstreams: 1,
                    },
                );
            }
            if latest.state == "stale" && end < until {
                points.push(CreditPoint {
                    at_unix_millis: end.saturating_add(1),
                    balance: None,
                    observed_at_unix_millis: None,
                    contributing_upstreams: 0,
                    eligible_upstreams: 1,
                });
            }
        } else {
            push_state_point(
                &mut points,
                CreditPoint {
                    at_unix_millis: observed_at.min(until),
                    balance: None,
                    observed_at_unix_millis: Some(observed_at),
                    contributing_upstreams: 0,
                    eligible_upstreams: 1,
                },
            );
        }
    } else if points.last().is_some_and(|point| point.balance.is_some()) {
        points.push(CreditPoint {
            at_unix_millis: until.min(now),
            balance: None,
            observed_at_unix_millis: None,
            contributing_upstreams: 0,
            eligible_upstreams: 1,
        });
    }
    points
}
fn bound_points(mut points: Vec<CreditPoint>, max_points: usize) -> Vec<CreditPoint> {
    if points.len() <= max_points {
        return points;
    }
    let last = points.len() - 1;
    let stride = (last as f64 / (max_points.saturating_sub(1) as f64)).ceil() as usize;
    let mut bounded = Vec::with_capacity(max_points);
    for index in (0..=last).step_by(stride.max(1)) {
        bounded.push(points[index].clone());
        if bounded.len() == max_points.saturating_sub(1) {
            break;
        }
    }
    if bounded.last().map(|point| point.at_unix_millis)
        != points.last().map(|point| point.at_unix_millis)
    {
        bounded.push(points.pop().expect("non-empty points"));
    }
    bounded
}

fn push_state_point(points: &mut Vec<CreditPoint>, point: CreditPoint) {
    if points
        .last()
        .is_some_and(|previous| previous.at_unix_millis == point.at_unix_millis)
    {
        points.pop();
    }
    points.push(point);
}

fn pool_history(
    histories: &[CreditUpstreamHistory],
    members: &[usize],
    since: u64,
    until: u64,
    now: u64,
) -> CreditPoolHistory {
    let mut times = BTreeMap::<u64, Vec<(usize, &CreditPoint)>>::new();
    for (member_index, &index) in members.iter().enumerate() {
        for point in &histories[index].series {
            if point.at_unix_millis <= until {
                times
                    .entry(point.at_unix_millis.max(since))
                    .or_default()
                    .push((member_index, point));
            }
        }
    }
    let eligible = members.len();
    let mut state = vec![None::<&CreditPoint>; eligible];
    let mut previous_coverage = Vec::new();
    let mut series = Vec::new();
    for (at, changes) in times {
        for (member, point) in changes {
            state[member] = Some(point);
        }
        let coverage = state
            .iter()
            .enumerate()
            .filter_map(|(index, point)| {
                point
                    .and_then(|point| point.balance.as_ref())
                    .map(|_| index)
            })
            .collect::<Vec<_>>();
        if !series.is_empty() && coverage != previous_coverage {
            series.push(CreditPoint {
                at_unix_millis: at,
                balance: None,
                observed_at_unix_millis: None,
                contributing_upstreams: coverage.len(),
                eligible_upstreams: eligible,
            });
        }
        let balance = exact_sum(
            state
                .iter()
                .flatten()
                .filter_map(|point| point.balance.as_deref()),
        );
        let observed_at = state
            .iter()
            .flatten()
            .filter(|point| point.balance.is_some())
            .filter_map(|point| point.observed_at_unix_millis)
            .min();
        series.push(CreditPoint {
            at_unix_millis: at,
            balance,
            observed_at_unix_millis: observed_at,
            contributing_upstreams: coverage.len(),
            eligible_upstreams: eligible,
        });
        previous_coverage = coverage;
    }
    let current = members
        .iter()
        .map(|&index| &histories[index].latest)
        .filter(|latest| latest.state == "fresh")
        .collect::<Vec<_>>();
    let latest = CreditPoint {
        at_unix_millis: now,
        balance: exact_sum(
            current
                .iter()
                .filter_map(|latest| latest.balance.as_deref()),
        ),
        observed_at_unix_millis: current
            .iter()
            .filter_map(|latest| latest.observed_at_unix_millis)
            .min(),
        contributing_upstreams: current.len(),
        eligible_upstreams: eligible,
    };
    if until == now {
        let current_coverage = members
            .iter()
            .enumerate()
            .filter_map(|(member, &index)| {
                (histories[index].latest.state == "fresh").then_some(member)
            })
            .collect::<Vec<_>>();
        if !series.is_empty() && current_coverage != previous_coverage {
            series.push(CreditPoint {
                at_unix_millis: now,
                balance: None,
                observed_at_unix_millis: None,
                contributing_upstreams: current.len(),
                eligible_upstreams: eligible,
            });
        }
        series.push(latest.clone());
    }
    CreditPoolHistory { latest, series }
}

/// Exact signed decimal addition. Individual provider strings are never
/// rewritten; only the pool sum is emitted in canonical plain-decimal form.
fn exact_sum<'a>(values: impl Iterator<Item = &'a str>) -> Option<String> {
    let mut digits = vec![0u8]; // little-endian base ten, including fractional digits
    let mut scale = 0usize;
    let mut negative = false;
    let mut observed = false;
    for raw in values {
        if !is_credit_balance(raw) {
            return None;
        }
        observed = true;
        let sign = raw.starts_with('-');
        let unsigned = raw.trim_start_matches(['+', '-']);
        let next_scale = unsigned
            .split_once('.')
            .map_or(0, |(_, fraction)| fraction.len());
        let mut next = unsigned
            .bytes()
            .rev()
            .filter(|byte| *byte != b'.')
            .map(|byte| byte - b'0')
            .collect::<Vec<_>>();
        let target_scale = scale.max(next_scale);
        if target_scale > scale {
            digits.resize(digits.len() + target_scale - scale, 0);
            digits.rotate_right(target_scale - scale);
        }
        if target_scale > next_scale {
            next.resize(next.len() + target_scale - next_scale, 0);
            next.rotate_right(target_scale - next_scale);
        }
        scale = target_scale;
        trim_digits(&mut digits);
        trim_digits(&mut next);
        if sign == negative {
            digits.resize(digits.len().max(next.len()), 0);
            let mut carry = 0;
            for (index, digit) in digits.iter_mut().enumerate() {
                let sum = *digit + next.get(index).copied().unwrap_or(0) + carry;
                *digit = sum % 10;
                carry = sum / 10;
            }
            if carry > 0 {
                digits.push(carry);
            }
        } else {
            let order = digits
                .len()
                .cmp(&next.len())
                .then_with(|| digits.iter().rev().cmp(next.iter().rev()));
            if order.is_lt() {
                std::mem::swap(&mut digits, &mut next);
                negative = sign;
            }
            let mut borrow = 0i16;
            for (index, digit) in digits.iter_mut().enumerate() {
                let difference =
                    i16::from(*digit) - i16::from(next.get(index).copied().unwrap_or(0)) - borrow;
                *digit = difference.rem_euclid(10) as u8;
                borrow = i16::from(difference < 0);
            }
        }
        trim_digits(&mut digits);
        if digits.iter().all(|digit| *digit == 0) {
            negative = false;
        }
    }
    if !observed {
        return None;
    }
    while digits.len() <= scale {
        digits.push(0);
    }
    let mut result = String::with_capacity(digits.len() + 2);
    if negative {
        result.push('-');
    }
    for index in (0..digits.len()).rev() {
        result.push(char::from(b'0' + digits[index]));
        if scale > 0 && index == scale {
            result.push('.');
        }
    }
    if scale > 0 {
        while result.ends_with('0') {
            result.pop();
        }
        if result.ends_with('.') {
            result.pop();
        }
    }
    Some(result)
}

fn trim_digits(digits: &mut Vec<u8>) {
    while digits.len() > 1 && digits.last() == Some(&0) {
        digits.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_fractional_signed_and_huge_pool_sums() {
        for (values, expected) in [
            (vec!["0.6", "0.6"], "1.2"),
            (vec!["-0.6", "-0.6"], "-1.2"),
            (vec!["12.99", "-0.42"], "12.57"),
            (
                vec!["99999999999999999999999.999", "+0.001"],
                "100000000000000000000000",
            ),
            (vec!["-12.99", "12.99"], "0"),
            (vec!["-0.0", "+5"], "5"),
        ] {
            assert_eq!(exact_sum(values.into_iter()).as_deref(), Some(expected));
        }
        assert_eq!(exact_sum(std::iter::empty()), None);
        assert_eq!(exact_sum(["NaN"].into_iter()), None);
    }
}
