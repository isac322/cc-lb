use cc_lb_storage_api::principal::{Limit, PrincipalKind, PrincipalRecord};
use cc_lb_storage_api::{CacheKeepaliveConfig, ClassifierConfig};

pub(super) fn principal_with_keepalive() -> cc_lb_storage_api::PrincipalCreate {
    cc_lb_storage_api::PrincipalCreate {
        name: "principal".to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: Vec::new(),
        allowed_upstreams: Vec::new(),
        default_limits: Vec::<Limit>::new(),
        cache_keepalive: Some(keepalive_config()),
    }
}

pub(super) fn principal_record_with_id(id: &str) -> PrincipalRecord {
    PrincipalRecord {
        id: id.parse().unwrap_or_else(|_| uuid::Uuid::from_u128(1)),
        name: id.to_owned(),
        kind: PrincipalKind::Machine,
        allowed_models: vec!["*".to_owned()],
        allowed_upstreams: Vec::new(),
        default_limits: Vec::new(),
        enabled: true,
        deleted_at_unix_secs: None,
        revision: 1,
        created_at_unix_secs: 0,
        updated_at_unix_secs: 0,
        router_terminal_strategy: Default::default(),
        cache_keepalive: Some(keepalive_config()),
    }
}

fn keepalive_config() -> CacheKeepaliveConfig {
    CacheKeepaliveConfig {
        enabled: true,
        refresh_lead_time_5m_secs: 30,
        refresh_lead_time_1h_secs: 300,
        max_refreshes_per_session: 3,
        max_total_duration_secs: 600,
        snapshot_max_bytes: 524_288,
        classifier: ClassifierConfig::default(),
    }
}
