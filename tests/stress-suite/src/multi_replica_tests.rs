use crate::multi_replica_config::{ReplicaPorts, render_replica_config};
use crate::multi_replica_evidence::{
    HotpathEffect, PerReplicaLimit, StorageEvidence, StoragePlaneEffect,
};

#[test]
fn postgres_config_rendering() {
    // Given: a replica with distinct dynamically allocated listener ports.
    let ports = ReplicaPorts::new(31_001, 31_002, 31_003);

    // When: the managed-key replica configuration is rendered.
    let config = render_replica_config("postgres://cc_lb:stress@postgres:5432/cc_lb", ports);

    // Then: it uses shared Postgres storage and the configured listener ports.
    assert!(config.contains("kind = \"postgres\""));
    assert!(config.contains("url = \"postgres://cc_lb:stress@postgres:5432/cc_lb\""));
    assert!(config.contains("proxy_addr = \"0.0.0.0:31001\""));
    assert!(config.contains("admin_addr = \"0.0.0.0:31002\""));
    assert!(config.contains("metrics_addr = \"0.0.0.0:31003\""));
}

#[test]
fn replica_dynamic_port_allocation() {
    // Given: two dynamically allocated replica port triples.
    let replicas = ReplicaPorts::allocate_many(2).expect("dynamic ports");
    let [first, second] = replicas.as_slice() else {
        panic!("two replica port triples");
    };

    // When: their listener ports are compared.
    let mut ports = first
        .all()
        .into_iter()
        .chain(second.all())
        .collect::<Vec<_>>();
    ports.sort_unstable();

    // Then: every proxy, admin, and metrics listener has a unique port.
    ports.dedup();
    assert_eq!(ports.len(), 6);
}

#[test]
fn storage_verdict_splits_hotpath_vs_plane() {
    // Given: storage probes with independent request-path and storage-plane outcomes.
    let evidence = StorageEvidence {
        backend: "postgres".to_owned(),
        plane_effect: StoragePlaneEffect::healthy(),
        hotpath_effect: HotpathEffect::healthy(),
    };

    // When: the storage evidence is validated.
    let result = evidence.validate();

    // Then: both effects must be explicitly populated and classified.
    assert!(result.is_ok());
}

#[test]
fn per_replica_limit_assert() {
    // Given: independent local limits for each replica after the managed-key wave.
    let limits = [
        PerReplicaLimit::new("replica-0", 2, 2),
        PerReplicaLimit::new("replica-1", 2, 2),
    ];

    // When: the per-replica assertion is evaluated.
    let result = PerReplicaLimit::validate_all(&limits);

    // Then: each replica respects its local limit without a global quota assertion.
    assert!(result.is_ok());
}
