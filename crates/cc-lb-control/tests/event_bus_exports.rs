use cc_lb_control::{
    BusReceiver, DEFAULT_LIFECYCLE_BROADCAST_CAPACITY, InMemoryBus, LifecycleBusReceiver,
    ReplicaIdentityProvider, RequestEventBus,
};

struct NoReplicaIdentityProvider;

impl ReplicaIdentityProvider for NoReplicaIdentityProvider {
    fn replica_identity(&self) -> Option<cc_lb_domain::ReplicaIdentity> {
        None
    }
}

#[test]
fn public_event_bus_contract_is_available() {
    let bus = InMemoryBus::new();
    let request_receiver = bus.subscribe();
    let lifecycle_receiver = bus.subscribe_lifecycle();
    assert_eq!(DEFAULT_LIFECYCLE_BROADCAST_CAPACITY, 2048);
    assert!(matches!(request_receiver, BusReceiver::InMemory(_)));
    assert!(matches!(
        lifecycle_receiver,
        LifecycleBusReceiver::InMemory(_)
    ));
}

#[test]
fn public_replica_identity_provider_contract_is_available() {
    // Given a provider without an assigned replica identity.
    let provider = NoReplicaIdentityProvider;

    // When the provider identity is requested.
    let replica_identity = provider.replica_identity();

    // Then control exposes the relocated provider trait.
    assert!(replica_identity.is_none());
}
