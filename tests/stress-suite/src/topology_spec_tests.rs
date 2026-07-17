use std::collections::HashSet;

use crate::netem_render::render_topology;
use crate::topology_spec::{TopologySpec, TopologyValidationError};

#[test]
fn topology_directional_rejects_localhost() {
    // Given: a direct data edge that would bypass the finalized bridge topology.
    let topology = TopologySpec::localhost_data_edge();

    // When: the topology boundary validates the directed edge.
    let result = topology.validate();

    // Then: direct localhost data transport has an explicit machine-readable rejection.
    assert!(matches!(
        result,
        Err(TopologyValidationError::DirectLocalhostDataEdge { .. })
    ));
}

#[test]
fn netem_render_is_directional() {
    // Given: paired client and proxy data edges with a deterministic seed.
    let topology = TopologySpec::smoke(1);

    // When: the pure netem plan renders every directed data edge.
    let rendered = render_topology(&topology).expect("smoke topology renders");
    let client_to_proxy = rendered
        .iter()
        .find(|edge| edge.edge_id == "client-to-proxy")
        .expect("client-to-proxy edge exists");
    let proxy_to_client = rendered
        .iter()
        .find(|edge| edge.edge_id == "proxy-to-client")
        .expect("proxy-to-client edge exists");

    // Then: each direction executes in its sender namespace with a distinct class.
    assert_ne!(client_to_proxy.sender_netns, proxy_to_client.sender_netns);
    assert_ne!(client_to_proxy.classid, proxy_to_client.classid);
    for command in rendered.iter().flat_map(|edge| &edge.commands) {
        let tokens = command.split_whitespace().collect::<Vec<_>>();
        let has_netem = tokens.contains(&"netem");
        let has_seed = tokens.contains(&"seed");
        assert_eq!(has_netem, has_seed, "seed placement in {command}");
        if has_netem {
            assert!(tokens.windows(2).any(|pair| pair == ["seed", "1"]));
        }
    }
}

#[test]
fn netem_render_per_destination_classid() {
    // Given: a validated smoke topology containing two finalized static destinations.
    let topology = TopologySpec::smoke(1);

    // When: the netem command sets are rendered for the directed data edges.
    let rendered = render_topology(&topology).expect("smoke topology renders");

    // Then: every destination filter binds a unique directed classid.
    let classids = rendered
        .iter()
        .map(|edge| edge.classid.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(classids.len(), rendered.len());
    for edge in &rendered {
        let filter = format!(
            "match ip dst {}/32 flowid {}",
            edge.destination_ipv4, edge.classid
        );
        assert!(
            edge.commands
                .iter()
                .any(|command| command.contains(&filter))
        );
    }
}
