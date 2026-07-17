use crate::elevated_preview::{ElevatedPreviewError, render_preview};

#[test]
fn elevated_preview_is_render_only() -> Result<(), ElevatedPreviewError> {
    // Given: the elevated-netns preview mode.
    // When: its command plan is rendered.
    let evidence = render_preview()?;

    // Then: commands remain sudo-marked data and none execute.
    assert_eq!(evidence.tier, "elevated-netns");
    assert!(evidence.requires_sudo);
    assert!(!evidence.executed);
    assert!(
        evidence
            .commands
            .iter()
            .all(|command| command.requires_sudo)
    );
    assert!(
        evidence
            .commands
            .iter()
            .any(|command| command.command == "sudo ip netns add ccstress_smoke_client")
    );
    assert!(evidence.commands.iter().any(
        |command| command.command == "sudo ip link add name ccstress_elevated_br0 type bridge"
    ));
    assert!(
        evidence
            .commands
            .iter()
            .any(|command| command.command.contains(" tc qdisc replace "))
    );
    Ok(())
}

#[test]
fn elevated_preview_has_cleanup() -> Result<(), ElevatedPreviewError> {
    // Given: the elevated-netns preview command plan.
    // When: setup and cleanup commands are viewed in evidence order.
    let evidence = render_preview()?;
    let ordered = evidence
        .commands
        .iter()
        .chain(&evidence.cleanup)
        .collect::<Vec<_>>();

    // Then: cleanup begins only after all setup and impairment commands.
    assert!(
        evidence
            .commands
            .iter()
            .all(|command| !command.command.contains(" qdisc del "))
    );
    assert_eq!(
        ordered
            .iter()
            .position(|command| command.command.contains(" qdisc del ")),
        Some(evidence.commands.len())
    );
    assert!(
        evidence
            .cleanup
            .iter()
            .any(|command| command.command == "sudo ip netns del ccstress_smoke_client")
    );
    assert!(
        evidence
            .cleanup
            .iter()
            .any(|command| command.command == "sudo ip link del dev ccstress_elevated_br0")
    );
    Ok(())
}
