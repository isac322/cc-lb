use crate::docker::Docker;
use crate::fabric_state::{ResourceNames, docker_command};

pub fn host_published_data_ports(
    docker: &Docker,
    names: &ResourceNames,
) -> Result<Vec<String>, String> {
    let mut ports = Vec::new();
    for name in [&names.probe, &names.cc_lb, &names.postgres] {
        let output = docker_command(
            docker,
            vec![
                "inspect".to_owned(),
                "--format".to_owned(),
                "{{json .HostConfig.PortBindings}}".to_owned(),
                name.clone(),
            ],
            "host port inspection",
        )?;
        let value: serde_json::Value = serde_json::from_str(output.trim())
            .map_err(|error| format!("host port inspection JSON parse failed: {error}"))?;
        if !value.is_null()
            && value
                .as_object()
                .is_none_or(|bindings| !bindings.is_empty())
        {
            ports.push(name.clone());
        }
    }
    Ok(ports)
}
