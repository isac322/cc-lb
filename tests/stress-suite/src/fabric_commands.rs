pub fn observer_commands(destination_ipv4: &str, classid: &str) -> Vec<String> {
    vec![
        "tc qdisc replace dev eth0 root handle 1: htb default 1".to_owned(),
        "tc class replace dev eth0 parent 1: classid 1:1 htb rate 1000mbit ceil 1000mbit"
            .to_owned(),
        format!(
            "tc class replace dev eth0 parent 1: classid {classid} htb rate 1000mbit ceil 1000mbit"
        ),
        format!(
            "tc filter replace dev eth0 protocol ip parent 1: prio 10 u32 match ip dst {destination_ipv4}/32 flowid {classid}"
        ),
    ]
}
