use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    println!("cargo:rerun-if-env-changed=GIT_SHA");
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-env-changed=CI_COMMIT_SHA");

    emit("CC_LB_VERSION", env!("CARGO_PKG_VERSION").to_owned());
    emit("CC_LB_GIT_SHA", git_sha());
    emit("CC_LB_BUILD_TIME", build_time());
    emit("CC_LB_RUSTC", rustc_version());
    emit("CC_LB_FEATURES", features());
    emit(
        "CC_LB_TARGET",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned()),
    );
}

fn emit(name: &str, value: String) {
    println!("cargo:rustc-env={name}={value}");
}

fn git_sha() -> String {
    for name in ["GIT_SHA", "GITHUB_SHA", "CI_COMMIT_SHA"] {
        if let Ok(value) = std::env::var(name) {
            let value = value.trim();
            if !value.is_empty() {
                return value.chars().take(7).collect();
            }
        }
    }

    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| output.status.success().then_some(output.stdout))
        .and_then(|stdout| String::from_utf8(stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn rustc_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    Command::new(&rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| output.status.success().then_some(output.stdout))
        .and_then(|stdout| String::from_utf8(stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or(rustc)
}

fn build_time() -> String {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return "unknown".to_owned();
    };
    utc_rfc3339(duration.as_secs())
}

fn features() -> String {
    let mut features = std::env::vars()
        .filter_map(|(name, _)| name.strip_prefix("CARGO_FEATURE_").map(feature_name))
        .collect::<Vec<_>>();
    features.sort();
    if features.is_empty() {
        "unknown".to_owned()
    } else {
        features.join(",")
    }
}

fn feature_name(value: &str) -> String {
    value.to_ascii_lowercase().replace('_', "-")
}

fn utc_rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let seconds_of_day = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}
