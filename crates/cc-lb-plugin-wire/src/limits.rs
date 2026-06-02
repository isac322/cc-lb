//! Guardrail constants for WASM plugin handshake protocol.
//!
//! This module centralizes all numeric and structural limits to prevent drift
//! between host and guest implementations. All constants are compile-time only;
//! runtime overrides are not supported.

// Custom section
pub const CUSTOM_SECTION_MAX_SIZE: usize = 64 * 1024;
pub const CUSTOM_SECTION_FIELD_COUNT: usize = 4;

// Handshake
pub const HANDSHAKE_WALL_MS: u64 = 200;
pub const HANDSHAKE_FUEL: u64 = 10_000_000;
pub const HANDSHAKE_OUTPUT_MAX_BYTES: usize = 32 * 1024;

// Self-check
pub const SELF_CHECK_WALL_MS: u64 = 200;
pub const SELF_CHECK_FUEL: u64 = 10_000_000;
pub const SELF_CHECK_OUTPUT_MAX_BYTES: usize = 8 * 1024;

// Function/capability counts
pub const FUNCTION_VERSIONS_KEYS_MAX: usize = 32;
pub const FUNCTION_VERSIONS_PER_FN_MAX: usize = 16;
pub const CAPABILITIES_MAX_COUNT: usize = 32;
pub const CAPABILITY_NAME_MAX_BYTES: usize = 64;
pub const IMPLEMENTED_FUNCTIONS_MAX: usize = 16;

// Plugin metadata
pub const PLUGIN_NAME_MAX_BYTES: usize = 64;
pub const PLUGIN_VERSION_MAX_BYTES: usize = 32;

// Versions
pub const VERSION_MIN: u32 = 1;
pub const VERSION_MAX: u32 = u16::MAX as u32;

// Augmented metadata
pub const AUGMENTED_METADATA_MAX_BYTES: usize = 256 * 1024;

// Registry
pub const REGISTRY_ROW_COUNT_SOFT_WARN: usize = 1_000;
pub const REGISTRY_ROW_COUNT_HARD_REJECT: usize = 10_000;

// Startup re-handshake
pub const STARTUP_HANDSHAKE_TOTAL_BUDGET_MS: u64 = 60_000;
pub const STARTUP_HANDSHAKE_PARALLEL_MAX: usize = 8;
pub const SKIP_HANDSHAKE_IF_FRESH_TTL_SECS: u64 = 7 * 24 * 3600;

// Regex patterns (compile-time strings)
pub const PLUGIN_NAME_PATTERN: &str = r"^[a-z][a-z0-9_-]*$";
pub const CAPABILITY_PATTERN: &str = r"^[a-z][a-z0-9_]*$";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_bounds_make_sense() {
        assert!(VERSION_MIN < VERSION_MAX);
        assert_eq!(VERSION_MIN, 1);
        assert_eq!(VERSION_MAX, u16::MAX as u32);
    }

    #[test]
    fn handshake_bounds_sensible() {
        assert!(HANDSHAKE_WALL_MS > 0);
        assert!(HANDSHAKE_FUEL > 0);
        assert!(HANDSHAKE_OUTPUT_MAX_BYTES > 0);
        assert!(HANDSHAKE_OUTPUT_MAX_BYTES < CUSTOM_SECTION_MAX_SIZE);
    }

    #[test]
    fn self_check_bounds_sensible() {
        assert!(SELF_CHECK_WALL_MS > 0);
        assert!(SELF_CHECK_FUEL > 0);
        assert!(SELF_CHECK_OUTPUT_MAX_BYTES > 0);
        assert!(SELF_CHECK_OUTPUT_MAX_BYTES < HANDSHAKE_OUTPUT_MAX_BYTES);
    }

    #[test]
    fn function_counts_consistent() {
        assert!(FUNCTION_VERSIONS_KEYS_MAX > 0);
        assert!(FUNCTION_VERSIONS_PER_FN_MAX > 0);
        assert!(IMPLEMENTED_FUNCTIONS_MAX <= FUNCTION_VERSIONS_KEYS_MAX);
    }

    #[test]
    fn capability_counts_sensible() {
        assert!(CAPABILITIES_MAX_COUNT > 0);
        assert!(CAPABILITY_NAME_MAX_BYTES > 0);
    }

    #[test]
    fn metadata_size_bounds_sensible() {
        assert!(PLUGIN_NAME_MAX_BYTES > 0);
        assert!(PLUGIN_VERSION_MAX_BYTES > 0);
        assert!(AUGMENTED_METADATA_MAX_BYTES > CUSTOM_SECTION_MAX_SIZE);
    }

    #[test]
    fn custom_section_field_count_matches_identity() {
        // PluginIdentity has: magic, abi_envelope, plugin_name, plugin_version = 4 fields
        assert_eq!(CUSTOM_SECTION_FIELD_COUNT, 4);
    }

    #[test]
    fn registry_rejection_above_warning() {
        assert!(REGISTRY_ROW_COUNT_SOFT_WARN < REGISTRY_ROW_COUNT_HARD_REJECT);
    }

    #[test]
    fn startup_budget_reasonable() {
        assert!(STARTUP_HANDSHAKE_TOTAL_BUDGET_MS >= 60_000);
        assert!(STARTUP_HANDSHAKE_PARALLEL_MAX > 0);
        assert!(STARTUP_HANDSHAKE_PARALLEL_MAX <= 16);
    }

    #[test]
    fn ttl_is_one_week() {
        assert_eq!(SKIP_HANDSHAKE_IF_FRESH_TTL_SECS, 7 * 24 * 3600);
    }

    #[test]
    fn wall_times_equal_across_phases() {
        // Both handshake and self-check have same timeout window
        assert_eq!(HANDSHAKE_WALL_MS, SELF_CHECK_WALL_MS);
    }

    #[test]
    fn fuel_equal_across_phases() {
        // Both handshake and self-check have same fuel budget
        assert_eq!(HANDSHAKE_FUEL, SELF_CHECK_FUEL);
    }
}
