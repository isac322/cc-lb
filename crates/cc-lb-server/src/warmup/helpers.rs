use std::hash::Hasher;

use siphasher::sip::SipHasher13;
use uuid::Uuid;

const JITTER_SPREAD_MS: u64 = 30_000;

pub fn stable_jitter_ms(upstream_id: Uuid, candidate_resets_at_unix_secs: u64) -> u64 {
    const SIPHASH_K0: u64 = 0;
    const SIPHASH_K1: u64 = 0;
    let mut hasher = SipHasher13::new_with_keys(SIPHASH_K0, SIPHASH_K1);
    hasher.write(upstream_id.as_bytes());
    hasher.write(&candidate_resets_at_unix_secs.to_le_bytes());
    hasher.finish() % JITTER_SPREAD_MS
}

#[cfg(test)]
mod tests;
