//! Recognizable, non-secret key fragment shown to operators.

/// Keys shorter than this reveal no suffix.
const MIN_LEN_FOR_SUFFIX: usize = 20;
const SUFFIX_CHARS: usize = 4;
/// Public lead shared by every Anthropic credential.
const LEAD: &str = "sk-ant-";
/// Hint for keys too short to reveal anything beyond a public label.
const OPAQUE: &str = "...";
/// Hint used when the masked form would reproduce the input verbatim, which
/// only happens when the hidden span itself spells the `...` marker.
const COLLISION: &str = "(hidden)";

/// Public Anthropic key families, matched case-sensitively. Only these labels
/// ever appear in a hint; any other text after `sk-ant-` may be secret.
const KNOWN_FAMILIES: [&str; 5] = [
    "sk-ant-api03-",
    "sk-ant-api01-",
    "sk-ant-admin01-",
    "sk-ant-oat01-",
    "sk-ant-usr-",
];

/// Returns an operator-visible hint that never reveals more than a public
/// label plus the last four characters, and always hides some key bytes.
///
/// - Known family with a nonempty body: the family, plus `...` and the last
///   four characters when the key is at least 20 bytes long.
/// - Any other shape: `sk-ant-...` (or `...` without the `sk-ant-` lead) plus
///   the last four characters when the key is at least 20 bytes long, else
///   `...`.
///
/// Returns `None` only for an empty key.
pub fn key_hint(key: &str) -> Option<String> {
    if key.is_empty() {
        return None;
    }
    let suffix = (key.len() >= MIN_LEN_FOR_SUFFIX)
        .then(|| {
            key.len()
                .checked_sub(SUFFIX_CHARS)
                .and_then(|start| key.get(start..))
        })
        .flatten();
    let family = KNOWN_FAMILIES
        .into_iter()
        .find(|family| key.len() > family.len() && key.starts_with(family));
    let hint = match (family, suffix) {
        (Some(family), Some(suffix)) => format!("{family}...{suffix}"),
        (Some(family), None) => family.to_owned(),
        (None, Some(suffix)) if key.starts_with(LEAD) => format!("{LEAD}...{suffix}"),
        (None, Some(suffix)) => format!("{OPAQUE}{suffix}"),
        (None, None) => OPAQUE.to_owned(),
    };
    // A hint must never equal the submitted value, even for literals that
    // contain the masking marker (e.g. `sk-ant-api03-...WXYZ` or `...`).
    Some(if hint == key {
        COLLISION.to_owned()
    } else {
        hint
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Characters of `key` the hint reveals, with the `...` marker removed.
    fn revealed(hint: &str) -> usize {
        hint.replace("...", "").len()
    }

    /// PF-06: hint format over keys of length 12, 20, 40, 108.
    #[test]
    fn pf_06_key_hint_format() {
        let k12 = "sk-ant-api03"; // 12 bytes, no trailing dash: no family
        assert_eq!(k12.len(), 12);
        assert_eq!(key_hint(k12).as_deref(), Some("..."));

        let k19 = "sk-ant-api03-abcdef";
        assert_eq!(k19.len(), 19);
        assert_eq!(key_hint(k19).as_deref(), Some("sk-ant-api03-"));

        let k20 = "sk-ant-api03-abcWXYZ";
        assert_eq!(k20.len(), 20);
        assert_eq!(key_hint(k20).as_deref(), Some("sk-ant-api03-...WXYZ"));

        let k40 = format!("sk-ant-api03-{}LAST", "x".repeat(23));
        assert_eq!(k40.len(), 40);
        assert_eq!(key_hint(&k40).as_deref(), Some("sk-ant-api03-...LAST"));

        let k108 = format!("sk-ant-api03-{}Q9z_", "A".repeat(91));
        assert_eq!(k108.len(), 108);
        assert_eq!(key_hint(&k108).as_deref(), Some("sk-ant-api03-...Q9z_"));

        assert_eq!(
            key_hint("sk-ant-admin01-abcdefghWXYZ").as_deref(),
            Some("sk-ant-admin01-...WXYZ")
        );
        assert_eq!(
            key_hint("sk-ant-oat01-abcdefghWXYZ").as_deref(),
            Some("sk-ant-oat01-...WXYZ")
        );
        assert_eq!(
            key_hint("bogus-key-without-family-0000").as_deref(),
            Some("...0000")
        );
        assert_eq!(
            key_hint("sk-ant--abcdefghijklmnop").as_deref(),
            Some("sk-ant-...mnop")
        );
        assert_eq!(key_hint(""), None);
    }

    #[test]
    fn pf_06_hint_never_reveals_more_than_family_and_four() {
        let key = format!("sk-ant-api03-{}", "s3cr3t".repeat(16));
        let hint = key_hint(&key).unwrap_or_default();
        assert_eq!(revealed(&hint), "sk-ant-api03-".len() + 4);
    }

    /// SEC-KEY-HINT-01: a key that is only a family label must not be echoed
    /// back whole.
    #[test]
    fn sec_key_hint_family_only_is_opaque() {
        for key in KNOWN_FAMILIES {
            assert_eq!(key_hint(key).as_deref(), Some("..."), "{key}");
        }
        assert_eq!(key_hint("sk-ant-api03").as_deref(), Some("..."));
    }

    /// SEC-KEY-HINT-01: an unknown, caller-defined family is never treated
    /// as public, whatever its length.
    #[test]
    fn sec_key_hint_oversized_family_is_not_revealed() {
        let token = "TOKENSECRET".repeat(20);
        let key = format!("sk-ant-{token}-tail");
        let hint = key_hint(&key).unwrap_or_default();
        assert_eq!(hint, "sk-ant-...tail");
        assert!(!hint.contains("TOKEN"), "{hint}");

        let short = "sk-ant-TOKEN1234-";
        assert!(short.len() < MIN_LEN_FOR_SUFFIX);
        assert_eq!(key_hint(short).as_deref(), Some("..."));
    }

    /// SEC-KEY-HINT-01: short keys reveal nothing but a public label.
    #[test]
    fn sec_key_hint_short_key_hides_body() {
        assert_eq!(key_hint("abc").as_deref(), Some("..."));
        assert_eq!(key_hint("sk-ant-api03-x").as_deref(), Some("sk-ant-api03-"));
        assert_eq!(key_hint("sk-ant-x").as_deref(), Some("..."));
    }

    /// SEC-KEY-HINT-01: no input of any shape is echoed whole, and at most a
    /// public label plus four characters are revealed.
    #[test]
    fn sec_key_hint_bounded_for_every_shape() {
        let max_label = KNOWN_FAMILIES.iter().map(|f| f.len()).max().unwrap_or(0);
        let mut keys: Vec<String> = Vec::new();
        for len in 0..64 {
            keys.push("Z".repeat(len));
            keys.push(format!("sk-ant-{}", "Z".repeat(len)));
            keys.push(format!("sk-ant-{}-", "Z".repeat(len)));
            for family in KNOWN_FAMILIES {
                keys.push(format!("{family}{}", "Z".repeat(len)));
            }
        }
        for key in keys.iter().filter(|key| !key.is_empty()) {
            let hint = key_hint(key).unwrap_or_default();
            assert_ne!(&hint, key, "hint echoes the whole key");
            assert!(
                revealed(&hint) <= max_label + SUFFIX_CHARS,
                "{key} -> {hint}"
            );
            assert!(revealed(&hint) < key.len(), "{key} -> {hint}");
        }
    }

    /// SEC-KEY-HINT-01: a literal whose hidden span spells the masking marker
    /// is not handed back verbatim.
    #[test]
    fn sec_key_hint_marker_literal_is_not_echoed() {
        for key in ["sk-ant-api03-...WXYZ", "...", "sk-ant-...WXYZWXYZWXYZ"] {
            let hint = key_hint(key).unwrap_or_default();
            assert_ne!(hint, key);
        }
        assert_eq!(key_hint("...").as_deref(), Some("(hidden)"));
    }
}
