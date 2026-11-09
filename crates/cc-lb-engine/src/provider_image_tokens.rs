use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

const UNKNOWN_OPENAI_TOKENS: u64 = 73_800;
const UNKNOWN_GEMINI_TOKENS: u64 = 65_536;
const PATCH_REJECTION_LIMIT: u64 = 30_000;
const GEMINI_OLD_TILE_TOKENS: u64 = 258;

type Ratio = (u64, u64);

#[derive(Clone, Copy)]
struct DetailPolicy {
    max_dim: u32,
    patch_budget: Option<u64>,
}

#[derive(Clone, Copy)]
struct PatchPolicy {
    low: DetailPolicy,
    high: DetailPolicy,
    original: Option<DetailPolicy>,
    auto: Detail,
    multiplier: Ratio,
}

#[derive(Clone, Copy)]
enum OpenAiPolicy {
    Patch(PatchPolicy),
    Tiles { base: u64, tile: u64 },
    Raw { multiplier: Ratio },
}

#[derive(Clone, Copy)]
enum Detail {
    Low,
    High,
    Original,
}

/// Decode only a bounded prefix of raw Base64 and inspect image headers.
///
/// Callers remove a data-URL prefix before invoking this function. The image body is
/// deliberately never decoded in full: all supported dimensions are in the first few
/// bytes of the PNG, JPEG, GIF, or WebP container.
pub(super) fn dimensions_base64(encoded: &str) -> Option<(u32, u32)> {
    const MAX_ENCODED_PREFIX: usize = 5_460; // 4,095 decoded bytes, enough for bounded scans.
    let prefix_len = encoded.len().min(MAX_ENCODED_PREFIX);
    let mut decoded = [0_u8; 4_096];
    let written = STANDARD
        .decode_slice(&encoded.as_bytes()[..prefix_len], &mut decoded)
        .ok()?;
    dimensions_bytes(&decoded[..written])
}

fn dimensions_bytes(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        if bytes.len() < 24 || &bytes[12..16] != b"IHDR" {
            return None;
        }
        return positive_pair(
            u32::from_be_bytes(bytes[16..20].try_into().ok()?),
            u32::from_be_bytes(bytes[20..24].try_into().ok()?),
        );
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        if bytes.len() < 10 {
            return None;
        }
        return positive_pair(
            u16::from_le_bytes(bytes[6..8].try_into().ok()?) as u32,
            u16::from_le_bytes(bytes[8..10].try_into().ok()?) as u32,
        );
    }
    if bytes.starts_with(b"RIFF") && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
        return webp_dimensions(bytes);
    }
    if bytes.starts_with(&[0xff, 0xd8]) {
        return jpeg_dimensions(bytes);
    }
    None
}

fn positive_pair(width: u32, height: u32) -> Option<(u32, u32)> {
    (width > 0 && height > 0).then_some((width, height))
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    let mut offset = 2;
    while offset < bytes.len() {
        if bytes[offset] != 0xff {
            return None;
        }
        while offset < bytes.len() && bytes[offset] == 0xff {
            offset += 1;
        }
        let marker = *bytes.get(offset)?;
        offset += 1;
        if marker == 0xd8 || marker == 0xd9 {
            continue;
        }
        if marker == 0xda {
            return None;
        }
        if marker == 0x01 || (0xd0..=0xd7).contains(&marker) {
            continue;
        }
        let length = u16::from_be_bytes(bytes.get(offset..offset + 2)?.try_into().ok()?) as usize;
        if length < 2 || offset.checked_add(length)? > bytes.len() {
            return None;
        }
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            if length < 7 {
                return None;
            }
            let height = u16::from_be_bytes(bytes[offset + 3..offset + 5].try_into().ok()?) as u32;
            let width = u16::from_be_bytes(bytes[offset + 5..offset + 7].try_into().ok()?) as u32;
            return positive_pair(width, height);
        }
        offset += length;
    }
    None
}

fn webp_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 20 {
        return None;
    }
    let chunk = &bytes[12..16];
    let chunk_size = u32::from_le_bytes(bytes[16..20].try_into().ok()?) as usize;
    let end = 20usize.checked_add(chunk_size)?.min(bytes.len());
    match chunk {
        b"VP8 " => {
            if end < 30 || bytes[23..26] != [0x9d, 0x01, 0x2a] {
                return None;
            }
            let width = u16::from_le_bytes(bytes[26..28].try_into().ok()?) & 0x3fff;
            let height = u16::from_le_bytes(bytes[28..30].try_into().ok()?) & 0x3fff;
            positive_pair(width as u32, height as u32)
        }
        b"VP8L" => {
            if end < 25 || bytes[20] != 0x2f {
                return None;
            }
            let width = 1 + u32::from(bytes[21]) + (u32::from(bytes[22] & 0x3f) << 8);
            let height = 1
                + u32::from(bytes[22] >> 6)
                + (u32::from(bytes[23]) << 2)
                + (u32::from(bytes[24] & 0x0f) << 10);
            positive_pair(width, height)
        }
        b"VP8X" => {
            if end < 30 {
                return None;
            }
            let width = 1
                + u32::from(bytes[24])
                + (u32::from(bytes[25]) << 8)
                + (u32::from(bytes[26]) << 16);
            let height = 1
                + u32::from(bytes[27])
                + (u32::from(bytes[28]) << 8)
                + (u32::from(bytes[29]) << 16);
            positive_pair(width, height)
        }
        _ => None,
    }
}

pub(super) fn openai_image_tokens(
    model: &str,
    detail: Option<&Value>,
    dimensions: Option<(u32, u32)>,
) -> u64 {
    match openai_policy(model) {
        Some(OpenAiPolicy::Patch(policy)) => patch_tokens(policy, detail, dimensions),
        Some(OpenAiPolicy::Tiles { base, tile }) => tile_tokens(base, tile, detail, dimensions),
        Some(OpenAiPolicy::Raw { multiplier }) => raw_tokens(multiplier, dimensions),
        None => UNKNOWN_OPENAI_TOKENS,
    }
}

fn patch_tokens(
    policy: PatchPolicy,
    detail: Option<&Value>,
    dimensions: Option<(u32, u32)>,
) -> u64 {
    let selected = match detail {
        None => Some(policy.auto),
        Some(Value::String(value)) => match value.as_str() {
            "low" => Some(Detail::Low),
            "high" => Some(Detail::High),
            "original" => policy.original.map(|_| Detail::Original),
            "auto" => Some(policy.auto),
            _ => None,
        },
        Some(_) => None,
    };
    selected
        .map(|detail| patch_tokens_for(policy, detail, dimensions))
        .unwrap_or_else(|| max_supported_patch_tokens(policy, dimensions))
}

fn max_supported_patch_tokens(policy: PatchPolicy, dimensions: Option<(u32, u32)>) -> u64 {
    let mut maximum = patch_tokens_for(policy, Detail::Low, dimensions);
    maximum = maximum.max(patch_tokens_for(policy, Detail::High, dimensions));
    if policy.original.is_some() {
        maximum = maximum.max(patch_tokens_for(policy, Detail::Original, dimensions));
    }
    maximum
}

fn patch_tokens_for(policy: PatchPolicy, detail: Detail, dimensions: Option<(u32, u32)>) -> u64 {
    let detail_policy = match detail {
        Detail::Low => policy.low,
        Detail::High => policy.high,
        Detail::Original => policy.original.unwrap_or(policy.high),
    };
    let patches = dimensions
        .and_then(|(width, height)| patch_count(width, height, detail_policy))
        .unwrap_or_else(|| patch_ceiling(detail_policy));
    ceil_ratio(patches, policy.multiplier)
}

fn patch_ceiling(policy: DetailPolicy) -> u64 {
    let square = ceil_div(u64::from(policy.max_dim), 32).saturating_pow(2);
    policy
        .patch_budget
        .unwrap_or(PATCH_REJECTION_LIMIT)
        .min(square)
        .min(PATCH_REJECTION_LIMIT)
}

fn patch_count(width: u32, height: u32, policy: DetailPolicy) -> Option<u64> {
    if width == 0 || height == 0 || policy.max_dim == 0 {
        return None;
    }
    let mut width = f64::from(width);
    let mut height = f64::from(height);
    let dimension = width.max(height);
    if dimension > f64::from(policy.max_dim) {
        let scale = f64::from(policy.max_dim) / dimension;
        width = (width * scale).floor().max(1.0);
        height = (height * scale).floor().max(1.0);
    }
    let mut patches = patch_product(width as u64, height as u64);
    if let Some(budget) = policy.patch_budget
        && patches > budget
    {
        let shrink = ((1024.0 * budget as f64) / (width * height)).sqrt();
        let scaled_width = width * shrink;
        let scaled_height = height * shrink;
        let width_ratio = (scaled_width / 32.0).floor() / (scaled_width / 32.0);
        let height_ratio = (scaled_height / 32.0).floor() / (scaled_height / 32.0);
        let adjusted = shrink * width_ratio.min(height_ratio);
        width = (width * adjusted).floor().max(1.0);
        height = (height * adjusted).floor().max(1.0);
        patches = patch_product(width as u64, height as u64);
        if patches > budget {
            patches = budget;
        }
    }
    Some(patches.min(PATCH_REJECTION_LIMIT))
}

fn patch_product(width: u64, height: u64) -> u64 {
    ceil_div(width, 32).saturating_mul(ceil_div(height, 32))
}

fn ceil_div(value: u64, divisor: u64) -> u64 {
    value.saturating_add(divisor - 1) / divisor
}

fn ceil_ratio(value: u64, (numerator, denominator): Ratio) -> u64 {
    value
        .saturating_mul(numerator)
        .saturating_add(denominator - 1)
        / denominator
}

fn tile_tokens(
    base: u64,
    tile: u64,
    detail: Option<&Value>,
    dimensions: Option<(u32, u32)>,
) -> u64 {
    let high = match detail {
        None => true,
        Some(Value::String(value)) => !matches!(value.as_str(), "low"),
        Some(_) => true,
    };
    if !high {
        return base;
    }
    let tiles = dimensions
        .and_then(|(width, height)| {
            if width == 0 || height == 0 {
                return None;
            }
            let mut width = f64::from(width);
            let mut height = f64::from(height);
            let dimension = width.max(height);
            if dimension > 2048.0 {
                let scale = 2048.0 / dimension;
                width = (width * scale).floor().max(1.0);
                height = (height * scale).floor().max(1.0);
            }
            let short = width.min(height);
            if short > 768.0 {
                if width <= height {
                    height = (height * 768.0 / width).floor().max(1.0);
                    width = 768.0;
                } else {
                    width = (width * 768.0 / height).floor().max(1.0);
                    height = 768.0;
                }
            }
            Some(ceil_div(width as u64, 512).saturating_mul(ceil_div(height as u64, 512)))
        })
        .unwrap_or(8);
    base.saturating_add(tiles.saturating_mul(tile))
}

fn raw_tokens(multiplier: Ratio, dimensions: Option<(u32, u32)>) -> u64 {
    let patches = dimensions
        .filter(|(width, height)| *width > 0 && *height > 0)
        .map(|(width, height)| {
            patch_product(u64::from(width), u64::from(height)).min(PATCH_REJECTION_LIMIT)
        })
        .unwrap_or(PATCH_REJECTION_LIMIT);
    ceil_ratio(patches, multiplier)
}

fn openai_policy(model: &str) -> Option<OpenAiPolicy> {
    openai_family_policy(model).or_else(|| openai_dated_policy(model))
}

fn openai_family_policy(model: &str) -> Option<OpenAiPolicy> {
    let patch = |low, high, original, auto, multiplier| {
        OpenAiPolicy::Patch(PatchPolicy {
            low,
            high,
            original,
            auto,
            multiplier,
        })
    };
    let d = |max_dim, patch_budget| DetailPolicy {
        max_dim,
        patch_budget,
    };
    let policy = match model {
        "gpt-6-astra" => patch(
            d(512, None),
            d(65_535, Some(2_500)),
            Some(d(65_535, None)),
            Detail::Original,
            (6, 5),
        ),
        "gpt-5.6-sol" | "gpt-5.6-terra" | "gpt-5.6-luna" => patch(
            d(512, None),
            d(2_048, Some(2_500)),
            Some(d(65_535, None)),
            Detail::Original,
            (6, 5),
        ),
        "gpt-5.5" => patch(
            d(512, None),
            d(2_048, Some(2_500)),
            Some(d(6_000, Some(10_000))),
            Detail::Original,
            (6, 5),
        ),
        "gpt-5.4" | "gpt-5.4-mini" | "gpt-5.4-nano" => patch(
            d(2_048, Some(6_144)),
            d(2_048, Some(2_500)),
            Some(d(6_000, Some(10_000))),
            Detail::High,
            (6, 5),
        ),
        "gpt-5.2" => patch(
            d(2_048, Some(6_144)),
            d(2_048, Some(6_144)),
            None,
            Detail::High,
            (6, 5),
        ),
        "gpt-4.1-mini" => patch(
            d(2_048, Some(6_144)),
            d(2_048, Some(6_144)),
            None,
            Detail::High,
            (81, 50),
        ),
        "gpt-5-mini" => OpenAiPolicy::Raw { multiplier: (6, 5) },
        "gpt-5-nano" => OpenAiPolicy::Raw { multiplier: (3, 2) },
        "gpt-4.1-nano" => OpenAiPolicy::Raw {
            multiplier: (123, 50),
        },
        "o4-mini" => OpenAiPolicy::Raw {
            multiplier: (43, 25),
        },
        "gpt-5.1" | "gpt-5" => OpenAiPolicy::Tiles {
            base: 70,
            tile: 140,
        },
        "gpt-4o" | "gpt-4.1" => OpenAiPolicy::Tiles {
            base: 85,
            tile: 170,
        },
        "gpt-4o-mini" => OpenAiPolicy::Tiles {
            base: 2_833,
            tile: 5_667,
        },
        "o1" | "o1-pro" | "o3" => OpenAiPolicy::Tiles {
            base: 75,
            tile: 150,
        },
        _ => return None,
    };
    Some(policy)
}

// Dated policies are opt-in: unsupported future snapshots must use the unknown fallback.
const OPENAI_DATED_SNAPSHOTS: &[(&str, &str)] = &[
    ("gpt-4.1-mini-2025-04-14", "gpt-4.1-mini"),
    ("gpt-4.1-nano-2025-04-14", "gpt-4.1-nano"),
    ("gpt-4o-2024-08-06", "gpt-4o"),
    ("gpt-4o-2024-11-20", "gpt-4o"),
    ("gpt-4.1-2025-04-14", "gpt-4.1"),
    ("gpt-4o-mini-2024-07-18", "gpt-4o-mini"),
    ("gpt-5-2025-04-16", "gpt-5"),
    ("gpt-5-mini-2025-08-07", "gpt-5-mini"),
    ("gpt-5-nano-2025-04-14", "gpt-5-nano"),
    ("o4-mini-2025-04-16", "o4-mini"),
    ("o3-2025-04-16", "o3"),
];

fn openai_dated_policy(model: &str) -> Option<OpenAiPolicy> {
    let (_, family) = OPENAI_DATED_SNAPSHOTS
        .iter()
        .find(|(snapshot, _)| *snapshot == model)?;
    openai_family_policy(family)
}

pub(super) fn gemini_image_tokens(
    model: &str,
    resolution: Option<&Value>,
    dimensions: Option<(u32, u32)>,
) -> u64 {
    match gemini_generation(model) {
        Some(true) => gemini_resolution_tokens(resolution),
        Some(false) => gemini_legacy_tokens(dimensions),
        None => UNKNOWN_GEMINI_TOKENS,
    }
}

fn gemini_resolution_tokens(resolution: Option<&Value>) -> u64 {
    let Some(value) = resolution else {
        return 1_120;
    };
    let level = value
        .as_str()
        .or_else(|| value.get("level").and_then(Value::as_str));
    match level {
        Some("MEDIA_RESOLUTION_LOW" | "low") => 280,
        Some("MEDIA_RESOLUTION_MEDIUM" | "medium") => 560,
        Some("MEDIA_RESOLUTION_HIGH" | "high") => 1_120,
        Some("MEDIA_RESOLUTION_ULTRA_HIGH" | "ultra_high") => 2_240,
        Some("MEDIA_RESOLUTION_UNSPECIFIED" | "unspecified") => 1_120,
        _ => 2_240,
    }
}

fn gemini_legacy_tokens(dimensions: Option<(u32, u32)>) -> u64 {
    let Some((width, height)) = dimensions.filter(|(width, height)| *width > 0 && *height > 0)
    else {
        return UNKNOWN_GEMINI_TOKENS;
    };
    if width <= 384 && height <= 384 {
        return GEMINI_OLD_TILE_TOKENS;
    }
    let crop = u64::from(width.min(height)).saturating_mul(2) / 3;
    if crop == 0 {
        return UNKNOWN_GEMINI_TOKENS;
    }
    let tiles = ceil_div(u64::from(width), crop).saturating_mul(ceil_div(u64::from(height), crop));
    tiles.saturating_mul(GEMINI_OLD_TILE_TOKENS)
}

/// The model list is intentionally explicit: a future model must not silently inherit
/// a resolution contract merely because its name shares a numeric prefix.
fn gemini_generation(model: &str) -> Option<bool> {
    let suffix = model.strip_prefix("gemini-")?;
    let major = suffix.as_bytes().first().copied()?;
    let boundary = suffix.as_bytes().get(1).copied();
    match (major, boundary) {
        (b'3' | b'4', None | Some(b'.' | b'-')) => Some(true),
        (b'1' | b'2', Some(b'.' | b'-')) => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(value: &str) -> Value {
        serde_json::from_str(value).unwrap()
    }

    #[test]
    fn official_openai_examples_and_detail_policies() {
        assert_eq!(
            openai_image_tokens("gpt-4o", Some(&json("\"low\"")), Some((4000, 4000))),
            85
        );
        assert_eq!(openai_image_tokens("gpt-4o", None, Some((2048, 2048))), 765);
        assert_eq!(
            openai_image_tokens("gpt-6-astra", Some(&json("\"high\"")), Some((1024, 1024))),
            1_229
        );
        assert_eq!(
            openai_image_tokens("gpt-6-astra", Some(&json("\"high\"")), Some((2048, 2048))),
            3_000
        );
        assert_eq!(
            openai_image_tokens("gpt-6-astra", Some(&json("\"high\"")), Some((4096, 512))),
            2_458
        );
        assert_eq!(
            openai_image_tokens("gpt-5.4", None, Some((2048, 2048))),
            3_000
        );
        assert_eq!(
            openai_image_tokens("gpt-5.4", Some(&json("\"low\"")), Some((2048, 2048))),
            4_916
        );
        assert_eq!(
            openai_image_tokens("gpt-5.4", Some(&json("\"bogus\"")), Some((4096, 512))),
            2_458
        );
        assert_eq!(openai_image_tokens("gpt-5.2", None, None), 4_916);
        assert_eq!(openai_image_tokens("gpt-4.1-mini", None, None), 6_636);
    }

    #[test]
    fn deprecated_rows_and_exact_dates() {
        assert_eq!(
            openai_image_tokens("gpt-5-nano", None, Some((1024, 1024))),
            1_536
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1-nano-2025-04-14", None, Some((1024, 1024))),
            2_520
        );
        assert_eq!(
            openai_image_tokens("o4-mini", None, Some((1024, 1024))),
            1_762
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1-mini-2025-04-14", None, Some((1024, 1024))),
            1_659
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1-mini-2025-04-15", None, Some((1024, 1024))),
            UNKNOWN_OPENAI_TOKENS
        );
        assert_eq!(
            openai_image_tokens("gpt-5-nano-2025-04-14", None, Some((1024, 1024))),
            1_536
        );
        assert_eq!(
            openai_image_tokens("gpt-4o-2024-11-20", None, Some((2048, 2048))),
            765
        );
        assert_eq!(
            openai_image_tokens("gpt-4o-2024-08-06", None, Some((2048, 2048))),
            765
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1-2025-04-14", None, Some((2048, 2048))),
            765
        );
        assert_eq!(
            openai_image_tokens("gpt-4o-mini-2024-07-18", None, None),
            48_169
        );
        assert_eq!(
            openai_image_tokens("gpt-5-2025-04-16", None, Some((2048, 2048))),
            630
        );
        assert_eq!(
            openai_image_tokens("gpt-5-2025-08-07", None, Some((2048, 2048))),
            UNKNOWN_OPENAI_TOKENS
        );
        assert_eq!(
            openai_image_tokens("gpt-5-mini-2025-08-07", None, Some((1024, 1024))),
            1_229
        );
        assert_eq!(
            openai_image_tokens("o3-2025-04-16", None, Some((1024, 1024))),
            675
        );
        assert_eq!(
            openai_image_tokens("o4-mini-2025-04-16", None, Some((1024, 1024))),
            1_762
        );
        assert_eq!(
            openai_image_tokens("not-a-model", None, None),
            UNKNOWN_OPENAI_TOKENS
        );
        assert_eq!(
            openai_image_tokens("gpt-5-nano-2025-04-14-2025-04-14", None, Some((1024, 1024))),
            UNKNOWN_OPENAI_TOKENS
        );
        let mut repeated = String::from("gpt-5-nano");
        for _ in 0..128 {
            repeated.push_str("-2025-04-14");
        }
        assert_eq!(
            openai_image_tokens(&repeated, None, Some((1024, 1024))),
            UNKNOWN_OPENAI_TOKENS
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1-2099-01-01", None, Some((1024, 1024))),
            UNKNOWN_OPENAI_TOKENS
        );
    }

    #[test]
    fn tile_aspect_ratio_and_unknown_detail() {
        assert_eq!(
            openai_image_tokens("gpt-4.1", None, Some((2048, 2048))),
            765
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1", Some(&json("\"bogus\"")), Some((4096, 512))),
            765
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1", None, Some((2048, 1024))),
            1_105
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1", None, Some((1024, 2048))),
            1_105
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1", None, Some((2048, 1400))),
            1_105
        );
        assert_eq!(
            openai_image_tokens("gpt-4.1", None, Some((1400, 2048))),
            1_105
        );
        assert_eq!(openai_image_tokens("gpt-4o-mini", None, None), 48_169);
        assert_eq!(
            openai_image_tokens("gpt-4o-mini", Some(&json("\"low\"")), Some((1024, 1024))),
            2_833
        );
    }

    #[test]
    fn gemini_examples_and_resolution_levels() {
        assert_eq!(
            gemini_image_tokens("gemini-1.5-flash", None, Some((384, 384))),
            258
        );
        assert_eq!(
            gemini_image_tokens("gemini-1.5-flash", None, Some((960, 540))),
            1_548
        );
        assert_eq!(
            gemini_image_tokens("gemini-1.5-flash", None, Some((1024, 1024))),
            1_032
        );
        assert_eq!(
            gemini_image_tokens("gemini-1.5-flash", None, Some((100_000, 385))),
            201_756
        );
        assert_eq!(
            gemini_image_tokens("gemini-3", Some(&json("\"low\"")), None),
            280
        );
        assert_eq!(gemini_image_tokens("gemini-4", None, None), 1_120);
        assert_eq!(
            gemini_image_tokens("gemini-3-pro", None, Some((1, 1))),
            1_120
        );
        assert_eq!(
            gemini_image_tokens("gemini-4-pro", Some(&json("{\"level\":\"low\"}")), None),
            280
        );
        assert_eq!(
            gemini_image_tokens("gemini-3-pro", Some(&json("\"bad\"")), None),
            2_240
        );
        assert_eq!(
            gemini_image_tokens("gemini-2.5-pro", None, None),
            UNKNOWN_GEMINI_TOKENS
        );
        assert_eq!(
            gemini_image_tokens("gemini-30-pro", None, Some((384, 384))),
            UNKNOWN_GEMINI_TOKENS
        );
        assert_eq!(
            gemini_image_tokens("gemini-30", None, None),
            UNKNOWN_GEMINI_TOKENS
        );
    }

    #[test]
    fn bounded_header_parsing_and_trailing_data() {
        let mut png = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        png.extend_from_slice(&4000_u32.to_be_bytes());
        png.extend_from_slice(&3000_u32.to_be_bytes());
        let mut encoded = STANDARD.encode(png);
        encoded.push_str(&"A".repeat(40_000));
        assert_eq!(dimensions_base64(&encoded), Some((4000, 3000)));
        assert_eq!(dimensions_base64(&"A".repeat(40_000)), None);
    }

    #[test]
    fn all_header_formats_and_invalid_boundaries() {
        let mut gif = b"GIF89a".to_vec();
        gif.extend_from_slice(&12_u16.to_le_bytes());
        gif.extend_from_slice(&34_u16.to_le_bytes());
        assert_eq!(dimensions_base64(&STANDARD.encode(gif)), Some((12, 34)));
        let mut jpeg = vec![0xff, 0xd8, 0xff, 0xc0, 0, 8, 8, 0, 34, 0, 12, 1];
        jpeg.extend_from_slice(&[0xff, 0xd9]);
        assert_eq!(dimensions_base64(&STANDARD.encode(jpeg)), Some((12, 34)));
        for chunk in [b"VP8X".as_slice(), b"VP8L".as_slice(), b"VP8 ".as_slice()] {
            let mut webp = b"RIFF\0\0\0\0WEBP".to_vec();
            webp.extend_from_slice(chunk);
            webp.extend_from_slice(&10_u32.to_le_bytes());
            webp.extend_from_slice(&[0; 10]);
            if chunk == &b"VP8X"[..] {
                webp.resize(30, 0);
                webp[24..27].copy_from_slice(&[99, 0, 0]);
                webp[27..30].copy_from_slice(&[77, 0, 0]);
            } else if chunk == &b"VP8L"[..] {
                webp.resize(25, 0);
                webp[20] = 0x2f;
                webp[21] = 9;
                webp[22] = 0;
                webp[23] = 5;
                webp[24] = 0;
            } else {
                webp.resize(30, 0);
                webp[23..26].copy_from_slice(&[0x9d, 0x01, 0x2a]);
                webp[26..28].copy_from_slice(&10_u16.to_le_bytes());
                webp[28..30].copy_from_slice(&20_u16.to_le_bytes());
            }
            assert!(dimensions_base64(&STANDARD.encode(webp)).is_some());
        }
        assert_eq!(dimensions_base64(&STANDARD.encode(b"\x89PNG\r\n")), None);
        let mut zero = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        zero.extend_from_slice(&0_u32.to_be_bytes());
        zero.extend_from_slice(&1_u32.to_be_bytes());
        assert_eq!(dimensions_base64(&STANDARD.encode(zero)), None);
    }
}
