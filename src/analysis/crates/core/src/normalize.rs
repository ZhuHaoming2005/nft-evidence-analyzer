//! Normalization shared with the snapshot exporter for live seed overlays.

use std::sync::LazyLock;

use regex::Regex;
use unicode_normalization::UnicodeNormalization;

static TRAILING_NAME_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"\s*#\s*[0-9a-fA-FxX]+\s*$").expect("valid name regex"),
        Regex::new(r"\s*#\s*\d+\s*$").expect("valid name regex"),
        Regex::new(r"\s*-\s*\d+\s*$").expect("valid name regex"),
        Regex::new(r"\s*:\s*\d+\s*$").expect("valid name regex"),
        Regex::new(r"\s*\(\s*\d+\s*\)\s*$").expect("valid name regex"),
        Regex::new(r"\s*\[\s*\d+\s*\]\s*$").expect("valid name regex"),
        Regex::new(r"\s*/\s*\d+\s*$").expect("valid name regex"),
        Regex::new(r"\s+No\.?\s*\d+\s*$").expect("valid name regex"),
        Regex::new(r"\s+nr\.?\s*\d+\s*$").expect("valid name regex"),
        Regex::new(r"\s+\d{1,12}\s*$").expect("valid name regex"),
    ]
});
static WHITESPACE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s+").expect("valid whitespace regex"));
static IPFS_HTTP_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^https?://[^/]+/ipfs/([A-Za-z0-9][^?#\s]*)").expect("valid IPFS gateway regex")
});
static ARWEAVE_HTTP_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^https?://(?:[^/]+\.)?arweave\.net/([A-Za-z0-9_-]{43}(?:/[^?#\s]*)?)")
        .expect("valid Arweave gateway regex")
});

pub fn normalize_name(raw: &str) -> String {
    let mut text = raw.nfkc().collect::<String>().trim().to_owned();
    for _ in 0..20 {
        let Some(updated) = TRAILING_NAME_PATTERNS.iter().find_map(|pattern| {
            let updated = pattern.replace(&text, "").trim().to_owned();
            (updated != text).then_some(updated)
        }) else {
            break;
        };
        text = updated;
    }
    WHITESPACE_RE.replace_all(&text, " ").trim().to_lowercase()
}

pub fn normalize_url(raw: &str) -> Option<String> {
    let text = raw.trim();
    if text.is_empty() {
        return None;
    }
    let lowered = text.to_lowercase();
    if matches!(
        lowered.as_str(),
        "nano" | "null" | "none" | "undefined" | "n/a" | "na" | "-" | "." | "false" | "true" | "0"
    ) || lowered.starts_with("data:")
    {
        return None;
    }
    if lowered.starts_with("ipfs://") {
        let mut tail = text[7..].to_owned();
        if tail.to_lowercase().starts_with("ipfs/") {
            tail = tail[5..].to_owned();
        }
        return normalized_content_url("ipfs:", &tail, true);
    }
    if lowered.starts_with("ar://") {
        return normalized_content_url("ar:", &text[5..], true);
    }
    if let Some(value) = IPFS_HTTP_RE
        .captures(text)
        .and_then(|captures| captures.get(1))
    {
        return normalized_content_url("ipfs:", value.as_str(), false);
    }
    if let Some(value) = ARWEAVE_HTTP_RE
        .captures(text)
        .and_then(|captures| captures.get(1))
    {
        return normalized_content_url("ar:", value.as_str(), false);
    }
    Some(lowered.trim_end_matches('/').to_owned())
}

fn normalized_content_url(prefix: &str, value: &str, trim_both_ends: bool) -> Option<String> {
    let value = value
        .split('?')
        .next()
        .unwrap_or_default()
        .split('#')
        .next()
        .unwrap_or_default();
    let value = if trim_both_ends {
        value.trim_matches('/')
    } else {
        value.trim_end_matches('/')
    };
    (!value.is_empty()).then(|| format!("{prefix}{value}"))
}
