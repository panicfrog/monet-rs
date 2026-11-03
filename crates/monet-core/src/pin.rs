//! Public-key (SPKI) pinning: config parsing and host matching.
//!
//! One implementation for every transport (the lesson from nitro-fetch, which
//! copies this logic three times across Kotlin and Swift). Config format is
//! `nitro-fetch-pinning.json`:
//!
//! ```json
//! {
//!   "api.example.com": {
//!     "publicKeyHashes": ["<base64 SHA-256 of the SPKI>", "..."],
//!     "includeSubdomains": false,
//!     "expirationDate": "2027-01-01"
//!   }
//! }
//! ```
//!
//! Parsing is strict and **fail-closed**: a broken config is an error, never
//! a silent "no pins". Host matching replicates Cronet's rule exactly
//! (cross-checked against nitro-fetch's `CertificatePinning.kt`): walk up
//! from the full host label by label; the *first live* entry decides — it
//! applies only on an exact match or with `includeSubdomains`; otherwise the
//! host is unpinned even if a parent entry would match.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

/// A UTC calendar date, day-granular, comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Date {
    days: i64, // days since 1970-01-01 UTC
}

impl Date {
    /// Builds a date, validating month/day ranges (leap years included).
    /// Years outside 1..=9999 are rejected.
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Option<Date> {
        if !(1..=9999).contains(&year) || !(1..=12).contains(&month) {
            return None;
        }
        if day == 0 || day > days_in_month(year, month) {
            return None;
        }
        Some(Date {
            days: days_from_civil(year, month, day),
        })
    }

    /// Today's UTC date.
    pub fn today_utc() -> Date {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before 1970")
            .as_secs() as i64;
        Date {
            days: secs.div_euclid(86_400),
        }
    }

    /// Parses a strict `YYYY-MM-DD` UTC date.
    pub fn parse(s: &str) -> Option<Date> {
        let bytes = s.as_bytes();
        if bytes.len() != 10
            || bytes[4] != b'-'
            || bytes[7] != b'-'
            || !bytes
                .iter()
                .enumerate()
                .all(|(i, b)| matches!(i, 4 | 7) || b.is_ascii_digit())
        {
            return None;
        }
        let year = s[0..4].parse::<i32>().ok()?;
        let month = s[5..7].parse::<u32>().ok()?;
        let day = s[8..10].parse::<u32>().ok()?;
        Date::from_ymd(year, month, day)
    }
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Howard Hinnant's `days_from_civil`.
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = i64::from(if m <= 2 { y - 1 } else { y });
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (i64::from(m) + 9) % 12; // [0, 11], March = 0
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// A pin entry: one host's pinned SPKI hashes and options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinEntry {
    /// Lowercased host, no trailing dot.
    pub host: String,
    /// Decoded SHA-256 hashes of pinned SubjectPublicKeyInfos.
    pub hashes: Vec<[u8; 32]>,
    /// Whether the entry also covers subdomains.
    pub include_subdomains: bool,
    /// Last day the entry is enforced (exclusive at its UTC midnight,
    /// matching Cronet's `expiration` semantics).
    pub expiration: Date,
}

/// A parsed pinning configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PinConfig {
    entries: Vec<PinEntry>,
}

/// Failures while parsing a pinning config. All of them mean "refuse to run
/// without pins", never "carry on unpinned".
#[derive(Debug, thiserror::Error)]
pub enum PinError {
    /// The file is not valid JSON, or not a top-level object.
    #[error("invalid pinning config: {0}")]
    Json(String),
    /// A host key is not a valid hostname.
    #[error("invalid pinning config: {0}")]
    Host(String),
    /// A hash entry is not a base64 SHA-256.
    #[error("invalid pinning config: {0}")]
    Hash(String),
    /// A date is malformed or not a real calendar date.
    #[error("invalid pinning config: {0}")]
    Date(String),
    /// The same (normalized) host appears twice.
    #[error("invalid pinning config: {0}")]
    Duplicate(String),
    /// `publicKeyHashes` is missing, not an array, or empty.
    #[error("invalid pinning config: {0}")]
    Hashes(String),
}

/// Cronet's no-expiration stand-in: pins stay enforced until the app ships
/// new ones.
fn no_expiration() -> Date {
    Date::from_ymd(2100, 1, 1).expect("2100-01-01 is a valid date")
}

impl PinConfig {
    /// Parses and validates a config. An empty object yields an empty (and
    /// therefore permissive) config — that is "pinning off by choice", which
    /// is distinct from a *broken* config.
    pub fn from_json(json: &str) -> Result<PinConfig, PinError> {
        let value: serde_json::Value = serde_json::from_str(json)
            .map_err(|e| PinError::Json(format!("not valid JSON ({e})")))?;
        let object = value
            .as_object()
            .ok_or_else(|| PinError::Json("expected a JSON object keyed by hostname".into()))?;

        let mut entries: Vec<PinEntry> = Vec::new();
        for (key, entry_value) in object {
            let host = normalize_host(key)?;
            if entries.iter().any(|e| e.host == host) {
                return Err(PinError::Duplicate(format!(
                    "\"{key}\" lists a host that is already in the file"
                )));
            }
            entries.push(parse_entry(&host, key, entry_value)?);
        }
        Ok(PinConfig { entries })
    }

    /// All entries, in file order.
    pub fn entries(&self) -> &[PinEntry] {
        &self.entries
    }

    /// Returns `true` when no hosts are pinned.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether `host` is covered by any live entry (today, UTC).
    pub fn is_pinned(&self, host: &str) -> bool {
        self.pins_for_host(host).is_some()
    }

    /// The entry whose pins apply to `host`, or `None` when unpinned.
    /// Uses today's UTC date.
    pub fn pins_for_host(&self, host: &str) -> Option<&PinEntry> {
        self.pins_for_host_at(host, Date::today_utc())
    }

    /// [`PinConfig::pins_for_host`] with an injectable "today" for tests.
    pub fn pins_for_host_at(&self, host: &str, today: Date) -> Option<&PinEntry> {
        let mut candidate = normalize_query_host(host);
        let mut exact = true;
        loop {
            // The first live entry decides; an expired entry does not stop
            // the walk — a live parent may still cover the host (Cronet's
            // rule, nitro's activePins).
            if let Some(entry) = self
                .entries
                .iter()
                .find(|e| e.host == candidate && e.expiration > today)
            {
                return (exact || entry.include_subdomains).then_some(entry);
            }
            match candidate.split_once('.') {
                Some((_, parent)) => {
                    candidate = parent.to_string();
                    exact = false;
                }
                None => return None,
            }
        }
    }
}

fn parse_entry(host: &str, key: &str, value: &serde_json::Value) -> Result<PinEntry, PinError> {
    let object = value
        .as_object()
        .ok_or_else(|| PinError::Host(format!("\"{key}\" must be an object")))?;

    let mut hashes: Option<Vec<[u8; 32]>> = None;
    let mut include_subdomains = false;
    let mut expiration = no_expiration();

    for (field, field_value) in object {
        match field.as_str() {
            "publicKeyHashes" => {
                let array = field_value.as_array().ok_or_else(|| {
                    PinError::Hashes(format!(
                        "\"{key}\" needs at least one entry in publicKeyHashes"
                    ))
                })?;
                let mut decoded = Vec::with_capacity(array.len());
                for item in array {
                    let raw = item.as_str().unwrap_or_default();
                    decoded.push(decode_hash(raw, key)?);
                }
                hashes = Some(decoded);
            }
            "includeSubdomains" => {
                include_subdomains = field_value.as_bool().ok_or_else(|| {
                    PinError::Host(format!("\"{key}\".includeSubdomains must be true or false"))
                })?;
            }
            "expirationDate" => {
                let raw = field_value.as_str().unwrap_or_default();
                expiration = Date::parse(raw).ok_or_else(|| {
                    PinError::Date(format!("\"{key}\".expirationDate must be yyyy-MM-dd"))
                })?;
            }
            // Unknown fields are tolerated, like nitro's reader.skipValue().
            _ => {}
        }
    }

    let hashes = hashes.ok_or_else(|| {
        PinError::Hashes(format!(
            "\"{key}\" needs at least one entry in publicKeyHashes"
        ))
    })?;
    if hashes.is_empty() {
        return Err(PinError::Hashes(format!(
            "\"{key}\" needs at least one entry in publicKeyHashes"
        )));
    }

    Ok(PinEntry {
        host: host.to_string(),
        hashes,
        include_subdomains,
        expiration,
    })
}

/// A padded base64 SHA-256 hash is always 43 alphabet characters plus '='.
fn decode_hash(raw: &str, key: &str) -> Result<[u8; 32], PinError> {
    let valid_shape = raw.len() == 44
        && raw.as_bytes()[43] == b'='
        && raw.as_bytes()[..43]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'+' || *b == b'/');
    if !valid_shape {
        return Err(PinError::Hash(format!(
            "\"{key}\" has a publicKeyHashes entry that isn't a base64 SHA-256 hash: {raw}"
        )));
    }
    let decoded = BASE64
        .decode(raw)
        .map_err(|_| PinError::Hash(format!("\"{key}\" hash is not valid base64: {raw}")))?;
    decoded
        .try_into()
        .map_err(|_| PinError::Hash(format!("\"{key}\" hash is not 32 bytes: {raw}")))
}

/// Normalizes a config key: lowercase, strip the trailing dot, then validate
/// as a hostname (no scheme, port, path, wildcard, or IP; punycode for IDNs).
fn normalize_host(key: &str) -> Result<String, PinError> {
    let host = key.to_ascii_lowercase();
    let host = host.strip_suffix('.').unwrap_or(&host);
    if !is_valid_hostname(host) {
        return Err(PinError::Host(format!(
            "\"{key}\" is not a hostname (no scheme, port, path, wildcard or IP; use punycode for IDNs)"
        )));
    }
    Ok(host.to_string())
}

fn normalize_query_host(host: &str) -> String {
    let host = host.to_ascii_lowercase();
    host.strip_suffix('.').unwrap_or(&host).to_string()
}

fn is_valid_hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    // Numeric hosts (IPv4-shaped) are rejected; IPv6 never matches the
    // label rules anyway (colons).
    if host.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return false;
    }
    host.split('.').all(|label| {
        (1..=63).contains(&label.len())
            && label
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A real base64 SHA-256 (43 chars + '='), valid shape for tests.
    const HASH: &str = "r2Kn1DbzEZBROoLdOZLRIyD0WfTGWhCsFlusjMO6Zjo=";

    fn entry_json() -> String {
        format!(
            r#"{{"api.example.com": {{"publicKeyHashes": ["{HASH}"], "includeSubdomains": true, "expirationDate": "2099-01-01"}}}}"#
        )
    }

    fn today() -> Date {
        Date::from_ymd(2026, 11, 3).unwrap()
    }

    #[test]
    fn parses_full_entry() {
        let config = PinConfig::from_json(&entry_json()).unwrap();
        assert_eq!(config.entries().len(), 1);
        let e = &config.entries()[0];
        assert_eq!(e.host, "api.example.com");
        assert_eq!(e.hashes.len(), 1);
        assert_eq!(e.hashes[0].len(), 32);
        assert!(e.include_subdomains);
        assert_eq!(e.expiration, Date::from_ymd(2099, 1, 1).unwrap());
    }

    #[test]
    fn defaults_and_unknown_fields() {
        let config = PinConfig::from_json(&format!(
            r#"{{"example.com": {{"publicKeyHashes": ["{HASH}"], "futureField": 42}}}}"#
        ))
        .unwrap();
        let e = &config.entries()[0];
        assert!(!e.include_subdomains);
        assert_eq!(e.expiration, no_expiration()); // 2100-01-01
    }

    #[test]
    fn empty_object_is_permissive() {
        let config = PinConfig::from_json("{}").unwrap();
        assert!(config.is_empty());
        assert!(!config.is_pinned("api.example.com"));
    }

    // --- host matching (nitro/Cronet semantics) ---

    fn config(entries: &[(&str, bool, Option<&str>)]) -> PinConfig {
        let json = entries
            .iter()
            .map(|(host, sub, exp)| {
                let exp = match exp {
                    Some(d) => format!("\"expirationDate\": \"{d}\", "),
                    None => String::new(),
                };
                format!(
                    "\"{host}\": {{\"publicKeyHashes\": [\"{HASH}\"], {exp}\"includeSubdomains\": {sub}}}"
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        PinConfig::from_json(&format!("{{{json}}}")).unwrap()
    }

    #[test]
    fn exact_match_applies() {
        let c = config(&[("api.example.com", false, None)]);
        assert!(c.pins_for_host_at("api.example.com", today()).is_some());
        // trailing dot + case are normalized on the query host too
        assert!(c.pins_for_host_at("API.Example.COM.", today()).is_some());
    }

    #[test]
    fn subdomains_need_the_flag() {
        let c = config(&[("example.com", false, None)]);
        assert!(c.pins_for_host_at("api.example.com", today()).is_none());
        let c = config(&[("example.com", true, None)]);
        assert!(c.pins_for_host_at("api.example.com", today()).is_some());
        assert!(c.pins_for_host_at("a.b.example.com", today()).is_some());
    }

    #[test]
    fn closer_non_subdomain_entry_shields_parent() {
        // Entry a.b.com exists without includeSubdomains; parent b.com has
        // it. A subdomain of the closer entry is NOT pinned — the first live
        // entry on the walk decides, and it declines.
        let c = config(&[("a.b.com", false, None), ("b.com", true, None)]);
        assert!(c.pins_for_host_at("x.a.b.com", today()).is_none());
        // ...but the closer entry still applies to its own host exactly
        assert!(c.pins_for_host_at("a.b.com", today()).is_some());
        // ...and the parent covers hosts outside the closer entry
        assert!(c.pins_for_host_at("x.b.com", today()).is_some());
        assert!(c.pins_for_host_at("b.com", today()).is_some());
    }

    #[test]
    fn expired_entry_does_not_stop_the_walk() {
        // api.example.com expired; example.com (parent, includeSubdomains) live.
        let c = config(&[
            ("api.example.com", true, Some("2020-01-01")),
            ("example.com", true, None),
        ]);
        assert_eq!(
            c.pins_for_host_at("api.example.com", today()).unwrap().host,
            "example.com"
        );
    }

    #[test]
    fn expired_exact_entry_is_fully_dead() {
        let c = config(&[("api.example.com", true, Some("2020-01-01"))]);
        assert!(c.pins_for_host_at("api.example.com", today()).is_none());
    }

    #[test]
    fn expiration_boundary_is_exclusive() {
        let c = config(&[("example.com", true, Some("2026-11-03"))]);
        // still live the day before, dead on its own date (UTC midnight)
        assert!(
            c.pins_for_host_at("example.com", Date::from_ymd(2026, 11, 2).unwrap())
                .is_some()
        );
        assert!(c.pins_for_host_at("example.com", today()).is_none());
    }

    #[test]
    fn unpinned_host_returns_none() {
        let c = config(&[("example.com", true, None)]);
        assert!(c.pins_for_host_at("other.org", today()).is_none());
        assert!(
            c.pins_for_host_at("example.com.evil.org", today())
                .is_none()
        );
    }

    // --- validation (fail-closed) ---

    fn assert_host_err(json: &str) {
        let err = PinConfig::from_json(json).unwrap_err();
        assert!(matches!(err, PinError::Host(_)), "got: {err:?}");
    }

    #[test]
    fn rejects_bad_hosts() {
        for key in [
            "https://x.com",
            "x.com:8080",
            "*.x.com",
            "1.2.3.4",
            "a..b.com",
            "-a.com",
            "a-.com",
            "naïve.com",
        ] {
            assert_host_err(&format!(r#"{{"{key}": {}}}"#, hash_entry()));
        }
        let long_label = format!("{}.com", "a".repeat(64));
        assert_host_err(&format!(r#"{{"{long_label}": {}}}"#, hash_entry()));
    }

    fn hash_entry() -> String {
        format!(r#"{{"publicKeyHashes": ["{HASH}"]}}"#)
    }

    #[test]
    fn duplicate_hosts_rejected_after_normalization() {
        let err = PinConfig::from_json(&format!(
            "{{\"Example.com.\": {}, \"example.com\": {}}}",
            hash_entry(),
            hash_entry()
        ))
        .unwrap_err();
        assert!(matches!(err, PinError::Duplicate(_)));
    }

    #[test]
    fn rejects_bad_hashes() {
        let cases = vec![
            String::new(),
            "short".to_string(),
            "A".repeat(43),               // missing padding
            format!("{}%", &HASH[..43]),  // invalid char
            format!("{}==", &HASH[..42]), // wrong padding
        ];
        for bad in &cases {
            let json = format!(r#"{{"x.com": {{"publicKeyHashes": ["{bad}"]}}}}"#);
            let err = PinConfig::from_json(&json).unwrap_err();
            assert!(matches!(err, PinError::Hash(_)), "got: {err:?}");
        }
    }

    #[test]
    fn rejects_missing_or_empty_or_nonarray_hashes() {
        let err = PinConfig::from_json(r#"{"x.com": {}}"#).unwrap_err();
        assert!(matches!(err, PinError::Hashes(_)));
        let err = PinConfig::from_json(r#"{"x.com": {"publicKeyHashes": []}}"#).unwrap_err();
        assert!(matches!(err, PinError::Hashes(_)));
        let err =
            PinConfig::from_json(r#"{"x.com": {"publicKeyHashes": "not-an-array"}}"#).unwrap_err();
        assert!(matches!(err, PinError::Hashes(_)));
    }

    #[test]
    fn rejects_bad_dates_and_wrong_types() {
        for bad in ["2027-02-30", "2027-13-01", "2027-1-1", "not-a-date", ""] {
            let json = format!(
                r#"{{"x.com": {{"publicKeyHashes": ["{HASH}"], "expirationDate": "{bad}"}}}}"#
            );
            let err = PinConfig::from_json(&json).unwrap_err();
            assert!(matches!(err, PinError::Date(_)), "got: {err:?}");
        }
        let json = format!(
            r#"{{"x.com": {{"publicKeyHashes": ["{HASH}"], "expirationDate": 20270101}}}}"#
        );
        assert!(matches!(
            PinConfig::from_json(&json).unwrap_err(),
            PinError::Date(_)
        ));
        let json = format!(
            r#"{{"x.com": {{"publicKeyHashes": ["{HASH}"], "includeSubdomains": "yes"}}}}"#
        );
        assert!(matches!(
            PinConfig::from_json(&json).unwrap_err(),
            PinError::Host(_)
        ));
    }

    #[test]
    fn rejects_broken_json_and_non_object() {
        assert!(matches!(
            PinConfig::from_json("not json").unwrap_err(),
            PinError::Json(_)
        ));
        assert!(matches!(
            PinConfig::from_json(r#"["a"]"#).unwrap_err(),
            PinError::Json(_)
        ));
    }

    // --- Date ---

    #[test]
    fn date_validation() {
        assert!(Date::from_ymd(2024, 2, 29).is_some()); // leap
        assert!(Date::from_ymd(2023, 2, 29).is_none());
        assert!(Date::from_ymd(2027, 2, 30).is_none());
        assert!(Date::from_ymd(2027, 12, 31).is_some());
        assert!(Date::from_ymd(2027, 0, 1).is_none());
        assert!(Date::parse("2026-11-03").is_some());
        assert!(Date::parse("2026-11-3").is_none());
        assert!(Date::parse("26-11-03").is_none());
    }

    #[test]
    fn date_epoch_roundtrip_ordering() {
        assert_eq!(Date::from_ymd(1970, 1, 1).unwrap().days, 0);
        assert!(Date::from_ymd(1970, 1, 2).unwrap() > Date::from_ymd(1970, 1, 1).unwrap());
        // 56 years (14 leap days) + 306 days into 2026
        assert_eq!(Date::from_ymd(2026, 11, 3).unwrap().days, 20_760);
    }
}
