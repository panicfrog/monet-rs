//! WHATWG-style header map.
//!
//! Semantics mirror the WHATWG `Headers` interface (cross-checked against
//! nitro-fetch's `src/Headers.ts`): names are normalized to lowercase, each
//! name holds an ordered list of values, `get` joins them with `", "`, and
//! iteration follows first-insertion order. Unlike the TS version, invalid
//! names and values are rejected with errors instead of throwing — header
//! strings can arrive from JS at runtime, so this must never panic.

use indexmap::IndexMap;

use crate::error::{Error, Result};

/// An ordered, case-insensitive multi-map of HTTP headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeaderMap {
    map: IndexMap<String, Vec<String>>,
}

impl HeaderMap {
    /// Creates an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a value to the entry for `name`, creating it if absent.
    ///
    /// Names are lowercased; values are trimmed of surrounding spaces/tabs
    /// and rejected if they contain NUL, CR, or LF.
    pub fn append(&mut self, name: &str, value: impl Into<String>) -> Result<()> {
        let name = normalize_name(name)?;
        let value = normalize_value(value.into())?;
        self.map.entry(name).or_default().push(value);
        Ok(())
    }

    /// Replaces the entry for `name` with a single value. Keeps the position
    /// of an existing entry (WHATWG `set` semantics).
    pub fn set(&mut self, name: &str, value: impl Into<String>) -> Result<()> {
        let name = normalize_name(name)?;
        let value = normalize_value(value.into())?;
        self.map.insert(name, vec![value]);
        Ok(())
    }

    /// Returns the values for `name` joined with `", "`, or `None`.
    ///
    /// This is lossy for `Set-Cookie` (whose values may contain commas);
    /// use [`HeaderMap::get_set_cookie`] for that header.
    pub fn get(&self, name: &str) -> Option<String> {
        self.get_all(name).map(|values| values.join(", "))
    }

    /// Returns all values for `name`, in append order.
    pub fn get_all(&self, name: &str) -> Option<&[String]> {
        self.map.get(&name.to_ascii_lowercase()).map(Vec::as_slice)
    }

    /// Returns all `Set-Cookie` values (unjoined), like the WHATWG
    /// `Headers.getSetCookie()` special case.
    pub fn get_set_cookie(&self) -> &[String] {
        self.get_all("set-cookie").unwrap_or(&[])
    }

    /// Returns `true` if `name` is present.
    pub fn has(&self, name: &str) -> bool {
        self.map.contains_key(&name.to_ascii_lowercase())
    }

    /// Removes the entry for `name`.
    pub fn remove(&mut self, name: &str) {
        self.map.shift_remove(&name.to_ascii_lowercase());
    }

    /// Iterates `(name, values)` in first-insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[String])> {
        self.map.iter().map(|(k, v)| (k.as_str(), v.as_slice()))
    }

    /// Iterates `(name, value)` pairs with values flattened, preserving
    /// order. This is the shape transports need for the wire.
    pub fn iter_flattened(&self) -> impl Iterator<Item = (&str, &str)> {
        self.map
            .iter()
            .flat_map(|(k, values)| values.iter().map(move |v| (k.as_str(), v.as_str())))
    }

    /// Number of distinct header names.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if there are no headers.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<'a> IntoIterator for &'a HeaderMap {
    type Item = (&'a str, &'a [String]);
    type IntoIter = Box<dyn Iterator<Item = (&'a str, &'a [String])> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// RFC 7230 token: visible ASCII except separators.
fn is_valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn normalize_name(name: &str) -> Result<String> {
    if !is_valid_name(name) {
        return Err(Error::InvalidHeaderName(name.to_string()));
    }
    Ok(name.to_ascii_lowercase())
}

fn normalize_value(value: String) -> Result<String> {
    let trimmed = value.trim_matches(|c| c == ' ' || c == '\t');
    if trimmed
        .bytes()
        .any(|b| b == 0x00 || b == b'\r' || b == b'\n')
    {
        return Err(Error::InvalidHeaderValue(value));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_is_case_insensitive_and_ordered() {
        let mut h = HeaderMap::new();
        h.append("Content-Type", "application/json").unwrap();
        h.append("content-type", "charset=utf-8").unwrap();
        h.append("X-B", "2").unwrap();
        h.append("X-A", "1").unwrap();
        assert_eq!(
            h.get("CONTENT-TYPE").unwrap(),
            "application/json, charset=utf-8"
        );
        // iteration follows first-insertion order of names
        let names: Vec<&str> = h.iter().map(|(k, _)| k).collect();
        assert_eq!(names, ["content-type", "x-b", "x-a"]);
    }

    #[test]
    fn set_replaces_in_place() {
        let mut h = HeaderMap::new();
        h.append("a", "1").unwrap();
        h.append("b", "2").unwrap();
        h.append("a", "3").unwrap();
        h.set("A", "x").unwrap();
        assert_eq!(h.get("a").unwrap(), "x");
        let names: Vec<&str> = h.iter().map(|(k, _)| k).collect();
        assert_eq!(names, ["a", "b"]); // position kept
    }

    #[test]
    fn set_cookie_is_not_joined() {
        let mut h = HeaderMap::new();
        h.append("Set-Cookie", "a=1; Path=/").unwrap();
        h.append("set-cookie", "b=2; Path=/, Max-Age=0").unwrap();
        assert_eq!(h.get_set_cookie().len(), 2);
        assert_eq!(h.get_set_cookie()[1], "b=2; Path=/, Max-Age=0");
    }

    #[test]
    fn flattened_pairs_group_by_name() {
        let mut h = HeaderMap::new();
        h.append("x", "1").unwrap();
        h.append("y", "a").unwrap();
        h.append("x", "2").unwrap();
        // Values are grouped under their name (WHATWG Headers cannot
        // reconstruct cross-name append order either).
        let pairs: Vec<(&str, &str)> = h.iter_flattened().collect();
        assert_eq!(pairs, [("x", "1"), ("x", "2"), ("y", "a")]);
    }

    #[test]
    fn invalid_names_rejected() {
        let mut h = HeaderMap::new();
        assert!(h.append("bad name", "v").is_err());
        assert!(h.append("a:b", "v").is_err());
        assert!(h.append("", "v").is_err());
        assert!(h.append("naïve", "v").is_err()); // non-ASCII
    }

    #[test]
    fn invalid_values_rejected_and_trimmed() {
        let mut h = HeaderMap::new();
        assert!(h.append("x", "a\r\nb").is_err());
        assert!(h.append("x", "a\0b").is_err());
        h.append("x", "  v \t").unwrap();
        assert_eq!(h.get("x").unwrap(), "v");
    }

    #[test]
    fn remove_works() {
        let mut h = HeaderMap::new();
        h.append("x", "1").unwrap();
        h.remove("X");
        assert!(!h.has("x"));
        assert!(h.get("x").is_none());
    }
}
