//! Query filter parsing for event store queries.

use chrono::{DateTime, Utc};

use crate::error::{Error, Result};
use crate::types::PublisherId;

use super::backend::HlcTimestamp;

/// Structured filters for querying the event store.
///
/// Parsed from Zenoh selector parameters, e.g.:
/// `myapp/store/**?after_seq=100&before_seq=200&limit=50`
#[derive(Debug, Clone, Default)]
pub struct QueryFilters {
    /// Only return events with seq > this value.
    pub after_seq: Option<u64>,
    /// Only return events with seq < this value.
    pub before_seq: Option<u64>,
    /// Only return events with timestamp after this.
    pub after_time: Option<DateTime<Utc>>,
    /// Only return events with timestamp before this.
    pub before_time: Option<DateTime<Utc>>,
    /// Only return events from this publisher.
    pub publisher_id: Option<PublisherId>,
    /// Maximum number of results.
    pub limit: Option<usize>,
}

impl QueryFilters {
    /// Parse filters from a Zenoh selector parameter string.
    ///
    /// Expected format: `key1=value1;key2=value2` (Zenoh uses `;` as separator).
    /// Also accepts `&` as separator for compatibility.
    pub fn from_selector(params: &str) -> Result<Self> {
        let mut filters = Self::default();

        for part in params.split([';', '&']) {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((key, value)) = part.split_once('=') {
                match key.trim() {
                    "after_seq" => {
                        filters.after_seq = Some(value.trim().parse::<u64>().map_err(|e| {
                            Error::InvalidConfig(format!("invalid after_seq: {e}"))
                        })?);
                    }
                    "before_seq" => {
                        filters.before_seq = Some(value.trim().parse::<u64>().map_err(|e| {
                            Error::InvalidConfig(format!("invalid before_seq: {e}"))
                        })?);
                    }
                    "after_time" => {
                        filters.after_time =
                            Some(value.trim().parse::<DateTime<Utc>>().map_err(|e| {
                                Error::InvalidConfig(format!("invalid after_time: {e}"))
                            })?);
                    }
                    "before_time" => {
                        filters.before_time =
                            Some(value.trim().parse::<DateTime<Utc>>().map_err(|e| {
                                Error::InvalidConfig(format!("invalid before_time: {e}"))
                            })?);
                    }
                    "limit" => {
                        filters.limit =
                            Some(value.trim().parse::<usize>().map_err(|e| {
                                Error::InvalidConfig(format!("invalid limit: {e}"))
                            })?);
                    }
                    "publisher_id" => {
                        let uuid = uuid::Uuid::parse_str(value.trim()).map_err(|e| {
                            Error::InvalidConfig(format!("invalid publisher_id: {e}"))
                        })?;
                        filters.publisher_id = Some(PublisherId::from_uuid(uuid));
                    }
                    _ => { /* ignore unknown parameters */ }
                }
            }
        }
        Ok(filters)
    }
}

/// Filters for HLC-ordered replay queries.
///
/// Returns events sorted by `(hlc_timestamp, publisher_id, seq)` for
/// deterministic cross-replica replay.
#[derive(Debug, Clone, Default)]
pub struct ReplayFilters {
    /// Only return events with HLC timestamp strictly after this value.
    pub after_hlc: Option<HlcTimestamp>,
    /// Only return events with HLC timestamp strictly before this value.
    pub before_hlc: Option<HlcTimestamp>,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Only return events with this exact application key.
    pub key: Option<String>,
    /// Only return events whose application key starts with this prefix.
    pub key_prefix: Option<String>,
}

impl ReplayFilters {
    /// Parse replay filters from a Zenoh selector parameter string.
    ///
    /// Recognised parameters:
    /// - `after_hlc={physical_ns}:{logical}` — HLC lower bound (exclusive)
    /// - `before_hlc={physical_ns}:{logical}` — HLC upper bound (exclusive)
    /// - `limit={n}` — maximum number of results
    /// - `key={exact_key}` — exact application key filter
    /// - `key_prefix={prefix}` — application key prefix filter
    pub fn from_selector(params: &str) -> Result<Self> {
        let mut filters = Self::default();

        for part in params.split([';', '&']) {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((k, v)) = part.split_once('=') {
                match k.trim() {
                    "after_hlc" => {
                        filters.after_hlc = Some(parse_hlc(v.trim())?);
                    }
                    "before_hlc" => {
                        filters.before_hlc = Some(parse_hlc(v.trim())?);
                    }
                    "limit" => {
                        filters.limit =
                            Some(v.trim().parse::<usize>().map_err(|e| {
                                Error::InvalidConfig(format!("invalid limit: {e}"))
                            })?);
                    }
                    "key" => {
                        filters.key = Some(v.trim().to_string());
                    }
                    "key_prefix" => {
                        filters.key_prefix = Some(v.trim().to_string());
                    }
                    _ => { /* ignore unknown parameters */ }
                }
            }
        }
        Ok(filters)
    }

    /// Returns `true` if key-scoped filtering is active.
    pub fn has_key_filter(&self) -> bool {
        self.key.is_some() || self.key_prefix.is_some()
    }

    /// Check whether the given application key matches the filter.
    pub fn matches_key(&self, app_key: Option<&str>) -> bool {
        if let Some(exact) = &self.key {
            return app_key == Some(exact.as_str());
        }
        if let Some(prefix) = &self.key_prefix {
            return app_key.is_some_and(|k| k.starts_with(prefix.as_str()));
        }
        true // no key filter → matches everything
    }
}

/// Parse an HLC timestamp from `"{physical_ns}:{logical}"` format.
fn parse_hlc(s: &str) -> Result<HlcTimestamp> {
    let (phys_str, log_str) = s.split_once(':').ok_or_else(|| {
        Error::InvalidConfig(format!(
            "invalid HLC format (expected physical:logical): {s}"
        ))
    })?;
    let physical_ns = phys_str
        .parse::<u64>()
        .map_err(|e| Error::InvalidConfig(format!("invalid HLC physical_ns: {e}")))?;
    let logical = log_str
        .parse::<u32>()
        .map_err(|e| Error::InvalidConfig(format!("invalid HLC logical: {e}")))?;
    Ok(HlcTimestamp {
        physical_ns,
        logical,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_seq_range() {
        let f = QueryFilters::from_selector("after_seq=10;before_seq=20").unwrap();
        assert_eq!(f.after_seq, Some(10));
        assert_eq!(f.before_seq, Some(20));
    }

    #[test]
    fn parse_with_ampersand() {
        let f = QueryFilters::from_selector("after_seq=5&limit=100").unwrap();
        assert_eq!(f.after_seq, Some(5));
        assert_eq!(f.limit, Some(100));
    }

    #[test]
    fn parse_empty() {
        let f = QueryFilters::from_selector("").unwrap();
        assert!(f.after_seq.is_none());
        assert!(f.limit.is_none());
    }

    #[test]
    fn parse_invalid_number() {
        assert!(QueryFilters::from_selector("after_seq=abc").is_err());
    }

    // --- ReplayFilters tests ---

    #[test]
    fn replay_filters_from_selector_with_key() {
        let f = ReplayFilters::from_selector("key=order-123&after_hlc=1000:0&limit=50").unwrap();
        assert_eq!(f.key, Some("order-123".to_string()));
        assert_eq!(
            f.after_hlc,
            Some(HlcTimestamp {
                physical_ns: 1000,
                logical: 0
            })
        );
        assert_eq!(f.limit, Some(50));
        assert!(f.key_prefix.is_none());
        assert!(f.has_key_filter());
    }

    #[test]
    fn replay_filters_from_selector_with_key_prefix() {
        let f = ReplayFilters::from_selector("key_prefix=user/42&limit=100").unwrap();
        assert_eq!(f.key_prefix, Some("user/42".to_string()));
        assert_eq!(f.limit, Some(100));
        assert!(f.key.is_none());
        assert!(f.has_key_filter());
    }

    #[test]
    fn replay_filters_from_selector_hlc_format() {
        let f = ReplayFilters::from_selector("after_hlc=1711843200000000000:5").unwrap();
        let hlc = f.after_hlc.unwrap();
        assert_eq!(hlc.physical_ns, 1711843200000000000);
        assert_eq!(hlc.logical, 5);
    }

    #[test]
    fn replay_filters_from_selector_before_hlc() {
        let f = ReplayFilters::from_selector("after_hlc=100:0&before_hlc=200:3").unwrap();
        assert_eq!(
            f.after_hlc,
            Some(HlcTimestamp {
                physical_ns: 100,
                logical: 0
            })
        );
        assert_eq!(
            f.before_hlc,
            Some(HlcTimestamp {
                physical_ns: 200,
                logical: 3
            })
        );
    }

    #[test]
    fn replay_filters_empty() {
        let f = ReplayFilters::from_selector("").unwrap();
        assert!(f.after_hlc.is_none());
        assert!(f.before_hlc.is_none());
        assert!(f.limit.is_none());
        assert!(f.key.is_none());
        assert!(f.key_prefix.is_none());
        assert!(!f.has_key_filter());
    }

    #[test]
    fn replay_filters_invalid_hlc_format() {
        assert!(ReplayFilters::from_selector("after_hlc=notanumber").is_err());
        assert!(ReplayFilters::from_selector("after_hlc=100").is_err()); // missing :logical
    }

    #[test]
    fn replay_filters_matches_key_exact() {
        let f = ReplayFilters {
            key: Some("order-A".to_string()),
            ..Default::default()
        };
        assert!(f.matches_key(Some("order-A")));
        assert!(!f.matches_key(Some("order-B")));
        assert!(!f.matches_key(None));
    }

    #[test]
    fn replay_filters_matches_key_prefix() {
        let f = ReplayFilters {
            key_prefix: Some("user/1".to_string()),
            ..Default::default()
        };
        assert!(f.matches_key(Some("user/1/orders")));
        assert!(f.matches_key(Some("user/1")));
        assert!(!f.matches_key(Some("user/2/orders")));
        assert!(!f.matches_key(None));
    }

    #[test]
    fn replay_filters_no_filter_matches_all() {
        let f = ReplayFilters::default();
        assert!(f.matches_key(Some("anything")));
        assert!(f.matches_key(None));
    }
}
