//! Query filter parsing for event store queries.

use chrono::{DateTime, Utc};

use crate::error::{Error, Result};

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

        for part in params.split(|c| c == ';' || c == '&') {
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
                    _ => { /* ignore unknown parameters */ }
                }
            }
        }
        Ok(filters)
    }
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
}
