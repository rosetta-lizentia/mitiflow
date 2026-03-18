use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identity for a publisher instance.
///
/// Wraps a UUID v7 to provide type safety — prevents accidentally mixing up
/// publisher IDs with event IDs or other UUIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PublisherId(Uuid);

impl PublisherId {
    /// Create a new random publisher ID (UUID v7 — time-ordered).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Create from an existing UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Encode as 16-byte big-endian representation for Zenoh attachments.
    pub fn to_bytes(&self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// Decode from 16-byte representation.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for PublisherId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PublisherId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for PublisherId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for PublisherId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Unique identity for an event.
///
/// Wraps a UUID v7 (time-ordered, globally unique).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(Uuid);

impl EventId {
    /// Create a new event ID (UUID v7).
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Create from an existing UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for EventId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for EventId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publisher_id_roundtrip_bytes() {
        let id = PublisherId::new();
        let bytes = id.to_bytes();
        let id2 = PublisherId::from_bytes(bytes);
        assert_eq!(id, id2);
    }

    #[test]
    fn publisher_id_roundtrip_string() {
        let id = PublisherId::new();
        let s = id.to_string();
        let id2: PublisherId = s.parse().unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn event_id_display_parse() {
        let id = EventId::new();
        let s = id.to_string();
        let id2: EventId = s.parse().unwrap();
        assert_eq!(id, id2);
    }
}
