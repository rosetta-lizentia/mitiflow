use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::types::EventId;

/// Generic event envelope carrying typed payload with metadata.
///
/// `Event<T>` is the unit of data in mitiflow. Each event gets a globally unique,
/// time-ordered UUID v7 ID and a UTC timestamp on creation.
///
/// The `seq` field is populated by the publisher when the event is sent, and
/// `key_expr` is populated by the subscriber when the event is received.
/// Neither should be set manually by application code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<T: Serialize> {
    /// Globally unique, time-ordered event identifier.
    pub id: EventId,
    /// Wall-clock timestamp when the event was created.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Sequence number assigned by the publisher (set during publish).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// Application payload.
    pub payload: T,
    /// Key expression on which the event was received (set by subscriber).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_expr: Option<String>,
}

impl<T: Serialize> Event<T> {
    /// Create a new event wrapping the given payload.
    ///
    /// Generates a UUID v7 ID and captures the current UTC time.
    pub fn new(payload: T) -> Self {
        Self {
            id: EventId::new(),
            timestamp: chrono::Utc::now(),
            seq: None,
            payload,
            key_expr: None,
        }
    }
}

impl<T: Serialize + DeserializeOwned> Event<T> {
    /// Serialize this event to JSON bytes.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    /// Deserialize an event from JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}

/// Received event with raw (unparsed) payload bytes.
///
/// This avoids double-deserialization when the subscriber pipeline needs to
/// inspect metadata before deciding whether to deserialize the full payload.
#[derive(Debug, Clone)]
pub struct RawEvent {
    /// Event ID extracted from the deserialized envelope.
    pub id: EventId,
    /// Sequence number from the Zenoh attachment.
    pub seq: u64,
    /// Publisher that sent this event.
    pub publisher_id: crate::types::PublisherId,
    /// Key expression the event was received on.
    pub key_expr: String,
    /// Raw JSON bytes of the full `Event<T>` (including payload).
    pub payload: Vec<u8>,
    /// Timestamp from the event envelope.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl RawEvent {
    /// Extract the application key from the key expression.
    ///
    /// Returns `Some(key)` for keyed events (`{prefix}/p/{partition}/k/{key}/{seq}`),
    /// `None` for unkeyed events.
    pub fn key(&self) -> Option<&str> {
        crate::attachment::extract_key(&self.key_expr)
    }

    /// Deserialize the payload into a typed `Event<T>` using the default codec.
    pub fn deserialize<T: Serialize + DeserializeOwned>(&self) -> crate::Result<Event<T>> {
        self.deserialize_with(crate::codec::CodecFormat::default())
    }

    /// Deserialize the payload into a typed `Event<T>` using the given codec.
    ///
    /// The payload bytes contain only the serialized `T` — all envelope
    /// metadata (`id`, `timestamp`, `seq`, `key_expr`) is taken from the
    /// `RawEvent` fields that were populated from the Zenoh attachment.
    pub fn deserialize_with<T: Serialize + DeserializeOwned>(
        &self,
        codec: crate::codec::CodecFormat,
    ) -> crate::Result<Event<T>> {
        let payload: T = codec.decode(&self.payload)?;
        Ok(Event {
            id: self.id,
            timestamp: self.timestamp,
            seq: Some(self.seq),
            payload,
            key_expr: Some(self.key_expr.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PublisherId;

    fn make_raw(key_expr: &str) -> RawEvent {
        RawEvent {
            id: EventId::new(),
            seq: 0,
            publisher_id: PublisherId::new(),
            key_expr: key_expr.to_string(),
            payload: vec![],
            timestamp: chrono::Utc::now(),
        }
    }

    #[test]
    fn raw_event_key_keyed() {
        let raw = make_raw("app/p/0/k/order-123/5");
        assert_eq!(raw.key(), Some("order-123"));
    }

    #[test]
    fn raw_event_key_hierarchical() {
        let raw = make_raw("app/p/2/k/user/42/orders/5");
        assert_eq!(raw.key(), Some("user/42/orders"));
    }

    #[test]
    fn raw_event_key_unkeyed() {
        let raw = make_raw("app/p/0/5");
        assert_eq!(raw.key(), None);
    }

    #[test]
    fn raw_event_key_no_p() {
        let raw = make_raw("app/events/5");
        assert_eq!(raw.key(), None);
    }
}
