//! Zenoh attachment encoding/decoding for event metadata.
//!
//! All per-event metadata is carried in a fixed 48-byte binary attachment so
//! the payload body contains only the user's serialized `T`. This keeps every
//! metadata field on the zero-copy path and removes the need to decode the
//! payload just to read `id` or `timestamp`.
//!
//! # Wire format (48 bytes)
//!
//! ```text
//! [ seq: u64 BE (8) | publisher_id: UUID (16) | event_id: UUID (16) | timestamp_ns: i64 BE (8) ]
//! ```

use zenoh::bytes::ZBytes;

use crate::error::{Error, Result};
use crate::types::{EventId, PublisherId};

const ATTACHMENT_SIZE: usize = 8 + 16 + 16 + 8; // 48 bytes

/// All per-event metadata decoded from a Zenoh attachment.
#[derive(Debug, Clone)]
pub struct EventMeta {
    /// Monotonic per-publisher sequence number.
    pub seq: u64,
    /// Identity of the publisher that created this event.
    pub pub_id: PublisherId,
    /// Globally unique event identifier (UUID v7).
    pub event_id: EventId,
    /// Wall-clock time when the event was created (UTC).
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Encode per-event metadata into a Zenoh attachment.
pub fn encode_metadata(
    pub_id: &PublisherId,
    seq: u64,
    event_id: &EventId,
    timestamp: &chrono::DateTime<chrono::Utc>,
) -> ZBytes {
    let mut buf = [0u8; ATTACHMENT_SIZE];
    buf[0..8].copy_from_slice(&seq.to_be_bytes());
    buf[8..24].copy_from_slice(&pub_id.to_bytes());
    buf[24..40].copy_from_slice(&event_id.to_bytes());
    buf[40..48].copy_from_slice(
        &timestamp
            .timestamp_nanos_opt()
            .unwrap_or(0)
            .to_be_bytes(),
    );
    ZBytes::from(buf.to_vec())
}

/// Read a big-endian u64 from `bytes` at the given offset.
fn read_u64(bytes: &[u8], offset: usize, field: &str) -> Result<u64> {
    let slice: [u8; 8] = bytes[offset..offset + 8]
        .try_into()
        .map_err(|e| Error::InvalidAttachment(format!("{field} decode: {e}")))?;
    Ok(u64::from_be_bytes(slice))
}

/// Read a 16-byte array from `bytes` at the given offset.
fn read_uuid_bytes(bytes: &[u8], offset: usize, field: &str) -> Result<[u8; 16]> {
    bytes[offset..offset + 16]
        .try_into()
        .map_err(|e| Error::InvalidAttachment(format!("{field} decode: {e}")))
}

/// Decode per-event metadata from a Zenoh attachment.
pub fn decode_metadata(attachment: &ZBytes) -> Result<EventMeta> {
    let bytes = attachment.to_bytes();
    if bytes.len() != ATTACHMENT_SIZE {
        return Err(Error::InvalidAttachment(format!(
            "expected {} bytes, got {}",
            ATTACHMENT_SIZE,
            bytes.len()
        )));
    }

    let seq = read_u64(&bytes, 0, "seq")?;
    let pub_id_bytes = read_uuid_bytes(&bytes, 8, "pub_id")?;
    let event_id_bytes = read_uuid_bytes(&bytes, 24, "event_id")?;
    let timestamp_ns = read_u64(&bytes, 40, "timestamp")? as i64;
    let timestamp = chrono::DateTime::from_timestamp_nanos(timestamp_ns);

    Ok(EventMeta {
        seq,
        pub_id: PublisherId::from_bytes(pub_id_bytes),
        event_id: EventId::from_bytes(event_id_bytes),
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let pub_id = PublisherId::new();
        let event_id = EventId::new();
        let seq = 42u64;
        let ts = chrono::Utc::now();
        let encoded = encode_metadata(&pub_id, seq, &event_id, &ts);
        let meta = decode_metadata(&encoded).unwrap();
        assert_eq!(meta.pub_id, pub_id);
        assert_eq!(meta.seq, seq);
        assert_eq!(meta.event_id, event_id);
        // Timestamp round-trips to nanosecond precision.
        assert_eq!(
            meta.timestamp.timestamp_nanos_opt(),
            ts.timestamp_nanos_opt()
        );
    }

    #[test]
    fn roundtrip_max_seq() {
        let pub_id = PublisherId::new();
        let event_id = EventId::new();
        let seq = u64::MAX;
        let ts = chrono::Utc::now();
        let encoded = encode_metadata(&pub_id, seq, &event_id, &ts);
        let meta = decode_metadata(&encoded).unwrap();
        assert_eq!(meta.seq, seq);
        assert_eq!(meta.pub_id, pub_id);
        assert_eq!(meta.event_id, event_id);
    }

    #[test]
    fn invalid_length() {
        let zbytes = ZBytes::from(vec![0u8; 10]);
        assert!(decode_metadata(&zbytes).is_err());
    }
}
