//! Zenoh attachment encoding/decoding for event metadata.
//!
//! All per-event metadata is carried in a fixed 50-byte binary attachment so
//! the payload body contains only the user's serialized `T`. This keeps every
//! metadata field on the zero-copy path and removes the need to decode the
//! payload just to read `id` or `timestamp`.
//!
//! # Wire format (50 bytes)
//!
//! ```text
//! [ seq: u64 BE (8) | publisher_id: UUID (16) | event_id: UUID (16) | timestamp_ns: i64 BE (8) | urgency_ms: u16 BE (2) ]
//! ```
//!
//! The partition is encoded in the Zenoh key expression (topic segment), not
//! in the attachment, so subscribers can filter by partition natively.
//!
//! `urgency_ms` is the publisher's requested latency budget for durability
//! confirmation, in milliseconds. `0` means broadcast the watermark
//! immediately. `0xFFFF` (`NO_URGENCY`) means no urgency — use the store's
//! default watermark interval.

use zenoh::bytes::ZBytes;

use crate::error::{Error, Result};
use crate::types::{EventId, PublisherId};

const ATTACHMENT_SIZE: usize = 8 + 16 + 16 + 8 + 2; // 50 bytes

/// Sentinel value: no urgency — use the store's default watermark interval.
pub const NO_URGENCY: u16 = u16::MAX; // 0xFFFF

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
    /// Requested durability latency budget in milliseconds.
    /// `0` = broadcast watermark immediately.
    /// [`NO_URGENCY`] (`0xFFFF`) = use the store's default watermark interval.
    pub urgency_ms: u16,
}

/// Encode per-event metadata into a Zenoh attachment.
pub fn encode_metadata(
    pub_id: &PublisherId,
    seq: u64,
    event_id: &EventId,
    timestamp: &chrono::DateTime<chrono::Utc>,
    urgency_ms: u16,
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
    buf[48..50].copy_from_slice(&urgency_ms.to_be_bytes());
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
    let urgency_ms = u16::from_be_bytes(
        bytes[48..50]
            .try_into()
            .map_err(|e| Error::InvalidAttachment(format!("urgency_ms decode: {e}")))?,
    );

    Ok(EventMeta {
        seq,
        pub_id: PublisherId::from_bytes(pub_id_bytes),
        event_id: EventId::from_bytes(event_id_bytes),
        timestamp,
        urgency_ms,
    })
}

/// Extract the partition number from a Zenoh key expression.
///
/// Expected format: `{prefix}/p/{partition}/{tail}`.
/// Returns `0` when no `/p/` segment is found (non-partitioned mode).
pub fn extract_partition(key: &str) -> u32 {
    // Look for "/p/" segment and parse the u32 after it.
    if let Some(idx) = key.find("/p/") {
        let rest = &key[idx + 3..];
        if let Some(end) = rest.find('/') {
            rest[..end].parse().unwrap_or(0)
        } else {
            rest.parse().unwrap_or(0)
        }
    } else {
        0
    }
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
        let encoded = encode_metadata(&pub_id, seq, &event_id, &ts, NO_URGENCY);
        let meta = decode_metadata(&encoded).unwrap();
        assert_eq!(meta.pub_id, pub_id);
        assert_eq!(meta.seq, seq);
        assert_eq!(meta.event_id, event_id);
        assert_eq!(meta.urgency_ms, NO_URGENCY);
        // Timestamp round-trips to nanosecond precision.
        assert_eq!(
            meta.timestamp.timestamp_nanos_opt(),
            ts.timestamp_nanos_opt()
        );
    }

    #[test]
    fn roundtrip_with_urgency() {
        let pub_id = PublisherId::new();
        let event_id = EventId::new();
        let seq = 7u64;
        let ts = chrono::Utc::now();
        let encoded = encode_metadata(&pub_id, seq, &event_id, &ts, 500);
        let meta = decode_metadata(&encoded).unwrap();
        assert_eq!(meta.seq, seq);
        assert_eq!(meta.pub_id, pub_id);
        assert_eq!(meta.urgency_ms, 500);
    }

    #[test]
    fn roundtrip_no_urgency() {
        let pub_id = PublisherId::new();
        let event_id = EventId::new();
        let ts = chrono::Utc::now();
        let encoded = encode_metadata(&pub_id, 1, &event_id, &ts, NO_URGENCY);
        let meta = decode_metadata(&encoded).unwrap();
        assert_eq!(meta.urgency_ms, NO_URGENCY);
    }

    #[test]
    fn roundtrip_immediate_urgency() {
        let pub_id = PublisherId::new();
        let event_id = EventId::new();
        let ts = chrono::Utc::now();
        let encoded = encode_metadata(&pub_id, 1, &event_id, &ts, 0);
        let meta = decode_metadata(&encoded).unwrap();
        assert_eq!(meta.urgency_ms, 0); // 0 = broadcast immediately
    }

    #[test]
    fn roundtrip_max_seq() {
        let pub_id = PublisherId::new();
        let event_id = EventId::new();
        let seq = u64::MAX;
        let ts = chrono::Utc::now();
        let encoded = encode_metadata(&pub_id, seq, &event_id, &ts, NO_URGENCY);
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

    #[test]
    fn extract_partition_from_key() {
        assert_eq!(extract_partition("myapp/events/p/3/42"), 3);
        assert_eq!(extract_partition("myapp/events/p/0/some/key"), 0);
        assert_eq!(extract_partition("myapp/events/p/255/data"), 255);
    }

    #[test]
    fn extract_partition_missing_segment() {
        // No /p/ segment → defaults to 0 (non-partitioned mode).
        assert_eq!(extract_partition("myapp/events/42"), 0);
        assert_eq!(extract_partition("topic/data"), 0);
    }

    #[test]
    fn extract_partition_invalid_number() {
        // Non-numeric partition → defaults to 0.
        assert_eq!(extract_partition("myapp/events/p/abc/42"), 0);
    }
}
