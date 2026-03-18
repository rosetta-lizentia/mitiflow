//! Zenoh attachment encoding/decoding for event metadata.
//!
//! Sequence numbers and publisher IDs are sent via Zenoh's binary attachment
//! mechanism rather than serialized inside the event body. This keeps metadata
//! on the zero-copy path and avoids adding to the serde cost.
//!
//! # Wire format (24 bytes)
//!
//! ```text
//! [ seq: u64 BE (8 bytes) | publisher_id: UUID (16 bytes) ]
//! ```

use zenoh::bytes::ZBytes;

use crate::error::{Error, Result};
use crate::types::PublisherId;

const ATTACHMENT_SIZE: usize = 8 + 16; // u64 + UUID

/// Encode a sequence number and publisher ID into a Zenoh attachment.
pub fn encode_metadata(pub_id: &PublisherId, seq: u64) -> ZBytes {
    let mut buf = [0u8; ATTACHMENT_SIZE];
    buf[..8].copy_from_slice(&seq.to_be_bytes());
    buf[8..].copy_from_slice(&pub_id.to_bytes());
    ZBytes::from(buf.to_vec())
}

/// Decode a sequence number and publisher ID from a Zenoh attachment.
pub fn decode_metadata(attachment: &ZBytes) -> Result<(PublisherId, u64)> {
    let bytes = attachment.to_bytes();
    if bytes.len() != ATTACHMENT_SIZE {
        return Err(Error::InvalidAttachment(format!(
            "expected {} bytes, got {}",
            ATTACHMENT_SIZE,
            bytes.len()
        )));
    }

    let seq = u64::from_be_bytes(
        bytes[..8]
            .try_into()
            .map_err(|e| Error::InvalidAttachment(format!("seq decode: {e}")))?,
    );

    let pub_id_bytes: [u8; 16] = bytes[8..24]
        .try_into()
        .map_err(|e| Error::InvalidAttachment(format!("pub_id decode: {e}")))?;

    Ok((PublisherId::from_bytes(pub_id_bytes), seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let pub_id = PublisherId::new();
        let seq = 42u64;
        let encoded = encode_metadata(&pub_id, seq);
        let (decoded_id, decoded_seq) = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded_id, pub_id);
        assert_eq!(decoded_seq, seq);
    }

    #[test]
    fn roundtrip_max_seq() {
        let pub_id = PublisherId::new();
        let seq = u64::MAX;
        let encoded = encode_metadata(&pub_id, seq);
        let (decoded_id, decoded_seq) = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded_id, pub_id);
        assert_eq!(decoded_seq, seq);
    }

    #[test]
    fn invalid_length() {
        let zbytes = ZBytes::from(vec![0u8; 10]);
        assert!(decode_metadata(&zbytes).is_err());
    }
}
