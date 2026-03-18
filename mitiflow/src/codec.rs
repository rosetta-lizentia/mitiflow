//! Pluggable encoding/decoding for event payloads.
//!
//! Provides a [`CodecFormat`] enum for selecting the wire format of events.
//! The default format is JSON via `serde_json`.

use serde::{Serialize, de::DeserializeOwned};

use crate::error::Result;

/// Encoding format for event payloads.
///
/// Select the serialization format used for encoding and decoding events
/// on the wire. Defaults to [`CodecFormat::Json`].
///
/// # Example
///
/// ```rust
/// use mitiflow::codec::CodecFormat;
///
/// let codec = CodecFormat::Json;
/// let data = vec![1u32, 2, 3];
/// let bytes = codec.encode(&data).unwrap();
/// let decoded: Vec<u32> = codec.decode(&bytes).unwrap();
/// assert_eq!(data, decoded);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub enum CodecFormat {
    /// JSON encoding via `serde_json`.
    #[default]
    Json,
}

impl CodecFormat {
    /// Encode a serializable value into bytes.
    pub fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        match self {
            CodecFormat::Json => serde_json::to_vec(value).map_err(Into::into),
        }
    }

    /// Decode bytes into a deserializable value.
    pub fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        match self {
            CodecFormat::Json => serde_json::from_slice(bytes).map_err(Into::into),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_codec_roundtrip() {
        let codec = CodecFormat::Json;
        let original = vec![1u32, 2, 3];
        let bytes = codec.encode(&original).unwrap();
        let decoded: Vec<u32> = codec.decode(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn json_codec_struct() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Foo {
            x: i32,
            y: String,
        }

        let codec = CodecFormat::Json;
        let original = Foo {
            x: 42,
            y: "hello".to_string(),
        };
        let bytes = codec.encode(&original).unwrap();
        let decoded: Foo = codec.decode(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn json_codec_decode_error() {
        let codec = CodecFormat::Json;
        let result = codec.decode::<Vec<u32>>(b"not json");
        assert!(result.is_err());
    }
}
