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
    #[default]
    /// JSON encoding via `serde_json`.
    Json,
    /// MessagePack encoding via `rmp-serde`.
    MsgPack,
    /// Postcard encoding via `postcard`.
    Postcard,
}

impl CodecFormat {
    /// Encode a serializable value into bytes.
    pub fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        match self {
            CodecFormat::Json => serde_json::to_vec(value).map_err(Into::into),
            CodecFormat::MsgPack => rmp_serde::to_vec(value).map_err(Into::into),
            CodecFormat::Postcard => postcard::to_allocvec(value).map_err(Into::into),
        }
    }

    /// Decode bytes into a deserializable value.
    pub fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        match self {
            CodecFormat::Json => serde_json::from_slice(bytes).map_err(Into::into),
            CodecFormat::MsgPack => rmp_serde::from_slice(bytes).map_err(Into::into),
            CodecFormat::Postcard => postcard::from_bytes(bytes).map_err(Into::into),
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
    fn msgpack_codec_roundtrip() {
        let codec = CodecFormat::MsgPack;
        let original = vec![1u32, 2, 3];
        let bytes = codec.encode(&original).unwrap();
        let decoded: Vec<u32> = codec.decode(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn msgpack_codec_struct() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Foo {
            x: i32,
            y: String,
        }

        let codec = CodecFormat::MsgPack;
        let original = Foo {
            x: 42,
            y: "hello".to_string(),
        };
        let bytes = codec.encode(&original).unwrap();
        let decoded: Foo = codec.decode(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn postcard_codec_roundtrip() {
        let codec = CodecFormat::Postcard;
        let original = vec![1u32, 2, 3];
        let bytes = codec.encode(&original).unwrap();
        let decoded: Vec<u32> = codec.decode(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn postcard_codec_struct() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Foo {
            x: i32,
            y: String,
        }

        let codec = CodecFormat::Postcard;
        let original = Foo {
            x: 42,
            y: "hello".to_string(),
        };
        let bytes = codec.encode(&original).unwrap();
        let decoded: Foo = codec.decode(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn codecs_produce_distinct_bytes() {
        let data = vec![1u32, 2, 3];
        let json = CodecFormat::Json.encode(&data).unwrap();
        let msgpack = CodecFormat::MsgPack.encode(&data).unwrap();
        let postcard = CodecFormat::Postcard.encode(&data).unwrap();
        assert_ne!(json, msgpack);
        assert_ne!(json, postcard);
        assert_ne!(msgpack, postcard);
    }
}
