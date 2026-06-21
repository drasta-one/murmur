use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
// As a placeholder, we might use MessagePayload directly later

/// A length-prefixed frame codec for postcard.
pub struct PostcardCodec;

impl PostcardCodec {
    pub fn encode<T: Serialize>(item: &T, dst: &mut BytesMut) -> anyhow::Result<()> {
        let serialized = postcard::to_allocvec(item)?;
        let len = serialized.len() as u32;

        dst.reserve(4 + serialized.len());
        dst.put_u32(len);
        dst.put_slice(&serialized);

        Ok(())
    }

    pub fn decode<T: for<'de> Deserialize<'de>>(src: &mut BytesMut) -> anyhow::Result<Option<T>> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[0..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // We have enough data
        src.advance(4);
        let data = src.split_to(length);

        let item: T = postcard::from_bytes(&data)?;
        Ok(Some(item))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use murmur_core::net::NetMessage;

    #[test]
    fn test_postcard_codec_encode_decode() {
        let mut buffer = BytesMut::new();

        let msg = NetMessage::HeartbeatPing;
        PostcardCodec::encode(&msg, &mut buffer).unwrap();

        // Length prefix is u32 (4 bytes), plus data
        assert!(buffer.len() > 4);

        let decoded = PostcardCodec::decode::<NetMessage>(&mut buffer).unwrap();
        assert!(decoded.is_some());

        match decoded.unwrap() {
            NetMessage::HeartbeatPing => {}
            _ => panic!("Decoded wrong message type"),
        }
    }

    #[test]
    fn test_postcard_codec_partial_buffer() {
        let mut buffer = BytesMut::new();

        let msg = NetMessage::HeartbeatPing;
        PostcardCodec::encode(&msg, &mut buffer).unwrap();

        // Truncate buffer to simulate partial read
        let mut partial_buffer = buffer.split_to(2);
        let decoded = PostcardCodec::decode::<NetMessage>(&mut partial_buffer).unwrap();
        assert!(decoded.is_none());
    }
}
