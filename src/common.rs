use bytes::BytesMut;

pub trait Serializable {
    fn serialize(&self, buf: &mut BytesMut);
}

pub trait Deserializable {
    fn deserialize(frame: &[u8]) -> Self;
}
