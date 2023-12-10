use bytes::Bytes;
use kafka_protocol::protocol::StrBytes;

pub(super) fn to_kafka_str(rust_string: &str) -> StrBytes {
    <string::String<bytes::Bytes> as string::TryFrom<Bytes>>::try_from(Bytes::copy_from_slice(rust_string.as_bytes())).unwrap()
}

pub(super) fn to_kafka_strs(rust_strings: &[String]) -> Vec<StrBytes> {
    rust_strings
        .iter()
        .map(|string_value| {
            to_kafka_str(string_value)
        })
        .collect()
}