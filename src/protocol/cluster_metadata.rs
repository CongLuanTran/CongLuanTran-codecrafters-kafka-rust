use super::primitive::Varint;

#[derive(Debug)]
struct RecordBatch {
    partition_leader_epoch: i32,
    magic_byte: i8,
    crc: i32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records_length: i32,
}

#[derive(Debug)]
struct Record {
    attributes: i8,
    timestamp_delta: Varint,
    offset_delta: Varint,
    key: Vec<u8>,
    headers: Vec<u8>,
}
