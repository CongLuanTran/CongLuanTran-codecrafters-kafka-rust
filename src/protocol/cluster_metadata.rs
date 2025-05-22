use std::io::Read;

use anyhow::{bail, Ok};
use uuid::Uuid;

use super::primitive::{CompactString, Serializable, TagSection, UnsignedVarint, Varint};

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
struct Record<T: Serializable> {
    attributes: i8,
    timestamp_delta: Varint,
    offset_delta: Varint,
    key: Vec<u8>,
    value: T,
    headers: Vec<u8>,
}

impl<T: Serializable> Serializable for Record<T> {
    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn deserialize(bytes: &[u8]) -> anyhow::Result<(Self, &[u8])> {
        todo!()
    }
}

struct TopicRecord {
    pub frame_version: i8,
    pub record_type: i8,
    pub version: i8,
    pub name: String,
    pub uuid: Uuid,
    pub tag_buffer: TagSection,
}

impl Serializable for TopicRecord {
    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn deserialize(bytes: &[u8]) -> anyhow::Result<(Self, &[u8])> {
        let Some((&frame_version, bytes)) = bytes.split_first() else {
            bail!("Not enough bytes left");
        };
        let Some((&record_type, bytes)) = bytes.split_first() else {
            bail!("Not enough bytes left");
        };
        let Some((&version, bytes)) = bytes.split_first() else {
            bail!("Not enough bytes left");
        };
        let frame_version = frame_version as i8;
        let record_type = record_type as i8;
        let version = version as i8;

        let (name_length, bytes) = UnsignedVarint::deserialize(bytes)?;
        let Some((name, bytes)) = bytes.split_at_checked(*name_length as usize) else {
            bail!("Not enough bytes left");
        };
        let name = String::from_utf8_lossy(name).to_string();
        let Some((uiid, bytes)) = bytes.split_at_checked(16) else {
            bail!("Not enough bytes left");
        };
        let uuid = Uuid::from_bytes(uiid.try_into()?);
        let (tag_field_count, bytes) = UnsignedVarint::deserialize(bytes)?;
        Ok((
            Self {
                frame_version,
                record_type,
                version,
                name,
                uuid,
                tag_buffer: TagSection(None),
            },
            bytes,
        ))
    }
}
