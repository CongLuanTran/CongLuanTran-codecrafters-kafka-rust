use super::primitive::{CompactArray, CompactString, Serializable, TagSection};
use anyhow::{anyhow, Ok, Result};
use uuid::Uuid;

#[derive(Debug)]
pub struct TopicRequest {
    pub name: CompactString,
    pub tag_buffer: TagSection,
}

impl Serializable for TopicRequest {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.name.serialize());
        buf.extend(self.tag_buffer.serialize());
        buf
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (name, bytes) = CompactString::deserialize(bytes)?;
        let (tag_buffer, bytes) = TagSection::deserialize(bytes)?;
        Ok((TopicRequest { name, tag_buffer }, bytes))
    }
}

#[derive(Debug, Default)]
pub struct TopicResponse {
    pub error_code: i16,
    pub name: CompactString,
    pub topic_id: Uuid,
    pub is_internal: bool,
    pub partitions: CompactArray<Partition>,
    pub topic_authorized_operations: i32,
    pub tag_buffer: TagSection,
}

impl Serializable for TopicResponse {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.error_code.to_be_bytes());
        buf.extend(self.name.serialize());
        buf.extend(self.topic_id.as_bytes());
        buf.push(self.is_internal as u8);
        buf.extend(self.partitions.serialize());
        buf.extend(self.topic_authorized_operations.to_be_bytes());
        buf.extend(self.tag_buffer.serialize());
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        todo!()
    }
}

impl TopicResponse {
    pub fn unknown_topic(name: String) -> Self {
        TopicResponse {
            name: CompactString(Some(name)),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default)]
pub struct Partition {
    error_code: i16,
}

impl Serializable for Partition {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.error_code.to_be_bytes());
        buf
    }

    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (error_code, bytes) = bytes
            .split_at_checked(2)
            .ok_or(anyhow!("Error: not enough bytes left"))?;
        let error_code = i16::from_be_bytes(error_code.try_into()?);
        Ok((Partition { error_code }, bytes))
    }
}

#[derive(Debug)]
pub struct Cursor {
    topic_name: CompactString,
    partition_index: i32,
    tag_buffer: TagSection,
}

impl Serializable for Cursor {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.topic_name.serialize());
        buf.extend(self.partition_index.to_be_bytes());
        buf.extend(self.tag_buffer.serialize());
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (topic_name, bytes) = CompactString::deserialize(bytes)?;
        let (partition_index, bytes) = bytes
            .split_at_checked(4)
            .ok_or(anyhow!("Error: not enough bytes left"))?;
        let partition_index = i32::from_be_bytes(partition_index.try_into()?);
        let (tag_buffer, bytes) = TagSection::deserialize(bytes)?;
        Ok((
            Cursor {
                topic_name,
                partition_index,
                tag_buffer,
            },
            bytes,
        ))
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsRequest {
    pub topics: CompactArray<TopicRequest>,
    pub response_partition_limit: i32,
    pub cursor: Option<Cursor>,
    pub tag_buffer: TagSection,
}

impl Serializable for DescribeTopicPartitionsRequest {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.topics.serialize());
        buf.extend(self.response_partition_limit.to_be_bytes());
        match self.cursor {
            Some(ref cursor) => buf.extend(cursor.serialize()),
            None => buf.push(0xff),
        }
        buf.extend(self.tag_buffer.serialize());
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let (topics, bytes) = CompactArray::<TopicRequest>::deserialize(bytes)?;
        let (response_partition_limit, bytes) = bytes
            .split_at_checked(4)
            .ok_or(anyhow!("Error: not enough bytes left"))?;
        let response_partition_limit = i32::from_be_bytes(response_partition_limit.try_into()?);
        let (cursor, bytes) = if bytes[0] == 0xff {
            (None, &bytes[1..])
        } else {
            let (cursor, bytes) = Cursor::deserialize(&bytes[1..])?;
            (Some(cursor), bytes)
        };
        let (tag_buffer, bytes) = TagSection::deserialize(bytes)?;
        Ok((
            DescribeTopicPartitionsRequest {
                topics,
                response_partition_limit,
                cursor,
                tag_buffer,
            },
            bytes,
        ))
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsResponse {
    pub throttle_time: i32,
    pub topics: CompactArray<TopicResponse>,
    pub next_cursor: Option<Cursor>,
    pub tag_buffer: TagSection,
}

impl Serializable for DescribeTopicPartitionsResponse {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.throttle_time.to_be_bytes());
        buf.extend(self.topics.serialize());
        match self.next_cursor {
            Some(ref cursor) => buf.extend(cursor.serialize()),
            None => buf.push(0xff),
        }
        buf.extend(self.tag_buffer.serialize());
        buf
    }
    fn deserialize(bytes: &[u8]) -> Result<(Self, &[u8])> {
        todo!()
    }
}
