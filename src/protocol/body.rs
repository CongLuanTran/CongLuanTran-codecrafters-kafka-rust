use super::{
    api_version::ApiVersionsResponse, describe_topic_partitions::DescribeTopicPartitionsResponse,
    primitive::Serializable,
};

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
    DescribeTopicPartitions(DescribeTopicPartitionsResponse),
}

impl ResponseBody {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersions(payload) => payload.serialize(),
            ResponseBody::DescribeTopicPartitions(payload) => payload.serialize(),
        }
    }
}
