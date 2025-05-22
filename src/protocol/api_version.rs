use crate::protocol::{
    body::ResponseBody,
    header::{ResponseHeader, ResponseHeaderV0},
    response::Response,
};

use super::{
    header::RequestHeader,
    primitive::{Serializable, TagSection, UnsignedVarint},
};

pub struct ApiVersionsRequest;

impl ApiVersionsRequest {
    const SUPPORTED_API: [ApiVersion; 2] = [
        ApiVersion {
            api_key: 18,
            min_version: 0,
            max_version: 4,
            tag_buffer: TagSection(None),
        },
        ApiVersion {
            api_key: 75,
            min_version: 0,
            max_version: 0,
            tag_buffer: TagSection(None),
        },
    ];

    pub fn handle_request(correlation_id: i32, request_header: RequestHeader) -> Option<Response> {
        let response_header = ResponseHeader::V0(ResponseHeaderV0 { correlation_id });
        let (error_code, api_keys): (i16, &[ApiVersion]) = match request_header.request_api_version
        {
            4 => (0, &Self::SUPPORTED_API),
            _ => (35, &[]),
        };

        let response_body = ResponseBody::ApiVersions(ApiVersionsResponse {
            error_code,
            api_keys,
            throttle_time_ms: 0,
            tag_buffer: TagSection(None),
        });
        Some(Response {
            header: response_header,
            body: response_body,
        })
    }
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: &'static [ApiVersion],
    pub throttle_time_ms: i32,
    pub tag_buffer: TagSection,
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: TagSection,
}

impl Serializable for ApiVersion {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.api_key.to_be_bytes());
        buf.extend(self.min_version.to_be_bytes());
        buf.extend(self.max_version.to_be_bytes());
        buf.extend(self.tag_buffer.serialize());
        buf
    }
    fn deserialize(bytes: &[u8]) -> anyhow::Result<(Self, &[u8])> {
        todo!()
    }
}

impl Serializable for ApiVersionsResponse {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.error_code.to_be_bytes());
        buf.extend(UnsignedVarint(self.api_keys.len() as u32 + 1).serialize());
        for api_key in self.api_keys {
            buf.extend(api_key.serialize());
        }
        buf.extend(self.throttle_time_ms.to_be_bytes());
        buf.extend(self.tag_buffer.serialize());
        buf
    }
    fn deserialize(bytes: &[u8]) -> anyhow::Result<(Self, &[u8])> {
        todo!()
    }
}
