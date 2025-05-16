use crate::protocol::primitive::UnsignedVarint;

#[derive(Debug)]
pub struct ApiVersionResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
    pub tag_buffer: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub tag_buffer: Option<Vec<u8>>,
}

impl ApiVersion {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.api_key.to_be_bytes());
        buf.extend(self.min_version.to_be_bytes());
        buf.extend(self.max_version.to_be_bytes());
        match &self.tag_buffer {
            Some(bytes) => {
                buf.extend(UnsignedVarint(bytes.len() as u32 + 1).encode());
                buf.extend(bytes);
            }
            None => {
                buf.extend(UnsignedVarint(0).encode());
            }
        };
        buf
    }
}

impl ApiVersionResponse {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.error_code.to_be_bytes());
        buf.extend(UnsignedVarint(self.api_keys.len() as u32 + 1).encode());
        for api_key in &self.api_keys {
            buf.extend(api_key.to_be_bytes());
        }
        buf.extend(self.throttle_time_ms.to_be_bytes());
        match &self.tag_buffer {
            Some(bytes) => {
                buf.extend(UnsignedVarint(bytes.len() as u32 + 1).encode());
                buf.extend(bytes);
            }
            None => {
                buf.extend(UnsignedVarint(0).encode());
            }
        };
        buf
    }
}
