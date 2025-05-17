use super::{apiversion::ApiVersionsResponse, primitive::Serializable};

#[derive(Debug)]
pub enum ResponseBody {
    ApiVersions(ApiVersionsResponse),
}

impl ResponseBody {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            ResponseBody::ApiVersions(payload) => payload.serialize(),
        }
    }
}
