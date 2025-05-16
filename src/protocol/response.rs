use crate::protocol::apiversion::ApiVersionResponse;
use crate::protocol::header::ResponseHeader;

#[derive(Debug)]
pub struct Response {
    pub header: ResponseHeader,
    pub body: ApiVersionResponse,
}

impl Response {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.header.to_be_bytes());
        buf.extend(self.body.to_be_bytes());
        buf
    }
}
