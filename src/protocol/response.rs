use super::{body::ResponseBody, header::ResponseHeader};

#[derive(Debug)]
pub struct Response {
    pub header: ResponseHeader,
    pub body: ResponseBody,
}

impl Response {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.header.serialize());
        buf.extend(self.body.serialize());
        buf
    }
}
