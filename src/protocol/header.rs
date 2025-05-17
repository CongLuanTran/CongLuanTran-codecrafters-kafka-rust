#![allow(dead_code)]

#[derive(Debug)]
pub struct RequestHeader {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub tag_buffer: Vec<u8>,
}

#[derive(Debug)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ResponseHeader {
    pub fn serialize(&self) -> [u8; 4] {
        self.correlation_id.to_be_bytes()
    }
}

impl RequestHeader {
    pub fn deserialize(msg_buf: Vec<u8>) -> std::io::Result<Self> {
        // Initialize an offset, this will be incremented after the reading of each field
        let mut offset = 0;

        // Read the API key
        let request_api_key = i16::from_be_bytes(msg_buf[offset..offset + 2].try_into().unwrap());
        offset += 2;

        // Read the API version
        let request_api_version =
            i16::from_be_bytes(msg_buf[offset..offset + 2].try_into().unwrap());
        offset += 2;

        // Read the correlation id
        let correlation_id = i32::from_be_bytes(msg_buf[offset..offset + 4].try_into().unwrap());
        offset += 4;

        // Initialize the client id to a null string
        let mut client_id: Option<String> = None;
        // Then read the first two bytes which indicate the length of the string
        let len_client_id =
            i16::from_be_bytes(msg_buf[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        match len_client_id {
            // If the length N is positive then read the next N bytes as client id
            n if n > 0 => {
                client_id = Some(
                    String::from_utf8_lossy(&msg_buf[offset..offset + len_client_id]).to_string(),
                );
                offset += len_client_id;
            }
            // Else do nothing (in the documentation, null string is indicated by a -1 in the length
            // field)
            _ => {}
        }

        // For now, don't care about parsing the tag buffer
        let tag_buffer = msg_buf[offset..].to_vec();

        Ok(RequestHeader {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
            tag_buffer,
        })
    }
}
