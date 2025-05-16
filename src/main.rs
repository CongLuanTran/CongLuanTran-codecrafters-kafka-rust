#![allow(unused_imports)]
#![allow(dead_code)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

#[derive(Debug)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    tag_buffer: Vec<u8>,
}

#[derive(Debug)]
struct ResponseHeader {
    correlation_id: i32,
}

#[derive(Debug)]
struct ApiVersionResponse {
    error_code: i16,
    api_keys: Vec<ApiVersion>,
}

#[derive(Debug)]
struct ApiVersion {
    api_key: i16,
    min_version: i16,
    max_version: i16,
}

impl ApiVersion {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.api_key.to_be_bytes());
        buf.extend(self.min_version.to_be_bytes());
        buf.extend(self.max_version.to_be_bytes());
        buf
    }
}

impl ApiVersionResponse {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.error_code.to_be_bytes());
        buf.extend((self.api_keys.len() as i32).to_be_bytes());
        for api_key in &self.api_keys {
            buf.extend(api_key.to_be_bytes());
        }
        buf
    }
}

impl ResponseHeader {
    fn to_be_bytes(&self) -> [u8; 4] {
        self.correlation_id.to_be_bytes()
    }
}

#[derive(Debug)]
struct Response {
    header: ResponseHeader,
    body: ApiVersionResponse,
}

impl Response {
    fn to_be_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(self.header.to_be_bytes());
        buf.extend(self.body.to_be_bytes());
        buf
    }
}

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    // First read the message size
    let mut size_buf = [0; 4];
    stream.read_exact(&mut size_buf)?;
    let message_size = i32::from_be_bytes(size_buf) as usize;

    // Then read the rest of the message into a buffer
    let mut msg_buf = vec![0; message_size];
    stream.read_exact(&mut msg_buf)?;

    // Parse the request
    let request = parse_request(msg_buf).unwrap();

    // Send response, this is very unstructured for now, will be refactored later
    let correlation_id: i32 = request.correlation_id;

    let response_header = ResponseHeader { correlation_id };
    let response_body = ApiVersionResponse {
        error_code: 0,
        api_keys: vec![ApiVersion {
            api_key: 18,
            min_version: 0,
            max_version: 4,
        }],
    };
    let response = Response {
        header: response_header,
        body: response_body,
    };

    let payload = response.to_be_bytes();
    let message_size: i32 = payload.len() as i32;
    stream.write_all(&message_size.to_be_bytes())?;
    if request.request_api_key == 18 {
        stream.write_all(&payload)?;
    }
    Ok(())
}

fn parse_request(msg_buf: Vec<u8>) -> std::io::Result<RequestHeader> {
    // Initialize an offset, this will be incremented after the reading of each field
    let mut offset = 0;

    // Read the API key
    let request_api_key = i16::from_be_bytes(msg_buf[offset..offset + 2].try_into().unwrap());
    offset += 2;

    // Read the API version
    let request_api_version = i16::from_be_bytes(msg_buf[offset..offset + 2].try_into().unwrap());
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
            client_id =
                Some(String::from_utf8_lossy(&msg_buf[offset..offset + len_client_id]).to_string());
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

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(e) = handle_connection(stream) {
                    eprintln!("Failed to write to client: {e}");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
