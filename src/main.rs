#![allow(unused_imports)]
use anyhow::Result;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use codecrafters_kafka::protocol::{
    api_version::{ApiVersion, ApiVersionsRequest, ApiVersionsResponse},
    body::ResponseBody,
    describe_topic_partitions::{
        DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, TopicResponse,
    },
    header::{RequestHeader, ResponseHeader, ResponseHeaderV0, ResponseHeaderV1},
    primitive::{CompactArray, Serializable, TagSection},
    response::Response,
};

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    if let Err(e) = process_connection(&mut stream) {
        eprintln!("Connection closed or error: {e}");
    }
    Ok(())
}

fn process_connection(stream: &mut TcpStream) -> Result<()> {
    let mut size_buf = [0; 4];

    loop {
        // Early return if reading message size fails
        if let Err(e) = stream.read_exact(&mut size_buf) {
            return Err(anyhow::anyhow!(e));
        }

        // Read the exact number of bytes specified by the size
        let message_size = i32::from_be_bytes(size_buf) as usize;
        let mut msg_buf = vec![0; message_size];
        stream.read_exact(&mut msg_buf)?;

        // Parse the message
        let (request_header, request_body) = RequestHeader::deserialize(&msg_buf)?;
        let correlation_id: i32 = request_header.correlation_id;

        // Select the appropriate response based on the API key
        let response = match request_header.request_api_key {
            18 => ApiVersionsRequest::handle_request(correlation_id, request_header),
            75 => {
                let (request_body, _bytes) =
                    DescribeTopicPartitionsRequest::deserialize(request_body)?;
                request_body.handle_request(correlation_id)
            }
            _ => None,
        };

        if let Some(value) = response {
            let payload = value.to_be_bytes();
            let message_size: i32 = payload.len() as i32;
            stream.write_all(&message_size.to_be_bytes())?;
            stream.write_all(&payload)?;
        }
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Spawn a new thread for each individual connection
                thread::spawn(|| {
                    // Each thread handle one connection until the client terminate the connection
                    if let Err(e) = handle_connection(stream) {
                        eprintln!("Failed to write to client: {e}");
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
