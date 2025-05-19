#![allow(unused_imports)]
use anyhow::Result;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use codecrafters_kafka::protocol::{
    api_version::{ApiVersion, ApiVersionsResponse},
    body::ResponseBody,
    describe_topic_partitions::{
        DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, TopicResponse,
    },
    header::{RequestHeader, ResponseHeader, ResponseHeaderV0, ResponseHeaderV1},
    primitive::{CompactArray, Serializable, TagSection},
    response::Response,
};

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

fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // First create a buffer to hold message size
    let mut size_buf = [0; 4];

    // Loop until connection is terminated (by the cliend or by error)
    loop {
        match stream.read_exact(&mut size_buf) {
            Ok(_) => {
                // First read the message size
                let message_size = i32::from_be_bytes(size_buf) as usize;

                // Then read the rest of the message into a buffer
                let mut msg_buf = vec![0; message_size];
                stream.read_exact(&mut msg_buf)?;

                // Parse the request
                let (request_header, request_body) = RequestHeader::deserialize(&msg_buf)?;

                // Send response, this is very unstructured for now, will be refactored later
                let correlation_id: i32 = request_header.correlation_id;

                let response = match request_header.request_api_key {
                    18 => {
                        let response_header =
                            ResponseHeader::V0(ResponseHeaderV0 { correlation_id });
                        let (error_code, api_keys): (i16, &[ApiVersion]) =
                            match request_header.request_api_version {
                                4 => (0, &SUPPORTED_API),
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
                    75 => {
                        let response_header = ResponseHeader::V1(ResponseHeaderV1 {
                            correlation_id,
                            tag_buffer: TagSection(None),
                        });
                        let (request_body, bytes) =
                            DescribeTopicPartitionsRequest::deserialize(request_body)?;

                        let mut response_topics = vec![];
                        if let Some(topics) = request_body.topics.as_ref() {
                            for topic in topics {
                                if let Some(topic_name) = topic.name.as_ref() {
                                    response_topics
                                        .push(TopicResponse::unkown_topic(topic_name.clone()))
                                }
                            }
                        }
                        let response_body = ResponseBody::DescribeTopicPartitions(
                            DescribeTopicPartitionsResponse {
                                throttle_time: 0,
                                topics: CompactArray(Some(response_topics)),
                                next_cursor: None,
                                tag_buffer: TagSection(None),
                            },
                        );

                        Some(Response {
                            header: response_header,
                            body: response_body,
                        })
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
            Err(e) => {
                println!("Connection closed or error: {e}");
                break;
            }
        }
    }
    Ok(())
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
