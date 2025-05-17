#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use codecrafters_kafka::protocol::{
    apiversion::{ApiVersion, ApiVersionResponse},
    header::{RequestHeader, ResponseHeader},
    response::Response,
};

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
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
                let request = RequestHeader::parse_header(msg_buf).unwrap();

                // Send response, this is very unstructured for now, will be refactored later
                let correlation_id: i32 = request.correlation_id;

                if request.request_api_key == 18 {
                    let response_header = ResponseHeader { correlation_id };
                    let (error_code, api_keys) = match request.request_api_version {
                        4 => (
                            0,
                            vec![
                                ApiVersion {
                                    api_key: 18,
                                    min_version: 0,
                                    max_version: 4,
                                    tag_buffer: None,
                                },
                                ApiVersion {
                                    api_key: 75,
                                    min_version: 0,
                                    max_version: 0,
                                    tag_buffer: None,
                                },
                            ],
                        ),
                        _ => (35, vec![]),
                    };

                    let response_body = ApiVersionResponse {
                        error_code,
                        api_keys,
                        throttle_time_ms: 0,
                        tag_buffer: None,
                    };
                    let response = Response {
                        header: response_header,
                        body: response_body,
                    };

                    let payload = response.to_be_bytes();
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
