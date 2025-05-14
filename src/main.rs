#![allow(unused_imports)]
#![allow(dead_code)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

struct Request {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
    tag_buffer: Vec<u8>,
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

    let message_size: i32 = request.correlation_id;
    let correlation_id: i32 = 7;
    stream.write_all(&message_size.to_be_bytes())?;
    stream.write_all(&correlation_id.to_be_bytes())?;
    Ok(())
}

fn parse_request(msg_buf: Vec<u8>) -> std::io::Result<Request> {
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

    Ok(Request {
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
