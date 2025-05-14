#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buffer = String::new();
    stream.read_to_string(&mut buffer)?;

    let message_size: i32 = 0;
    let correlation_id: i32 = 7;
    stream.write_all(&message_size.to_be_bytes())?;
    stream.write_all(&correlation_id.to_be_bytes())?;
    Ok(())
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
