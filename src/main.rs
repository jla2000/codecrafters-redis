#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                // let mut input = String::new();
                // stream.read_to_string(&mut input).unwrap();

                // for request in input.lines() {
                //     if request.starts_with("PING") {
                //         stream.write_all(b"+PONG\r\n").unwrap()
                //     }
                // }
                stream.write_all(b"+PONG\r\n").unwrap()
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
