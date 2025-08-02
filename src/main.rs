#![allow(unused_imports)]
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listener.set_nonblocking(true).unwrap();

    let mut clients = Vec::new();

    loop {
        if let Ok((client, _)) = listener.accept() {
            clients.push(client);
        }

        for client in &mut clients {
            handle_request(client);
        }
    }
}

fn handle_request(client: &mut TcpStream) {
    let mut buf = [0; 512];

    if client.read(&mut buf).unwrap() > 0 {
        _ = client.write(b"+PONG\r\n").unwrap();
    }
}
