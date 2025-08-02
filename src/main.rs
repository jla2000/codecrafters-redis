#![allow(unused_imports)]
use core::num;
use std::{
    collections::{HashMap, VecDeque},
    fmt::format,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    str::FromStr,
    sync::{Arc, Mutex},
};

use nom::{
    branch::alt,
    bytes::complete::{take, take_until, take_while},
    combinator::map_res,
    error::ParseError,
    multi::{fold, many, many_m_n},
    number,
    sequence::{preceded, terminated},
    IResult, Parser,
};
use nom::{bytes::tag, character::complete::char, sequence::delimited};
use strum::EnumString;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db = Mutex::new(HashMap::new());

    std::thread::scope(|s| {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("Client connected");
                    s.spawn(|| handle_client(stream, &db));
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    })
}

fn handle_client(mut stream: TcpStream, db: &Mutex<HashMap<String, String>>) {
    let mut buf = [0; 512];

    while stream.read(&mut buf).unwrap() > 0 {
        let data = parse_array(&mut buf).unwrap().1;
        dbg!(&data);

        let response = match data.as_slice() {
            ["PING"] => build_simple_string("PONG"),
            ["ECHO", message] => build_bulk_string(message),
            ["GET", key] => {
                if let Some(value) = db.lock().unwrap().get(*key) {
                    build_bulk_string(value)
                } else {
                    "$-1\r\n".into()
                }
            }
            ["SET", key, value] => {
                assert!(db
                    .lock()
                    .unwrap()
                    .insert(key.to_string(), value.to_string())
                    .is_none());
                build_simple_string("OK")
            }
            _ => unimplemented!(),
        };

        stream.write_all(response.as_bytes()).unwrap();
    }
}

fn build_bulk_string(data: &str) -> String {
    format!("${}\r\n{}\r\n", data.len(), data)
}

fn build_simple_string(data: &str) -> String {
    format!("+{data}\r\n")
}

fn parse_array(input: &[u8]) -> IResult<&[u8], Vec<&str>> {
    let (input, num_elements) = delimited(char('*'), parse_number, tag("\r\n")).parse(input)?;
    many(0..=num_elements, parse_bulk_string).parse(input)
}

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], &str> {
    map_res(
        delimited(char('+'), take_until("\r\n"), tag("\r\n")),
        std::str::from_utf8,
    )
    .parse(input)
}

fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], &str> {
    let (input, len) = delimited(char('$'), parse_number, tag("\r\n")).parse(input)?;
    let (input, data) =
        map_res(terminated(take(len), tag("\r\n")), std::str::from_utf8).parse(input)?;
    Ok((input, data))
}

fn parse_number(input: &[u8]) -> IResult<&[u8], usize> {
    let digits = map_res(take_while(|b: u8| b.is_ascii_digit()), std::str::from_utf8);
    map_res(digits, str::parse::<usize>).parse(input)
}

#[test]
fn parse_array_test() {
    println!("{:?}", parse_bulk_string(b"$4\r\nECHO\r\n").unwrap());
    println!(
        "{:?}",
        parse_array(b"*2\r\n$4\r\nECHO\r\n$4\r\nPING\r\n").unwrap()
    );
}
