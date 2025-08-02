#![allow(unused_imports)]
use core::num;
use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    str::FromStr,
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

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client connected");
                std::thread::spawn(|| handle_client(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 512];

    while stream.read(&mut buf).unwrap() > 0 {
        let data = parse_array(&mut buf).unwrap().1;
        dbg!(&data);

        match data.as_slice() {
            ["PING"] => stream.write_all(b"PONG").unwrap(),
            ["ECHO", message] => stream
                .write_all(build_binary_string(message).as_bytes())
                .unwrap(),
            _ => {}
        }
    }
}

fn parse_array(input: &[u8]) -> IResult<&[u8], Vec<&str>> {
    let (input, num_elements) = delimited(char('*'), parse_number, tag("\r\n")).parse(input)?;
    many(0..=num_elements, parse_binary_string).parse(input)
}

fn parse_simple_string(input: &[u8]) -> IResult<&[u8], &str> {
    map_res(
        delimited(char('+'), take_until("\r\n"), tag("\r\n")),
        std::str::from_utf8,
    )
    .parse(input)
}

fn parse_binary_string(input: &[u8]) -> IResult<&[u8], &str> {
    let (input, len) = delimited(char('$'), parse_number, tag("\r\n")).parse(input)?;
    let (input, data) =
        map_res(terminated(take(len), tag("\r\n")), std::str::from_utf8).parse(input)?;
    Ok((input, data))
}

fn build_binary_string(data: &str) -> String {
    format!("${}\r\n{}\r\n", data.len(), data)
}

fn parse_number(input: &[u8]) -> IResult<&[u8], usize> {
    let digits = map_res(take_while(|b: u8| b.is_ascii_digit()), std::str::from_utf8);
    map_res(digits, str::parse::<usize>).parse(input)
}

#[test]
fn parse_array_test() {
    println!("{:?}", parse_binary_string(b"$4\r\nECHO\r\n").unwrap());
    println!(
        "{:?}",
        parse_array(b"*2\r\n$4\r\nECHO\r\n$4\r\nPING\r\n").unwrap()
    );
}
