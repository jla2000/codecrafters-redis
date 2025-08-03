#![allow(unused_imports)]
use core::num;
use std::{
    collections::{HashMap, VecDeque},
    fmt::format,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
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

#[derive(Default)]
struct Database {
    values: HashMap<String, (String, Option<SystemTime>)>,
    lists: HashMap<String, Vec<String>>,
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db = Mutex::new(Database::default());

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

fn handle_client(mut stream: TcpStream, db: &Mutex<Database>) {
    let mut buf = [0; 512];

    while stream.read(&mut buf).unwrap() > 0 {
        let data = parse_array(&buf).unwrap().1;
        dbg!(&data);

        let mut cmd_parts = data.into_iter();
        match cmd_parts.next().unwrap().to_ascii_uppercase().as_str() {
            "PING" => send_simple_string(&mut stream, "PONG"),
            "ECHO" => send_bulk_string(&mut stream, cmd_parts.next().unwrap()),
            "RPUSH" => {
                let key = cmd_parts.next().unwrap();

                let mut db = db.lock().unwrap();
                let list = db.lists.entry(key.into()).or_default();
                list.extend(cmd_parts.map(String::from));

                send_integer(&mut stream, list.len())
            }
            "GET" => {
                let key = cmd_parts.next().unwrap();

                let values = &mut db.lock().unwrap().values;
                match values.get(key) {
                    Some((value, None)) => send_bulk_string(&mut stream, value),
                    Some((value, Some(expiry))) => {
                        if SystemTime::now() < *expiry {
                            send_bulk_string(&mut stream, value)
                        } else {
                            values.remove(key).unwrap();
                            send_null_bulk_string(&mut stream);
                        }
                    }
                    _ => send_null_bulk_string(&mut stream),
                }
            }
            "SET" => {
                let key = cmd_parts.next().unwrap();
                let value = cmd_parts.next().unwrap();
                let expiry = if "PX"
                    == cmd_parts
                        .next()
                        .map_or(String::new(), |s| s.to_ascii_uppercase())
                {
                    Some(
                        SystemTime::now()
                            + Duration::from_millis(cmd_parts.next().unwrap().parse().unwrap()),
                    )
                } else {
                    None
                };

                assert!(db
                    .lock()
                    .unwrap()
                    .values
                    .insert(key.to_string(), (value.to_string(), expiry))
                    .is_none());

                send_simple_string(&mut stream, "OK")
            }
            "LRANGE" => {
                let key = cmd_parts.next().unwrap();

                let start_idx: isize = cmd_parts.next().unwrap().parse().unwrap();
                let end_idx: isize = cmd_parts.next().unwrap().parse().unwrap();

                match db.lock().unwrap().lists.get(key) {
                    Some(list) if !list.is_empty() => {
                        let abs_start_idx = if start_idx < 0 {
                            list.len() - usize::try_from(start_idx.abs()).unwrap()
                        } else {
                            start_idx.try_into().unwrap()
                        };
                        let abs_end_idx: usize = if end_idx < 0 {
                            list.len() - usize::try_from(end_idx.abs()).unwrap()
                        } else {
                            end_idx.try_into().unwrap()
                        };

                        let range =
                            abs_start_idx.min(list.len() - 1)..=abs_end_idx.min(list.len() - 1);
                        send_string_array(&mut stream, &list[range]);
                    }
                    _ => {
                        send_string_array(&mut stream, &[]);
                    }
                }
            }
            _ => unimplemented!(),
        };
    }
}

fn send_string_array(stream: &mut TcpStream, data: &[String]) {
    write!(stream, "*{}\r\n", data.len()).unwrap();
    data.iter()
        .for_each(|element| send_bulk_string(stream, element));
}

fn send_null_bulk_string(stream: &mut TcpStream) {
    write!(stream, "$-1\r\n").unwrap();
}

fn send_bulk_string(stream: &mut TcpStream, data: &str) {
    write!(stream, "${}\r\n{}\r\n", data.len(), data).unwrap();
}

fn send_simple_string(stream: &mut TcpStream, data: &str) {
    write!(stream, "+{data}\r\n").unwrap();
}

fn send_integer(stream: &mut TcpStream, value: usize) {
    write!(stream, ":{value}\r\n").unwrap()
}

fn parse_array(input: &[u8]) -> IResult<&[u8], Vec<&str>> {
    let (input, num_elements) = delimited(char('*'), parse_number, tag("\r\n")).parse(input)?;
    many(0..=num_elements, parse_bulk_string).parse(input)
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
