use std::{
    collections::{BTreeMap, HashMap},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::fd::{AsFd, AsRawFd, RawFd},
    time::{Duration, Instant},
};

use nix::{
    poll::PollTimeout,
    sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags},
};
use nom::{
    bytes::complete::{take, take_while},
    combinator::map_res,
    multi::many,
    sequence::terminated,
    IResult, Parser,
};
use nom::{bytes::tag, character::complete::char, sequence::delimited};

#[derive(Default)]
struct Database {
    values: HashMap<String, String>,
    lists: HashMap<String, Vec<String>>,
}

enum TimeoutAction {
    InvalidateEntry(String),
    SendNullResponse(RawFd),
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    listener.set_nonblocking(true).unwrap();

    let epoll = Epoll::new(EpollCreateFlags::empty()).unwrap();
    epoll
        .add(
            listener.as_fd(),
            EpollEvent::new(EpollFlags::EPOLLIN, listener.as_raw_fd() as u64),
        )
        .unwrap();

    let mut db = Database::default();
    let mut event_buffer = [EpollEvent::empty(); 16];
    let mut streams = HashMap::new();
    let mut timeouts = BTreeMap::new();

    loop {
        while let Some((timeout, action)) = timeouts.first_key_value() {
            if Instant::now() >= *timeout {
                match action {
                    TimeoutAction::InvalidateEntry(key) => {
                        db.values.remove(key.as_str());
                    }
                    TimeoutAction::SendNullResponse(fd) => {
                        let stream = streams.get_mut(fd).unwrap();
                        send_null_bulk_string(stream);
                    }
                }

                timeouts.pop_first();
            } else {
                break;
            }
        }

        let poll_timeout = if let Some((timeout, _)) = timeouts.first_key_value() {
            timeout
                .duration_since(Instant::now())
                .as_millis()
                .try_into()
                .unwrap()
        } else {
            PollTimeout::NONE
        };

        let num_events = epoll.wait(&mut event_buffer, poll_timeout).unwrap();

        for event in &event_buffer[..num_events] {
            let fd = event.data() as RawFd;
            if fd == listener.as_raw_fd() {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(true).unwrap();

                        epoll
                            .add(
                                stream.as_fd(),
                                EpollEvent::new(EpollFlags::EPOLLIN, stream.as_raw_fd() as u64),
                            )
                            .unwrap();

                        streams.insert(stream.as_raw_fd(), stream);
                    }
                    Err(e) => println!("Failed to accept stream: {e}"),
                }
            } else {
                let stream = streams.get_mut(&fd).unwrap();
                handle_stream(stream, &mut db, &mut timeouts);
            }
        }
    }
}

fn handle_stream(
    stream: &mut TcpStream,
    db: &mut Database,
    timeouts: &mut BTreeMap<Instant, TimeoutAction>,
) {
    let mut buf = [0; 512];

    match stream.read(&mut buf) {
        Ok(amount) if amount > 0 => {
            let data = parse_array(&buf).unwrap().1;

            let mut cmd_parts = data.into_iter();
            match cmd_parts.next().unwrap().to_ascii_uppercase().as_str() {
                "PING" => send_simple_string(stream, "PONG"),
                "ECHO" => send_bulk_string(stream, cmd_parts.next().unwrap()),
                "RPUSH" => {
                    let key = cmd_parts.next().unwrap();

                    let list = db.lists.entry(key.into()).or_default();
                    list.extend(cmd_parts.map(String::from));

                    send_integer(stream, list.len())
                }
                "LPUSH" => {
                    let key = cmd_parts.next().unwrap();

                    let list = db.lists.entry(key.into()).or_default();
                    list.splice(..0, cmd_parts.map(String::from).rev());

                    send_integer(stream, list.len())
                }
                "LPOP" => {
                    let key = cmd_parts.next().unwrap();

                    let list = db.lists.entry(key.into()).or_default();

                    let amount = cmd_parts.next().map_or(1, |s| s.parse::<usize>().unwrap());
                    match list.len() {
                        0 => send_null_bulk_string(stream),
                        _ if amount > 1 => send_string_array(
                            stream,
                            list.drain(0..amount).collect::<Vec<_>>().as_slice(),
                        ),
                        _ => send_bulk_string(stream, &list.remove(0)),
                    }
                }
                "GET" => {
                    let key = cmd_parts.next().unwrap();

                    match db.values.get(key) {
                        Some(value) => send_bulk_string(stream, value),
                        _ => send_null_bulk_string(stream),
                    }
                }
                "SET" => {
                    let key = cmd_parts.next().unwrap();
                    let value = cmd_parts.next().unwrap();
                    if "PX"
                        == cmd_parts
                            .next()
                            .map_or(String::new(), |s| s.to_ascii_uppercase())
                    {
                        let timeout = Instant::now()
                            + Duration::from_millis(cmd_parts.next().unwrap().parse().unwrap());
                        timeouts.insert(timeout, TimeoutAction::InvalidateEntry(key.into()));
                    }

                    assert!(db
                        .values
                        .insert(key.to_string(), value.to_string())
                        .is_none());

                    send_simple_string(stream, "OK")
                }
                "LRANGE" => {
                    let key = cmd_parts.next().unwrap();

                    let start_idx = cmd_parts.next().unwrap().parse().unwrap();
                    let end_idx = cmd_parts.next().unwrap().parse().unwrap();

                    match db.lists.get(key) {
                        Some(list) if !list.is_empty() => {
                            let range = handle_index(start_idx, list.len())
                                ..=handle_index(end_idx, list.len());
                            send_string_array(stream, &list[range]);
                        }
                        _ => {
                            send_string_array(stream, &[]);
                        }
                    }
                }
                "LLEN" => {
                    let key = cmd_parts.next().unwrap();
                    match db.lists.get(key) {
                        Some(list) => send_integer(stream, list.len()),
                        None => send_integer(stream, 0),
                    }
                }
                "BLPOP" => {
                    let key = cmd_parts.next().unwrap();
                    let timeout: u64 = cmd_parts.next().unwrap().parse().unwrap();

                    let list = db.lists.entry(key.into()).or_default();
                    if list.is_empty() {
                        if timeout > 0 {
                            timeouts.insert(
                                Instant::now() + Duration::from_millis(timeout),
                                TimeoutAction::SendNullResponse(stream.as_raw_fd()),
                            );
                        }
                    } else {
                        let element = list.drain(0..amount).next().unwrap();
                        send_bulk_string(stream, &element);
                    }
                }
                _ => unimplemented!(),
            }
        }
        _ => {}
    }
}

fn handle_index(index: isize, list_len: usize) -> usize {
    let abs_index = if index < 0 {
        let abs = usize::try_from(index.abs()).unwrap();
        list_len.saturating_sub(abs)
    } else {
        index.try_into().unwrap()
    };

    abs_index.min(list_len - 1)
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
