use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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

#[derive(Default, Debug)]
struct List {
    content: Vec<String>,
    waiting: VecDeque<RawFd>,
}

#[derive(Default)]
struct Database {
    values: HashMap<String, String>,
    lists: HashMap<String, List>,
}

enum TimeoutAction {
    InvalidateEntry(String),
    StopWaiting(RawFd, String),
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
                    TimeoutAction::StopWaiting(fd, list_name) => {
                        let list = db.lists.get_mut(list_name).unwrap();

                        if let Some(wait_idx) = list.waiting.iter().position(|val| val == fd) {
                            _ = list.waiting.remove(wait_idx);
                            let stream = streams.get_mut(fd).unwrap();
                            send_null_bulk_string(stream);
                        }
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
                handle_stream(fd, &mut streams, &mut db, &mut timeouts).unwrap();
            }
        }
    }
}

fn handle_stream(
    fd: RawFd,
    streams: &mut HashMap<RawFd, TcpStream>,
    db: &mut Database,
    timeouts: &mut BTreeMap<Instant, TimeoutAction>,
) -> std::io::Result<()> {
    let mut buf = [0; 512];

    let stream = streams.get_mut(&fd).unwrap();
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
                    list.content.extend(cmd_parts.map(String::from));

                    send_integer(stream, list.content.len());
                    dbg!(&list);

                    while list.content.len() >= list.waiting.len() && !list.waiting.is_empty() {
                        let waiting_fd = list.waiting.pop_front().unwrap();
                        let waiting_client = streams.get_mut(&waiting_fd).unwrap();

                        println!("Sending response to waiting {waiting_fd} waiting on {key}");
                        send_bulk_string(
                            waiting_client,
                            &list.content.drain(0..amount).next().unwrap(),
                        );
                    }
                }
                "LPUSH" => {
                    let key = cmd_parts.next().unwrap();

                    let list = db.lists.entry(key.into()).or_default();
                    list.content.splice(..0, cmd_parts.map(String::from).rev());

                    send_integer(stream, list.content.len());

                    while list.content.len() >= list.waiting.len() && !list.waiting.is_empty() {
                        let waiting_client =
                            streams.get_mut(&list.waiting.pop_front().unwrap()).unwrap();

                        send_bulk_string(
                            waiting_client,
                            &list.content.drain(0..amount).next().unwrap(),
                        );
                    }
                }
                "LPOP" => {
                    let key = cmd_parts.next().unwrap();

                    let list = db.lists.entry(key.into()).or_default();

                    let amount = cmd_parts.next().map_or(1, |s| s.parse::<usize>().unwrap());
                    match list.content.len() {
                        0 => send_null_bulk_string(stream),
                        _ if amount > 1 => send_string_array(
                            stream,
                            list.content.drain(0..amount).collect::<Vec<_>>().as_slice(),
                        ),
                        _ => send_bulk_string(stream, &list.content.remove(0)),
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
                        Some(list) if !list.content.is_empty() => {
                            let range = handle_index(start_idx, list.content.len())
                                ..=handle_index(end_idx, list.content.len());
                            send_string_array(stream, &list.content[range]);
                        }
                        _ => {
                            send_string_array(stream, &[]);
                        }
                    }
                }
                "LLEN" => {
                    let key = cmd_parts.next().unwrap();
                    match db.lists.get(key) {
                        Some(list) => send_integer(stream, list.content.len()),
                        None => send_integer(stream, 0),
                    }
                }
                "BLPOP" => {
                    let key = cmd_parts.next().unwrap();
                    let timeout: u64 = cmd_parts.next().unwrap().parse().unwrap();

                    let list = db.lists.entry(key.into()).or_default();
                    if list.content.is_empty() {
                        if timeout > 0 {
                            timeouts.insert(
                                Instant::now() + Duration::from_millis(timeout),
                                TimeoutAction::StopWaiting(stream.as_raw_fd(), key.into()),
                            );
                        }
                        list.waiting.push_back(fd);
                        println!("Register {fd} waiting on {key}");
                    } else {
                        let element = list.content.drain(0..amount).next().unwrap();
                        send_bulk_string(stream, &element);
                    }
                }
                _ => unimplemented!(),
            }
        }
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
        Err(e) => return Err(e),
    }

    Ok(())
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
