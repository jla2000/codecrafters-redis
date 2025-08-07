use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt::Display,
    rc::Rc,
    str::FromStr,
    time::Duration,
};

use nom::{
    bytes::complete::{take, take_while},
    combinator::map_res,
    multi::many,
    sequence::terminated,
    IResult, Parser,
};
use nom::{bytes::tag, character::complete::char, sequence::delimited};
use smol::{
    channel::Sender,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    LocalExecutor, Timer,
};

enum WaitSignal {
    Timeout,
    Completed,
}

#[derive(Default)]
struct List {
    content: Vec<String>,
    waiting: VecDeque<Sender<WaitSignal>>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
struct StreamKey(usize, usize);

impl Display for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}

impl FromStr for StreamKey {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let [milliseconds, sequence] = s.split("-").collect::<Vec<_>>().as_slice() {
            Ok(StreamKey(
                milliseconds.parse().map_err(|_| ())?,
                sequence.parse().map_err(|_| ())?,
            ))
        } else {
            Err(())
        }
    }
}

#[derive(Default)]
struct StreamEntry(Vec<(String, String)>);

#[derive(Default)]
struct Database {
    streams: HashMap<String, Vec<(StreamKey, StreamEntry)>>,
    values: HashMap<String, String>,
    lists: HashMap<String, List>,
}

struct State<'a> {
    database: RefCell<Database>,
    executor: LocalExecutor<'a>,
}

fn main() {
    let state = Rc::new(State {
        database: RefCell::new(Database::default()),
        executor: LocalExecutor::new(),
    });

    let state_for_executor = state.clone();
    smol::block_on(state.executor.run(async move {
        let acceptor = TcpListener::bind("127.0.0.1:6379").await.unwrap();

        while let Ok((stream, _)) = acceptor.accept().await {
            let state_for_client = state_for_executor.clone();
            state_for_executor
                .executor
                .spawn(async move { create_client(stream, state_for_client).await })
                .detach();
        }
    }));
}

async fn create_client(mut stream: TcpStream, state: Rc<State<'_>>) {
    let mut buf = [0; 512];
    loop {
        match stream.read(&mut buf).await {
            Ok(num_bytes_read) if num_bytes_read > 0 => match parse_array(&buf[..num_bytes_read]) {
                Ok((_, request)) => handle_request(&request, &mut stream, state.clone()).await,
                Err(e) => {
                    println!("Received invalid request: {e}");
                    break;
                }
            },
            _ => break,
        }
    }
}

async fn handle_request(request: &Vec<&str>, stream: &mut TcpStream, state: Rc<State<'_>>) {
    println!("Received request: {request:?}");

    match request.as_slice() {
        ["PING"] => send_simple_string(stream, "PONG").await,
        ["ECHO", message] => send_bulk_string(stream, *message).await,
        ["GET", key] => match state.database.borrow().values.get(*key) {
            Some(value) => send_bulk_string(stream, value).await,
            _ => send_null_bulk_string(stream).await,
        },
        ["SET", key, value] => {
            state
                .database
                .borrow_mut()
                .values
                .insert(key.to_string(), value.to_string());

            send_simple_string(stream, "OK").await
        }
        ["SET", key, value, "PX" | "px", timeout_ms] => {
            state
                .database
                .borrow_mut()
                .values
                .insert(key.to_string(), value.to_string());

            let duration = Duration::from_millis(timeout_ms.parse().unwrap());
            let key = key.to_string();
            let cloned_state = state.clone();

            state
                .executor
                .spawn(async move {
                    _ = Timer::after(duration).await;
                    _ = cloned_state.database.borrow_mut().values.remove(&key);
                })
                .detach();

            send_simple_string(stream, "OK").await
        }
        ["RPUSH", key, values @ ..] => {
            let mut db = state.database.borrow_mut();
            let list = db.lists.entry(key.to_string()).or_default();
            list.content.extend(values.iter().map(|s| s.to_string()));

            send_integer(stream, list.content.len()).await;

            while !list.content.is_empty() && !list.waiting.is_empty() {
                let client = list.waiting.pop_front().unwrap();
                client.send(WaitSignal::Completed).await.unwrap();
            }
        }
        ["LPOP", key] => {
            let mut db = state.database.borrow_mut();
            let list = db.lists.entry(key.to_string()).or_default();

            if let Some(element) = list.content.pop() {
                send_string_array(stream, &[key.to_string(), element]).await
            } else {
                send_null_bulk_string(stream).await
            }
        }
        ["LLEN", key] => {
            let db = state.database.borrow();
            match db.lists.get(*key) {
                Some(list) => send_integer(stream, list.content.len()).await,
                None => send_integer(stream, 0).await,
            }
        }
        ["LRANGE", key, start, end] => {
            let start_idx = start.parse().unwrap();
            let end_idx = end.parse().unwrap();
            let db = state.database.borrow();

            match db.lists.get(*key) {
                Some(list) if !list.content.is_empty() => {
                    let range = handle_index(start_idx, list.content.len())
                        ..=handle_index(end_idx, list.content.len());
                    send_string_array(stream, &list.content[range]).await;
                }
                _ => {
                    send_string_array(stream, &[]).await;
                }
            }
        }
        ["BRPOP", key, timeout] => {
            let timeout = Duration::from_secs_f32(timeout.parse().unwrap());

            match wait_for_list(key, state.clone(), timeout).await {
                WaitSignal::Timeout => send_null_bulk_string(stream).await,
                WaitSignal::Completed => {
                    let mut db = state.database.borrow_mut();
                    let list = db.lists.entry(key.to_string()).or_default();

                    let element = list.content.pop().unwrap();
                    send_string_array(stream, &[key.to_string(), element]).await
                }
            }
        }
        ["TYPE", key] => {
            let db = state.database.borrow();
            if db.lists.contains_key(*key) {
                send_simple_string(stream, "list").await;
            } else if db.values.contains_key(*key) {
                send_simple_string(stream, "string").await;
            } else if db.streams.contains_key(*key) {
                send_simple_string(stream, "stream").await;
            } else {
                send_simple_string(stream, "none").await;
            }
        }
        ["XADD", key, id, values @ ..] => {
            let mut db = state.database.borrow_mut();
            let db_stream = db.streams.entry(key.to_string()).or_default();

            let id: StreamKey = id.parse().unwrap();

            match (id, db_stream.last()) {
                _ if id < StreamKey(0, 1) => {
                    send_simple_error(stream, "The ID specified in XADD must be greater than 0-0")
                        .await
                }
                (_, Some((last_key, _))) if id <= *last_key => send_simple_error(
                    stream,
                    "The ID specified in XADD is equal or smaller than the target stream top item",
                )
                .await,
                _ => {
                    _ = db_stream.push((id, StreamEntry(Vec::new())));
                    send_bulk_string(stream, &id.to_string()).await;
                }
            }
        }
        _ => {}
    }
}

async fn wait_for_list(key: &str, state: Rc<State<'_>>, timeout: Duration) -> WaitSignal {
    let mut db = state.database.borrow_mut();
    let list = db.lists.entry(key.to_string()).or_default();

    if list.content.is_empty() {
        let (sender, receiver) = smol::channel::bounded(1);

        list.waiting.push_back(sender.clone());
        drop(db);

        if !timeout.is_zero() {
            state
                .executor
                .spawn(async move {
                    _ = Timer::after(timeout).await;
                    _ = sender.send(WaitSignal::Timeout).await;
                })
                .detach();
        }

        return receiver.recv().await.unwrap();
    }

    WaitSignal::Completed
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

async fn send_string_array(stream: &mut TcpStream, data: &[String]) {
    stream
        .write_all(format!("*{}\r\n", data.len()).as_bytes())
        .await
        .unwrap();

    for element in data {
        send_bulk_string(stream, element).await;
    }
}

async fn send_null_bulk_string(stream: &mut TcpStream) {
    stream.write_all(b"$-1\r\n").await.unwrap();
}

async fn send_simple_error(stream: &mut TcpStream, data: &str) {
    stream
        .write_all(format!("-ERR {}\r\n", data).as_bytes())
        .await
        .unwrap();
}

async fn send_bulk_string(stream: &mut TcpStream, data: &str) {
    stream
        .write_all(format!("${}\r\n{}\r\n", data.len(), data).as_bytes())
        .await
        .unwrap();
}

async fn send_simple_string(stream: &mut TcpStream, data: &str) {
    stream
        .write_all(format!("+{data}\r\n").as_bytes())
        .await
        .unwrap();
}

async fn send_integer(stream: &mut TcpStream, value: usize) {
    stream
        .write_all(format!(":{value}\r\n").as_bytes())
        .await
        .unwrap()
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
