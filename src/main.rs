use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, VecDeque},
    io::{Read, Write},
    os::fd::{AsFd, AsRawFd, RawFd},
    rc::Rc,
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
use ringbuffer::GrowableAllocRingBuffer;
use smol::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    LocalExecutor, Timer,
};

#[derive(Default, Debug)]
struct List {
    content: Vec<String>,
    waiting: VecDeque<RawFd>,
}

#[derive(Default)]
struct Database {
    values: HashMap<String, String>,
    lists: HashMap<String, List>,
    clients: HashMap<RawFd, TcpStream>,
}

enum TimeoutAction {
    InvalidateEntry(String),
    StopWaiting(RawFd, String),
}

#[derive(Debug)]
enum Event {
    DataReceived(RawFd),
    InvalidateEntry(String),
    StopWaiting(RawFd, String),
}

struct Reactor {
    pending_events: GrowableAllocRingBuffer<Event>,
    timeouts: BTreeMap<Instant, Event>,
    epoll: Epoll,
    epoll_event_buffer: EpollEventBuffer,
}

struct EpollEventBuffer {
    buffer: [EpollEvent; 32],
    num_elements: usize,
}

impl Reactor {
    fn register_oneshot(&mut self, fd: impl AsFd + Copy) {
        self.epoll
            .add(
                fd,
                EpollEvent::new(
                    EpollFlags::EPOLLIN | EpollFlags::EPOLLONESHOT,
                    fd.as_fd().as_raw_fd() as u64,
                ),
            )
            .unwrap();
    }

    fn reactivate_oneshot(&mut self, fd: impl AsFd + Copy) {
        self.epoll
            .modify(
                fd,
                &mut EpollEvent::new(
                    EpollFlags::EPOLLIN | EpollFlags::EPOLLONESHOT,
                    fd.as_fd().as_raw_fd() as u64,
                ),
            )
            .unwrap();
    }

    fn register_timeout(&mut self, timeout: Instant, event: Event) {
        _ = self.timeouts.insert(timeout, event);
    }
}

impl Default for Reactor {
    fn default() -> Self {
        Self {
            pending_events: GrowableAllocRingBuffer::default(),
            timeouts: BTreeMap::default(),
            epoll: Epoll::new(EpollCreateFlags::empty()).unwrap(),
            epoll_event_buffer: EpollEventBuffer {
                buffer: [EpollEvent::empty(); 32],
                num_elements: 0,
            },
        }
    }
}

impl Iterator for Reactor {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_timeout = if let Some((timeout, _)) = self.timeouts.first_key_value() {
                if Instant::now() >= *timeout {
                    break Some(self.timeouts.pop_first().unwrap().1);
                } else {
                    timeout
                        .duration_since(Instant::now())
                        .as_millis()
                        .try_into()
                        .unwrap()
                }
            } else {
                PollTimeout::NONE
            };

            for event in &self.epoll_event_buffer.buffer[..self.epoll_event_buffer.num_elements] {
                let fd = event.data() as RawFd;
                self.pending_events.push_back(Event::DataReceived(fd));
            }
            self.epoll_event_buffer.num_elements = 0;

            if let Some(event) = self.pending_events.pop_front() {
                break Some(event);
            } else {
                self.epoll_event_buffer.num_elements = self
                    .epoll
                    .wait(&mut self.epoll_event_buffer.buffer, next_timeout)
                    .unwrap();
            }
        }
    }
}

async fn create_client<'db>(
    mut stream: TcpStream,
    db: Rc<RefCell<Database>>,
    executor: Rc<LocalExecutor<'_>>,
) {
    let mut buf = [0; 512];
    loop {
        match stream.read(&mut buf).await {
            Ok(num_bytes_read) if num_bytes_read > 0 => match parse_array(&buf[..num_bytes_read]) {
                Ok((_, request)) => {
                    handle_request(&request, &mut stream, db.clone(), executor.clone()).await
                }
                Err(e) => {
                    println!("Received invalid request: {e}");
                    break;
                }
            },
            _ => break,
        }
    }
}

async fn handle_request(
    request: &Vec<&str>,
    stream: &mut TcpStream,
    db: Rc<RefCell<Database>>,
    executor: Rc<LocalExecutor<'_>>,
) {
    println!("Received request: {request:?}");

    match request.as_slice() {
        ["PING"] => send_bulk_string(stream, "PONG").await,
        ["ECHO", message] => send_bulk_string(stream, *message).await,
        ["GET", key] => match db.borrow().values.get(*key) {
            Some(value) => send_bulk_string(stream, value).await,
            _ => send_null_bulk_string(stream).await,
        },
        ["SET", key, value] => {
            db.borrow_mut()
                .values
                .insert(key.to_string(), value.to_string());

            send_simple_string(stream, "OK").await
        }
        ["SET", key, value, "PX", timeout_ms] => {
            db.borrow_mut()
                .values
                .insert(key.to_string(), value.to_string());

            let duration = Duration::from_millis(timeout_ms.parse().unwrap());
            let key = key.to_string();

            executor
                .spawn(async move {
                    _ = Timer::after(duration).await;
                    _ = db.borrow_mut().values.remove(&key);
                })
                .detach();

            send_simple_string(stream, "OK").await
        }
        _ => {}
    }
}

fn main() {
    let db = Rc::new(RefCell::new(Database::default()));
    let executor = Rc::new(LocalExecutor::new());

    let executor_clone = executor.clone();
    smol::block_on(executor.run(async move {
        let acceptor = TcpListener::bind("127.0.0.1:6379").await.unwrap();

        while let Ok((stream, _)) = acceptor.accept().await {
            let db_clone = db.clone();
            let another_executor_clone = executor_clone.clone();

            executor_clone
                .spawn(async move {
                    create_client(stream, db_clone, another_executor_clone.clone()).await
                })
                .detach();
        }
    }));

    // let mut db = Database::default();
    // let mut streams = HashMap::new();
    // let mut event_loop = Reactor::default();

    // let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    // listener.set_nonblocking(true).unwrap();
    // event_loop.register_oneshot(listener.as_fd());
    //
    // while let Some(event) = event_loop.next() {
    //     match event {
    //         Event::DataReceived(fd) if fd == listener.as_raw_fd() => {
    //             match listener.accept() {
    //                 Ok((stream, _)) => {
    //                     stream.set_nonblocking(true).unwrap();
    //                     event_loop.register_oneshot(stream.as_fd());
    //                     streams.insert(stream.as_raw_fd(), stream);
    //                 }
    //                 Err(e) => println!("Error accepting client: {e}"),
    //             }
    //             event_loop.reactivate_oneshot(listener.as_fd());
    //         }
    //         Event::DataReceived(fd) => {
    //             handle_stream(fd, &mut streams, &mut db, &mut event_loop).unwrap();
    //             event_loop.reactivate_oneshot(streams.get(&fd).unwrap().as_fd());
    //         }
    //         Event::StopWaiting(fd, key) => {
    //             let list = db.lists.get_mut(&key).unwrap();
    //
    //             if let Some(wait_idx) = list.waiting.iter().position(|val| *val == fd) {
    //                 _ = list.waiting.remove(wait_idx);
    //                 let stream = streams.get_mut(&fd).unwrap();
    //                 send_null_bulk_string(stream);
    //             }
    //         }
    //         Event::InvalidateEntry(key) => {
    //             _ = db.values.remove(&key);
    //         }
    //         _ => println!("Unknown event received: {event:?}"),
    //     }
    // }
}

// fn handle_stream(
//     fd: RawFd,
//     streams: &mut HashMap<RawFd, TcpStream>,
//     db: &mut Database,
//     event_loop: &mut Reactor,
// ) -> std::io::Result<()> {
//     let mut buf = [0; 512];
//
//     let stream = streams.get_mut(&fd).unwrap();
//     match stream.read(&mut buf) {
//         Ok(amount) if amount > 0 => {
//             let data = parse_array(&buf).unwrap().1;
//
//             let mut cmd_parts = data.into_iter();
//             match cmd_parts.next().unwrap().to_ascii_uppercase().as_str() {
//                 "PING" => send_simple_string(stream, "PONG"),
//                 "ECHO" => send_bulk_string(stream, cmd_parts.next().unwrap()),
//                 "RPUSH" => {
//                     let key = cmd_parts.next().unwrap();
//
//                     let list = db.lists.entry(key.into()).or_default();
//                     list.content.extend(cmd_parts.map(String::from));
//
//                     send_integer(stream, list.content.len());
//
//                     while !list.content.is_empty() && !list.waiting.is_empty() {
//                         let waiting_fd = list.waiting.pop_front().unwrap();
//                         let waiting_client = streams.get_mut(&waiting_fd).unwrap();
//
//                         let element = list.content.drain(..1).next().unwrap();
//                         send_string_array(waiting_client, &[key.into(), element]);
//                     }
//                 }
//                 "LPUSH" => {
//                     let key = cmd_parts.next().unwrap();
//
//                     let list = db.lists.entry(key.into()).or_default();
//                     list.content.splice(..0, cmd_parts.map(String::from).rev());
//
//                     send_integer(stream, list.content.len());
//
//                     while !list.content.is_empty() && !list.waiting.is_empty() {
//                         let waiting_fd = list.waiting.pop_front().unwrap();
//                         let waiting_client = streams.get_mut(&waiting_fd).unwrap();
//
//                         let element = list.content.drain(..1).next().unwrap();
//                         send_string_array(waiting_client, &[key.into(), element]);
//                     }
//                 }
//                 "LPOP" => {
//                     let key = cmd_parts.next().unwrap();
//
//                     let list = db.lists.entry(key.into()).or_default();
//
//                     let amount = cmd_parts.next().map_or(1, |s| s.parse::<usize>().unwrap());
//                     match list.content.len() {
//                         0 => send_null_bulk_string(stream),
//                         _ if amount > 1 => send_string_array(
//                             stream,
//                             list.content.drain(0..amount).collect::<Vec<_>>().as_slice(),
//                         ),
//                         _ => send_bulk_string(stream, &list.content.remove(0)),
//                     }
//                 }
//                 "GET" => {
//                     let key = cmd_parts.next().unwrap();
//
//                     match db.values.get(key) {
//                         Some(value) => send_bulk_string(stream, value),
//                         _ => send_null_bulk_string(stream),
//                     }
//                 }
//                 "SET" => {
//                     let key = cmd_parts.next().unwrap();
//                     let value = cmd_parts.next().unwrap();
//                     if "PX"
//                         == cmd_parts
//                             .next()
//                             .map_or(String::new(), |s| s.to_ascii_uppercase())
//                     {
//                         let timeout = Instant::now()
//                             + Duration::from_millis(cmd_parts.next().unwrap().parse().unwrap());
//                         event_loop.register_timeout(timeout, Event::InvalidateEntry(key.into()));
//                     }
//
//                     _ = db.values.insert(key.to_string(), value.to_string());
//
//                     send_simple_string(stream, "OK")
//                 }
//                 "LRANGE" => {
//                     let key = cmd_parts.next().unwrap();
//
//                     let start_idx = cmd_parts.next().unwrap().parse().unwrap();
//                     let end_idx = cmd_parts.next().unwrap().parse().unwrap();
//
//                     match db.lists.get(key) {
//                         Some(list) if !list.content.is_empty() => {
//                             let range = handle_index(start_idx, list.content.len())
//                                 ..=handle_index(end_idx, list.content.len());
//                             send_string_array(stream, &list.content[range]);
//                         }
//                         _ => {
//                             send_string_array(stream, &[]);
//                         }
//                     }
//                 }
//                 "LLEN" => {
//                     let key = cmd_parts.next().unwrap();
//                     match db.lists.get(key) {
//                         Some(list) => send_integer(stream, list.content.len()),
//                         None => send_integer(stream, 0),
//                     }
//                 }
//                 "BLPOP" => {
//                     let key = cmd_parts.next().unwrap();
//                     let timeout: f32 = cmd_parts.next().unwrap().parse().unwrap();
//
//                     let list = db.lists.entry(key.into()).or_default();
//                     if list.content.is_empty() {
//                         if timeout > 0.0 {
//                             event_loop.register_timeout(
//                                 Instant::now() + Duration::from_secs_f32(timeout),
//                                 Event::StopWaiting(stream.as_raw_fd(), key.into()),
//                             );
//                         }
//                         list.waiting.push_back(fd);
//                     } else {
//                         let element = list.content.drain(..1).next().unwrap();
//                         send_string_array(stream, &[key.into(), element]);
//                     }
//                 }
//                 _ => unimplemented!(),
//             }
//         }
//         Ok(_) => {}
//         Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
//         Err(e) => return Err(e),
//     }
//
//     Ok(())
// }
//
// fn handle_index(index: isize, list_len: usize) -> usize {
//     let abs_index = if index < 0 {
//         let abs = usize::try_from(index.abs()).unwrap();
//         list_len.saturating_sub(abs)
//     } else {
//         index.try_into().unwrap()
//     };
//
//     abs_index.min(list_len - 1)
// }
//
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
