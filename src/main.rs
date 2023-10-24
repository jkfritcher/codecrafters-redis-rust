use anyhow::{Result, Error};

use futures::future::{BoxFuture, FutureExt};

use std::{
    collections::HashMap,
    convert::From,
    sync::Arc, path::PathBuf, os::unix::prelude::OsStrExt,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
struct DataStoreValue {
    value: Vec<u8>,
    expiry: Option<Instant>,
}

struct State {
    datastore: HashMap<Vec<u8>,DataStoreValue>,
    rdb_path: Option<PathBuf>,
}

impl State {
    fn new() -> Self {
        State {
            datastore: HashMap::new(),
            rdb_path: None,
        }
    }

    fn new_with_rdbpath(rdb_path: PathBuf) -> Self {
        State {
            datastore: HashMap::new(),
            rdb_path: Some(rdb_path),
        }
    }
}

#[derive(Debug, Clone)]
enum Command {
    INVALID(String),
    PING,
    ECHO(Vec<u8>),
    GET(Vec<u8>),
    SET(Vec<u8>, Vec<u8>),
    SETPX(Vec<u8>, Vec<u8>, Duration),
    CONFIGGET(Vec<u8>),
}

impl From<DataType> for Command {
    fn from(data: DataType) -> Self {
        match data {
            DataType::Array(args) => {
                if args.len() == 0 {
                    return Command::INVALID("Invalid data type for command. must be a non-empty array".to_string());
                }
                let name = String::from_utf8_lossy(match args[0] {
                    DataType::BulkString(ref cmd) => cmd,
                    _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                });
                match name.to_lowercase().as_str() {
                    "ping" => Command::PING,
                    "echo" => {
                        if args.len() != 2 {
                            return Command::INVALID("Invalid data type for command. must be an array of length 2".to_string());
                        }
                        let msg = match args[1] {
                            DataType::BulkString(ref msg) => msg,
                            _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                        };
                        Command::ECHO(msg.clone())
                    }
                    "get" => {
                        if args.len() != 2 {
                            return Command::INVALID("Invalid data type for command. must be an array of length 2".to_string());
                        }
                        let key = match args[1] {
                            DataType::BulkString(ref key) => key,
                            _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                        };
                        Command::GET(key.clone())
                    }
                    "set" => {
                        if args.len() != 3 && args.len() != 5 {
                            return Command::INVALID("Invalid data type for command. must be an array of length 3 or 5".to_string());
                        }
                        let key = match args[1] {
                            DataType::BulkString(ref key) => key,
                            _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                        };
                        let value = match args[2] {
                            DataType::BulkString(ref value) => value,
                            _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                        };
                        match args.len() {
                            3 => { Command::SET(key.clone(), value.clone()) }
                            5 => {
                                let arg = match args[3] {
                                    DataType::BulkString(ref arg) => arg,
                                    _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                                };
                                match arg.as_slice() {
                                    b"px" => (),
                                    _ => { return Command::INVALID("Invalid argument for command. PX is only accepted argument name".to_string()); }
                                };
                                let expiry = match args[4] {
                                    DataType::BulkString(ref expiry) => {
                                        let expiry = String::from_utf8_lossy(expiry).parse::<u64>().unwrap();
                                        Duration::from_millis(expiry)
                                    },
                                    _ => { return Command::INVALID("Invalid data type for command. PX argument must be a bulk string".to_string()); }
                                };
                                Command::SETPX(key.clone(), value.clone(), expiry)
                            }
                            _ => { todo!(); }
                        }
                    }
                    "config" => {
                        if args.len() != 3 {
                            return Command::INVALID("Invalid data type for command. must be an array of length 3".to_string());
                        }
                        let arg = match args[1] {
                            DataType::BulkString(ref arg) => arg,
                            _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                        };
                        match arg.as_slice() {
                            b"get" => (),
                            _ => { return Command::INVALID("Invalid argument for command. GET is only accepted argument name".to_string()); }
                        };
                        let key = match args[2] {
                            DataType::BulkString(ref key) => key,
                            _ => { return Command::INVALID("Invalid data type for command. GET argument must be a bulk string".to_string()); }
                        };
                        Command::CONFIGGET(key.clone())
                    }
                    _ => { todo!(); }
                }
            }
            _ => { return Command::INVALID("Invalid data type for command. must be an array".to_string()); }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DataType {
    SimpleString(String),
    SimpleError(String),
    Integer(u64),
    BulkString(Vec<u8>),
    Array(Vec<DataType>),
}

impl DataType {
    fn deserialize_data<'a>(reader: &'a mut BufReader<TcpStream>) -> BoxFuture<'a, Result<DataType>> {
        async move {
            let mut buffer = String::with_capacity(1024);
            let data;

            // Read first line of data type and dispatch to handler for further processing
            reader.read_line(&mut buffer).await?;
            buffer = buffer.trim().to_string();
            data = match buffer.chars().next() {
                Some('+') => DataType::SimpleString(buffer[1..].to_string()),
                Some('-') => DataType::SimpleError(buffer[1..].to_string()),
                Some(':') => DataType::Integer(buffer[1..].parse::<u64>()?),
                Some('$') => {
                    let len = buffer[1..].parse::<usize>()? + 2;
                    let mut data = vec![0; len];
                    reader.read_exact(&mut data).await?;
                    let foo = &data[0..(len - 2)];
                    DataType::BulkString(foo.to_vec())
                }
                Some('*') => {
                    let len = buffer[1..].parse::<usize>()?;
                    let mut data: Vec<DataType> = Vec::with_capacity(len);
                    for _ in 0..len {
                        data.push(DataType::deserialize_data(reader).await?);
                    }
                    DataType::Array(data)
                }
                Some(_) => return Err(Error::msg("Command protocol error: unknown data type prefix")),
                None => return Err(Error::msg("Client disconnected")),
            };
            Ok(data)
        }.boxed()
    }
}

async fn get_next_command(reader: &mut BufReader<TcpStream>) -> Result<Command> {
    let data = DataType::deserialize_data(reader).await?;
    Ok(Command::from(data))
}

async fn handle_command(stream: &mut TcpStream, cmd: Command, state: &Arc<RwLock<State>>) -> Result<()> {
    match cmd {
        Command::PING => {
            stream.write_all(b"+PONG\r\n").await?;
        }
        Command::ECHO(msg) => {
            let len = msg.len();
            stream.write_all(format!("${}\r\n", len).as_bytes()).await?;
            stream.write_all(&msg).await?;
            stream.write_all("\r\n".as_bytes()).await?;
        }
        Command::GET(key) => {
            let state_ro = state.as_ref().read().await;
            let ds = &state_ro.datastore;
            match ds.get(&key) {
                Some(dsv) => {
                    match dsv.expiry {
                        Some(expiry) => {
                            if expiry < Instant::now() {
                                drop(state_ro);
                                let mut state_rw = state.as_ref().write().await;
                                let ds = &mut state_rw.datastore;
                                ds.remove(&key);
                                stream.write_all(b"$-1\r\n").await?;
                            } else {
                                let len = dsv.value.len();
                                stream.write_all(format!("${}\r\n", len).as_bytes()).await?;
                                stream.write_all(&dsv.value).await?;
                                stream.write_all("\r\n".as_bytes()).await?;
                            }
                        }
                        None => {
                            let len = dsv.value.len();
                            stream.write_all(format!("${}\r\n", len).as_bytes()).await?;
                            stream.write_all(&dsv.value).await?;
                            stream.write_all("\r\n".as_bytes()).await?;
                        }
                    }
                }
                None => {
                    stream.write_all(b"$-1\r\n").await?;
                }
            }
        }
        Command::SET(key, value) => {
            let mut state = state.as_ref().write().await;
            let ds = &mut state.datastore;
            let dsv = DataStoreValue {
                value: value,
                expiry: None,
            };
            ds.insert(key, dsv);
            stream.write_all(b"+OK\r\n").await?;
        }
        Command::SETPX(key, value, expiry) => {
            let mut state = state.as_ref().write().await;
            let ds = &mut state.datastore;
            let dsv = DataStoreValue {
                value: value,
                expiry: Some(Instant::now() + expiry),
            };
            ds.insert(key, dsv);
            stream.write_all(b"+OK\r\n").await?;
        }
        Command::CONFIGGET(key) => {
            let state_ro = state.as_ref().read().await;
            let rdbpath = state_ro.rdb_path.as_ref().unwrap();
            match key.as_slice() {
                b"dir" => {
                    let dir = rdbpath.parent().unwrap().as_os_str();
                    stream.write_all(b"*2\r\n").await?;
                    stream.write_all(b"$3\r\ndir\r\n").await?;
                    stream.write_all(format!("${}\r\n", dir.len()).as_bytes()).await?;
                    stream.write_all(dir.as_bytes()).await?;
                    stream.write_all(b"\r\n").await?;
                }
                b"dbfilename" => {
                    let filename = rdbpath.file_name().unwrap();
                    stream.write_all(b"*2\r\n").await?;
                    stream.write_all(b"$10\r\ndbfilename\r\n").await?;
                    stream.write_all(format!("${}\r\n", filename.len()).as_bytes()).await?;
                    stream.write_all(filename.as_bytes()).await?;
                    stream.write_all(b"\r\n").await?;
                }
                _ => {
                    stream.write_all(b"$-1\r\n").await?;
                }
            }
        }
        Command::INVALID(msg) => {
            stream.write_all(format!("-{}\r\n", msg).as_bytes()).await?;
        }
    }
    Ok(())
}

async fn handle_connection(stream: TcpStream, state: Arc<RwLock<State>>) -> Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let command = get_next_command(&mut reader).await?;
        handle_command(reader.get_mut(), command, &state).await?;
    }

    #[allow(unreachable_code)]
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    eprintln!("Logs from your program will appear here!");

    let mut rdb_dir: Option<String> = None;
    let mut rdb_filename: Option<String> = None;

    // Iterate over command line arguments
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dir" => {
                rdb_dir = args.next().clone();
            }
            "--dbfilename" => {
                rdb_filename = args.next().clone();
            }
            _ => {
                println!("Unknown argument: {}", arg);
                return Ok(());
            }
        }
    }

    let state;
    if rdb_dir.is_some() {
        // Build rdb pathbuf
        let mut rdb_file = PathBuf::from(rdb_dir.unwrap());
        rdb_file.push(rdb_filename.unwrap_or("dump.rdb".to_string()));

        state = Arc::new(RwLock::new(State::new_with_rdbpath(rdb_file)));
    } else {
        state = Arc::new(RwLock::new(State::new()));
    }

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        // Clone the datastore to be captured by the closure
        let state = state.clone();
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, state).await {
                println!("an error occurred; error = {:?}", e);
            }
        });
    }
    #[allow(unreachable_code)  ]
    Ok(())
}
