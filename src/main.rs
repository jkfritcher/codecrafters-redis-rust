use anyhow::{Result, Error};

use futures::future::{BoxFuture, FutureExt};

use std::{
    collections::HashMap,
    convert::From,
    sync::Arc,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

#[derive(Debug, Clone)]
enum Command {
    INVALID(String),
    PING,
    ECHO(Vec<u8>),
    GET(Vec<u8>),
    SET(Vec<u8>, Vec<u8>),
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
                        if args.len() != 3 {
                            return Command::INVALID("Invalid data type for command. must be an array of length 3".to_string());
                        }
                        let key = match args[1] {
                            DataType::BulkString(ref key) => key,
                            _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                        };
                        let value = match args[2] {
                            DataType::BulkString(ref value) => value,
                            _ => { return Command::INVALID("Invalid data type for command. must be a bulk string".to_string()); }
                        };
                        Command::SET(key.clone(), value.clone())
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

async fn handle_command(stream: &mut TcpStream, cmd: Command, datastore: &Arc<RwLock<HashMap<Vec<u8>,Vec<u8>>>>) -> Result<()> {
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
            let ds = datastore.as_ref().read().await;
            match ds.get(&key) {
                Some(value) => {
                    let len = value.len();
                    stream.write_all(format!("${}\r\n", len).as_bytes()).await?;
                    stream.write_all(&value).await?;
                    stream.write_all("\r\n".as_bytes()).await?;
                }
                None => {
                    stream.write_all(b"$-1\r\n").await?;
                }
            }
        }
        Command::SET(key, value) => {
            let mut ds = datastore.as_ref().write().await;
            ds.insert(key, value);
            stream.write_all(b"+OK\r\n").await?;
        }
        Command::INVALID(msg) => {
            stream.write_all(format!("-{}\r\n", msg).as_bytes()).await?;
        }
    }
    Ok(())
}

async fn handle_connection(stream: TcpStream, datastore: Arc< RwLock< HashMap<Vec<u8>,Vec<u8>> > >) -> Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let command = get_next_command(&mut reader).await?;
        handle_command(reader.get_mut(), command, &datastore).await?;
    }

    #[allow(unreachable_code)]
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    eprintln!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    // Initialize datastore
    let datastore = Arc::new(RwLock::new(HashMap::new()));
    loop {
        // Clone the datastore to be captured by the closure
        let datastore = datastore.clone();
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, datastore).await {
                println!("an error occurred; error = {:?}", e);
            }
        });
    }
    #[allow(unreachable_code)  ]
    Ok(())
}
