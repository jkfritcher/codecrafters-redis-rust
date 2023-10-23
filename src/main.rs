use anyhow::{Result, Error};

use futures::future::{BoxFuture, FutureExt};

use std::convert::From;

use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[derive(Debug, Clone)]
enum Command {
    INVALID(String),
    PING,
    ECHO(Vec<u8>),
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
            eprintln!("buffer: {:?}", buffer);
            buffer = buffer.trim().to_string();
            data = match buffer.chars().next() {
                Some('+') => DataType::SimpleString(buffer[1..].to_string()),
                Some('-') => DataType::SimpleError(buffer[1..].to_string()),
                Some(':') => DataType::Integer(buffer[1..].parse::<u64>()?),
                Some('$') => {
                    let len = buffer[1..].parse::<usize>()? + 2;
                    let mut data = vec![0; len];
                    reader.read_exact(&mut data).await?;
                    eprintln!("data: {:?}, len: {}", data, len);
                    let foo = &data[0..(len - 2)];
                    DataType::BulkString(foo.to_vec())
                }
                Some('*') => {
                    let len = buffer[1..].parse::<usize>()?;
                    let mut data: Vec<DataType> = Vec::with_capacity(len);
                    for _ in 0..len {
                        data.push(DataType::deserialize_data(reader).await?);
                    }
                    eprintln!("data: {:?}", data);
                    DataType::Array(data)
                }
                Some(_) => return Err(Error::msg("Command protocol error: unknown data type prefix")),
                _ => return Err(Error::msg("Command protocol error: expected data type prefix")),
            };
            Ok(data)
        }.boxed()
    }
}

async fn get_next_command(reader: &mut BufReader<TcpStream>) -> Result<Command> {
    let data = DataType::deserialize_data(reader).await?;
    Ok(Command::from(data))
}

async fn handle_command(stream: &mut TcpStream, cmd: Command) -> Result<()> {
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
        Command::INVALID(msg) => {
            stream.write_all(format!("-{}\r\n", msg).as_bytes()).await?;
        }
    }
    Ok(())
}

async fn handle_connection(stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let command = get_next_command(&mut reader).await?;
        handle_command(reader.get_mut(), command).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    eprintln!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                println!("an error occurred; error = {:?}", e);
            }
        });
    }
    #[allow(unreachable_code)  ]
    Ok(())
}
