use anyhow::Result;

use std::convert::From;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[derive(Debug, Clone)]
enum Command {
    PING,
}

impl From<&str> for Command {
    fn from(str: &str) -> Self {
        match str.to_lowercase().as_str() {
            "ping" => Command::PING,
            _ => { todo!(); }
        }
    }
}

async fn read_command_from_connection(reader: &mut BufReader<TcpStream>) -> Result<Command> {
    let mut buffer = String::with_capacity(1024);

    // TODO Properly parse and handle commands
    // Read number of array elements for command
    reader.read_line(&mut buffer).await?;
    buffer.clear();
    // Read command length
    reader.read_line(&mut buffer).await?;
    buffer.clear();
    // Read command name
    reader.read_line(&mut buffer).await?;
    buffer.clear();

    Ok(Command::PING)
}

async fn handle_command(stream: &mut TcpStream, cmd: Command) -> Result<()> {
    match cmd {
        Command::PING => {
            stream.write_all(b"+PONG\r\n").await?;
        }
    }
    Ok(())
}

async fn handle_connection(stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let command = read_command_from_connection(&mut reader).await?;
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
