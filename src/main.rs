use anyhow::Result;

use std::{
    convert::From,
    io::{Write, Read, BufReader, BufRead},
    net::{TcpListener, TcpStream}
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

fn read_command_from_connection(reader: &mut BufReader<TcpStream>) -> Result<Command> {
    let mut buffer = String::with_capacity(1024);

    // TODO Properly parse and handle commands
    // Read number of array elements for command
    reader.read_line(&mut buffer)?;
    buffer.clear();
    // Read command length
    reader.read_line(&mut buffer)?;
    buffer.clear();
    // Read command name
    reader.read_line(&mut buffer)?;
    buffer.clear();

    Ok(Command::PING)
}

fn handle_command(stream: &mut TcpStream, cmd: Command) -> Result<()> {
    match cmd {
        Command::PING => {
            stream.write_all(b"+PONG\r\n")?;
        }
    }
    Ok(())
}

fn handle_connection(stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(stream);
    loop {
        let command = read_command_from_connection(&mut reader)?;
        handle_command(reader.get_mut(), command)?;
    }

    Ok(())
}

fn main() -> Result<()> {
    eprintln!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection(stream)?;
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
