use std::{
    io::Write,
    net::{Shutdown, TcpListener, TcpStream}
};

fn handle_connection(mut stream: TcpStream) -> Result<(), std::io::Error> {
    stream.write(b"+PONG\r\n")?;
    stream.shutdown(Shutdown::Both)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
