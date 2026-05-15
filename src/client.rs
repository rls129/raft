use std::io::{self, Write};
use std::net::TcpStream;
use std::{env, fs};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <file_path>", args[0]);
        std::process::exit(1);
    }

    let file_path = &args[1];
    let data = fs::canonicalize(file_path)
        .expect("Unable to canonicalised path.")
        .display()
        .to_string();

    let len = data.len();
    if len > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "File too large for u32 length prefix",
        ));
    }

    let len_u32 = len as u32;
    let len_bytes = len_u32.to_be_bytes();
    dbg!(&len_bytes);

    let mut stream = TcpStream::connect("127.0.0.1:3723")?;

    // Send length prefix
    let x = stream.write_all(&len_bytes)?;
    assert_eq!(x, ());

    // Send payload
    let x = stream.write_all(data.as_bytes())?;
    assert_eq!(x, ());

    stream.flush()?;
    stream.shutdown(std::net::Shutdown::Both)?;
    println!("Sent:\n{:?}\n{:?}", len_bytes, data.as_bytes());

    Ok(())
}
