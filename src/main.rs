#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    env::VarError,
    ffi::OsStr,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    string::FromUtf8Error,
};

use niri_ipc::{Event, Window, socket::SOCKET_PATH_ENV};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener as TokioListener,
    sync::mpsc::{self, error::SendError},
    task::JoinError,
};

#[derive(Debug)]
enum MainE {
    IoError(std::io::Error),
    JoinError(JoinError),
}

impl From<std::io::Error> for MainE {
    fn from(error: std::io::Error) -> Self {
        Self::IoError(error)
    }
}

impl From<JoinError> for MainE {
    fn from(value: JoinError) -> Self {
        Self::JoinError(value)
    }
}

#[tokio::main]
async fn main() -> Result<(), MainE> {
    let client_socket = TokioListener::bind("127.0.0.1:3723").await?;
    let mut path_set = HashSet::<(String, u64)>::new();

    type WindowId = u64;
    #[derive(Debug)]
    enum WindowAction {
        Open(WindowId),
        Close(WindowId),
    }
    let (sx_niri, mut rx_niri) = mpsc::channel::<WindowAction>(8);
    let (sx_path, mut rx_path) = mpsc::channel::<String>(8);

    // owner of the hashmap
    let commander = tokio::spawn(async move {
        enum Error {
            PathChannelClosed,
            NiriChannelClosed,
        }

        let mut pending_path = None;

        fn focus(id: u64) {
            let _ = Command::new("niri")
                .args(["msg", "action", "focus-window", "--id", &id.to_string()])
                .spawn();
        }

        loop {
            tokio::select! {
                path = rx_path.recv() => {
                    let Some(path) = path else {
                        return Err(Error::PathChannelClosed);
                    };
                    if let Some((_, id)) = path_set.iter().find(|x| x.0 == path) {
                        focus(*id);
                    } else {
                        tokio::spawn(open(path.clone()));
                        pending_path = Some(path);
                    }
                }

                wind = rx_niri.recv() => {
                    let Some(wind_id) = wind else {
                        return Err(Error::NiriChannelClosed);
                    };

                    match wind_id {
                        WindowAction::Open(id) => {
                            if let Some(path) = pending_path.take() {
                                path_set.insert((path, id));
                                focus(id);
                            }
                        },
                        WindowAction::Close(id) => {
                            path_set.retain(|x| x.1 != id);
                        },
                    }

                }
            }
        }

        #[allow(unreachable_code)]
        Ok::<(), Error>(())
    });

    // subscribe to niri events
    let niri_listerner = tokio::spawn(async move {
        enum Error {
            Serde(serde_json::Error),
            Tokio(std::io::Error),
            Niri(String),
            Mpsc(SendError<Window>),
            Var(VarError),
        }

        impl From<serde_json::Error> for Error {
            fn from(value: serde_json::Error) -> Self {
                Self::Serde(value)
            }
        }

        impl From<std::io::Error> for Error {
            fn from(value: std::io::Error) -> Self {
                Self::Tokio(value)
            }
        }

        impl From<String> for Error {
            fn from(value: String) -> Self {
                Self::Niri(value)
            }
        }

        impl From<SendError<Window>> for Error {
            fn from(value: SendError<Window>) -> Self {
                Self::Mpsc(value)
            }
        }

        impl From<VarError> for Error {
            fn from(value: VarError) -> Self {
                Self::Var(value)
            }
        }

        let niri_socket = tokio::net::UnixSocket::new_stream()?;
        let niri_sock_path = std::env::var(SOCKET_PATH_ENV)?;
        let mut niri_stream = niri_socket.connect(niri_sock_path).await?;

        let request = niri_ipc::Request::EventStream;
        let mut request_json = serde_json::to_string(&request)?;
        request_json.push('\n');
        niri_stream.write_all(request_json.as_bytes()).await?;

        let mut niri_stream_read = BufReader::new(niri_stream);

        loop {
            let mut response_json = String::new();
            let Ok(_) = niri_stream_read.read_line(&mut response_json).await else {
                continue;
            };
            let sx_niri = sx_niri.clone();
            tokio::spawn(async move {
                let reply = serde_json::from_str::<Event>(&response_json.clone())?;
                let _ = match reply {
                    Event::WindowClosed { id } => sx_niri.send(WindowAction::Close(id)).await,
                    Event::WindowOpenedOrChanged { window } => {
                        sx_niri.send(WindowAction::Open(window.id)).await
                    }
                    _ => Ok(()),
                };
                Ok::<(), Error>(())
            });
        }

        #[allow(unreachable_code)]
        Ok::<(), Error>(())
    });

    // handle client commands
    let cli_listener = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = client_socket.accept().await else {
                continue;
            };
            let sx_path = sx_path.clone();
            tokio::spawn(async move {
                #[allow(dead_code)]
                enum Error {
                    IoError(std::io::Error),
                    UtfErorr(FromUtf8Error),
                    SendError(SendError<String>),
                }

                impl From<std::io::Error> for Error {
                    fn from(value: std::io::Error) -> Self {
                        Self::IoError(value)
                    }
                }

                impl From<FromUtf8Error> for Error {
                    fn from(value: FromUtf8Error) -> Self {
                        Self::UtfErorr(value)
                    }
                }

                impl From<SendError<String>> for Error {
                    fn from(value: SendError<String>) -> Self {
                        Self::SendError(value)
                    }
                }

                let strlen = stream.read_u32().await?;
                let mut string_buffer: Vec<u8> = Vec::with_capacity(strlen as usize);
                string_buffer.resize(strlen as usize, 0);
                stream.read_exact(&mut string_buffer).await?;

                let file_path = String::from_utf8(string_buffer)?;

                let eof = stream.read_u32().await;
                assert!(eof.is_err_and(|x| x.kind() == std::io::ErrorKind::UnexpectedEof));

                // NOTE: here is the interesting part
                sx_path.send(file_path).await?;
                Ok::<(), Error>(())
            });
        }

        // This only exists to allow the compiler to infer the types
        // so that ? can work
        #[allow(unreachable_code)]
        std::io::Result::Ok(())
    });

    let _x = tokio::try_join!(commander, niri_listerner, cli_listener)?;

    Ok(())
}

fn send_notif(msg: &str) {
    let _ = Command::new("notify-send")
        .args(["boat", msg])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

enum OpenError {
    Spawn,
}
impl From<tokio::io::Error> for OpenError {
    fn from(_: tokio::io::Error) -> Self {
        Self::Spawn
    }
}

async fn open(path: String) -> Result<(), OpenError> {
    let path = match std::fs::canonicalize(path) {
        Ok(x) => x.display().to_string(),
        Err(e) => {
            send_notif(&format!("{}: Cannot open file: {:?}", line!(), e));
            return Ok(());
        }
    };

    let applications = enumerate_applications();

    let mime = match query_output("xdg-mime", &["query", "filetype", &path]) {
        Some(m) => m,
        None => {
            send_notif(&format!("{}: failed to determine mime type", line!()));
            return Ok(());
        }
    };

    let default_desktop = match query_output("xdg-mime", &["query", "default", &mime]) {
        Some(d) => d,
        None => {
            send_notif(&format!(
                "{}: failed to determine default application",
                line!()
            ));
            return Ok(());
        }
    };

    let entry = match applications.get(&default_desktop) {
        Some(e) => e,
        None => {
            send_notif(
                format!("{}: desktop entry not found: {}", line!(), default_desktop).as_str(),
            );
            return Ok(());
        }
    };

    fn exec_parts(exec: &str, file: &str) -> Vec<String> {
        let mut parts = Vec::new();
        for token in exec.split_whitespace() {
            // skip the xdg specification formatting
            // it also means we cannot have other %shiz
            if !token.starts_with("%") {
                parts.push(token.to_string());
            }
        }
        parts.push(file.to_string());
        parts
    }

    let exec_parts = exec_parts(&entry.exec, &path);

    if exec_parts.is_empty() {
        send_notif(&format!("{}: invalid exec line, {}", line!(), entry.exec));
        return Ok(());
    }

    let program = &exec_parts[0];
    let args = &exec_parts[1..];

    let temp_file_template = format!(
        "boat.{}.{}.XXXXX",
        Path::new(program)
            .canonicalize()
            .unwrap_or("not_found".into())
            .file_stem()
            .unwrap_or(OsStr::new("not_found"))
            .to_str()
            .unwrap_or("not_found"),
        path.chars()
            .map(|x| if x == '/' { '_' } else { x })
            .collect::<String>()
    );
    let mk_temp = match Command::new("mktemp")
        .args(["-t", &temp_file_template])
        .output()
    {
        Ok(x) => x,
        Err(x) => {
            send_notif(
                format!("failed to make temp log file. {} {}", x, temp_file_template).as_str(),
            );
            return Ok(());
        }
    };

    let mk_temp_file_name = match String::from_utf8(mk_temp.stdout) {
        Ok(x) => x,
        Err(x) => {
            send_notif(format!("failed to read temp log file. {}", x).as_str());
            return Ok(());
        }
    };

    let mk_temp_file = match std::fs::OpenOptions::new()
        .write(true)
        .open(mk_temp_file_name.trim_end())
    {
        Ok(x) => x,
        Err(x) => {
            send_notif(
                format!("failed to open temp log file. {} {}", mk_temp_file_name, x).as_str(),
            );
            return Ok(());
        }
    };

    let _ = if entry.terminal {
        let terminal = std::env::var("XDG_TERMINAL").unwrap_or_else(|_| "ghostty".to_string());
        let stdout = Stdio::from(mk_temp_file);
        Command::new(terminal)
            .arg("-e")
            .arg(program)
            .stdout(stdout)
            .stderr(Stdio::null())
            .args(args)
            .spawn()
    } else {
        Command::new(program).args(args).spawn()
    };

    Ok(())
}

#[derive(Debug, Clone)]
struct DesktopEntry {
    // name: String,
    exec: String,
    terminal: bool,
    desktop_file: String,
}

fn enumerate_applications() -> HashMap<String, DesktopEntry> {
    let mut entries = HashMap::new();

    let mut dirs = vec![PathBuf::from("/usr/share/applications")];

    if let Ok(home) = std::env::var("HOME") {
        dirs.push(PathBuf::from(format!("{home}/.local/share/applications")));
    }

    for dir in dirs {
        if !dir.exists() {
            continue;
        }

        let Ok(read_dir) = std::fs::read_dir(dir) else {
            continue;
        };

        for entry in read_dir.flatten() {
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) != Some("desktop") {
                continue;
            }

            if let Some(parsed) = parse_desktop_file(&path) {
                entries.insert(parsed.desktop_file.clone(), parsed);
            }
        }
    }

    entries
}

fn parse_desktop_file(path: &std::path::Path) -> Option<DesktopEntry> {
    let content = std::fs::read_to_string(path).ok()?;

    let mut in_desktop_entry = false;

    // let mut name = None;
    let mut exec = None;
    let mut terminal = false;

    for line in content.lines() {
        let line = line.trim();

        if line.starts_with('[') {
            in_desktop_entry = line == "[Desktop Entry]";
            continue;
        }

        if !in_desktop_entry || line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(_) = line.strip_prefix("Name=") {
            // name = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("Exec=") {
            exec = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("Terminal=") {
            terminal = value.eq_ignore_ascii_case("true");
        }
    }

    Some(DesktopEntry {
        // name: name?,
        exec: exec?,
        terminal,
        desktop_file: path.file_name()?.to_string_lossy().to_string(),
    })
}

fn query_output(cmd: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
