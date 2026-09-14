use std::io;

use http::StatusCode;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Debug, Eq, PartialEq)]
pub struct ObservedRequest {
    pub method: String,
    pub path: String,
}

pub fn serve_single_http_response(
    listener: TcpListener,
    status: StatusCode,
    body: &'static str,
) -> JoinHandle<io::Result<ObservedRequest>> {
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::with_capacity(1024);
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "client closed before completing HTTP request headers",
                ));
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
            if request.len() > 64 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP request headers exceeded 64 KiB",
                ));
            }
        }

        let request = std::str::from_utf8(&request)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let request_line = request.lines().next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "HTTP request line is missing")
        })?;
        let mut parts = request_line.split_whitespace();
        let method = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "HTTP method is missing"))?;
        let path = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "HTTP path is missing"))?;
        let version = parts
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "HTTP version is missing"))?;
        if parts.next().is_some() || !version.starts_with("HTTP/") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid HTTP request line: {request_line}"),
            ));
        }

        let reason = status.canonical_reason().unwrap_or("Unknown");
        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
            status.as_u16(),
            reason,
            body.len(),
            body,
        );
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await?;

        Ok(ObservedRequest {
            method: method.to_owned(),
            path: path.to_owned(),
        })
    })
}
