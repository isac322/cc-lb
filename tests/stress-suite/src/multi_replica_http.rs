use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

pub fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Result<HttpResponse, String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|error| error.to_string())?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "HTTP response is missing headers".to_owned())?;
    let status = head
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "HTTP response is missing status".to_owned())?
        .parse::<u16>()
        .map_err(|error| error.to_string())?;
    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

pub fn wait_for_healthy(address: SocketAddr) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if request(address, "GET", "/healthz", &[], "").is_ok_and(|response| response.status == 200)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("replica {address} did not become healthy"));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub fn wait_for_tcp(address: SocketAddr) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_secs(1)).is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("process {address} did not accept TCP"));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

pub fn wait_for_body(address: SocketAddr, path: &str, needle: &str) -> Result<u64, String> {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    loop {
        if request(
            address,
            "GET",
            path,
            &[("Authorization", "Bearer stress-admin")],
            "",
        )
        .is_ok_and(|response| response.status == 200 && response.body.contains(needle))
        {
            return u64::try_from(started.elapsed().as_millis()).map_err(|error| error.to_string());
        }
        if Instant::now() >= deadline {
            return Err(format!("{needle} did not propagate to {address}"));
        }
        thread::sleep(Duration::from_millis(50));
    }
}
