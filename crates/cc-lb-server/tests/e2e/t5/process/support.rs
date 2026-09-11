// tier-allow(silent-skip): child status is an explicit optional process state, not a skipped assertion until=2027-03-31
use std::io::Read;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::Duration;

use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
#[cfg(feature = "postgres")]
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
#[cfg(feature = "postgres")]
use tokio::task::JoinSet;
#[cfg(feature = "postgres")]
use url::Url;

pub const MASTER_KEY_HEX: &str = "0000000000000000000000000000000000000000000000000000000000000000";
pub const ADMIN_TOKEN: &str = "t5-process-admin-token";
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);
pub const EXIT_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Fixture {
    directory: TempDir,
}

impl Fixture {
    pub fn new(label: &str) -> Self {
        let prefix = format!("cc-lb-t5-{label}-");
        let directory = tempfile::Builder::new()
            .prefix(&prefix)
            .tempdir()
            .expect("create process fixture directory");
        Self { directory }
    }

    pub fn root(&self) -> &Path {
        self.directory.path()
    }

    pub fn path(&self, name: impl AsRef<Path>) -> PathBuf {
        self.root().join(name)
    }

    pub fn write(&self, name: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> PathBuf {
        let path = self.path(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create fixture parent directory");
        }
        std::fs::write(&path, contents).expect("write process fixture file");
        path
    }
}

// tier-allow(port-handoff): t5 process
pub struct ReservedAddr {
    listener: Option<StdTcpListener>,
    addr: SocketAddr,
}

impl ReservedAddr {
    pub fn bind() -> std::io::Result<Self> {
        let listener = StdTcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        Ok(Self {
            listener: Some(listener),
            addr,
        })
    }

    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn release(mut self) -> SocketAddr {
        drop(self.listener.take());
        self.addr
    }
}

pub struct ReservedAddrs {
    pub proxy: ReservedAddr,
    pub admin: ReservedAddr,
    pub metrics: ReservedAddr,
}

impl ReservedAddrs {
    pub fn bind() -> std::io::Result<Self> {
        Ok(Self {
            proxy: ReservedAddr::bind()?,
            admin: ReservedAddr::bind()?,
            metrics: ReservedAddr::bind()?,
        })
    }

    pub fn release(self) -> ServerAddrs {
        ServerAddrs {
            proxy: self.proxy.release(),
            admin: self.admin.release(),
            metrics: self.metrics.release(),
        }
    }
}
#[cfg(feature = "postgres")]
enum PostgresProxyCommand {
    SetAvailable {
        available: bool,
        acknowledged: oneshot::Sender<()>,
    },
    Shutdown {
        acknowledged: oneshot::Sender<()>,
    },
}

#[cfg(feature = "postgres")]
pub(crate) struct PostgresConnectionProxy {
    database_url: String,
    command_tx: mpsc::Sender<PostgresProxyCommand>,
    task: JoinHandle<std::io::Result<()>>,
}

#[cfg(feature = "postgres")]
impl PostgresConnectionProxy {
    pub(crate) async fn spawn(database_url: &str) -> std::io::Result<Self> {
        let mut proxied = Url::parse(database_url).map_err(std::io::Error::other)?;
        let host = proxied
            .host_str()
            .ok_or_else(|| std::io::Error::other("Postgres URL is missing a host"))?;
        let port = proxied
            .port_or_known_default()
            .ok_or_else(|| std::io::Error::other("Postgres URL is missing a port"))?;
        let target = tokio::net::lookup_host((host, port))
            .await?
            .next()
            .ok_or_else(|| std::io::Error::other("Postgres host did not resolve"))?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let proxy_addr = listener.local_addr()?;
        proxied
            .set_host(Some("127.0.0.1"))
            .map_err(|_| std::io::Error::other("failed to set Postgres proxy host"))?;
        proxied
            .set_port(Some(proxy_addr.port()))
            .map_err(|_| std::io::Error::other("failed to set Postgres proxy port"))?;

        let (command_tx, mut command_rx) = mpsc::channel(4);
        let task = tokio::spawn(async move {
            let mut available = true;
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    command = command_rx.recv() => {
                        match command {
                            Some(PostgresProxyCommand::SetAvailable {
                                available: next,
                                acknowledged,
                            }) => {
                                available = next;
                                if !available {
                                    connections.abort_all();
                                    while connections.join_next().await.is_some() {}
                                }
                                let _ = acknowledged.send(());
                            }
                            Some(PostgresProxyCommand::Shutdown { acknowledged }) => {
                                connections.abort_all();
                                while connections.join_next().await.is_some() {}
                                let _ = acknowledged.send(());
                                return Ok(());
                            }
                            None => return Ok(()),
                        }
                    }
                    accepted = listener.accept() => {
                        let (mut client, _) = accepted?;
                        if available {
                            connections.spawn(async move {
                                let mut server = TcpStream::connect(target).await?;
                                tokio::io::copy_bidirectional(&mut client, &mut server).await?;
                                Ok::<(), std::io::Error>(())
                            });
                        }
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });

        Ok(Self {
            database_url: proxied.into(),
            command_tx,
            task,
        })
    }

    pub(crate) async fn spawn_for_schema(
        database_url: &str,
        schema: &str,
    ) -> std::io::Result<Self> {
        let mut scoped = Url::parse(database_url).map_err(std::io::Error::other)?;
        scoped
            .query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema},public"));
        Self::spawn(scoped.as_str()).await
    }

    pub(crate) fn database_url(&self) -> &str {
        &self.database_url
    }

    pub(crate) async fn set_available(&self, available: bool) -> std::io::Result<()> {
        let (acknowledged, received) = oneshot::channel();
        self.command_tx
            .send(PostgresProxyCommand::SetAvailable {
                available,
                acknowledged,
            })
            .await
            .map_err(|_| std::io::Error::other("Postgres proxy task ended"))?;
        received
            .await
            .map_err(|_| std::io::Error::other("Postgres proxy did not acknowledge state"))
    }

    pub(crate) async fn shutdown(mut self) -> std::io::Result<()> {
        let (acknowledged, received) = oneshot::channel();
        self.command_tx
            .send(PostgresProxyCommand::Shutdown { acknowledged })
            .await
            .map_err(|_| std::io::Error::other("Postgres proxy task ended"))?;
        received
            .await
            .map_err(|_| std::io::Error::other("Postgres proxy did not acknowledge shutdown"))?;
        tokio::time::timeout(EXIT_TIMEOUT, &mut self.task)
            .await
            .map_err(|_| std::io::Error::other("Postgres proxy did not stop"))?
            .map_err(std::io::Error::other)?
    }
}
#[cfg(feature = "postgres")]
impl Drop for PostgresConnectionProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ServerAddrs {
    pub proxy: SocketAddr,
    pub admin: SocketAddr,
    pub metrics: SocketAddr,
}

pub fn cc_lb_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_cc-lb"))
}

pub fn serve_command(config_path: &Path) -> Command {
    let mut command = cc_lb_command();
    command.args(["serve", "--config"]).arg(config_path);
    command
}

pub fn base_server_config(
    addrs: ServerAddrs,
    storage_path: &Path,
    data_dir: &Path,
    key_env: &str,
    principal_id: &str,
) -> String {
    format!(
        r#"[listener]
proxy_addr = "{proxy}"
admin_addr = "{admin}"
metrics_addr = "{metrics}"

[runtime]
data_dir = "{data_dir}"

[storage]
kind = "sqlite"
path = "{storage_path}"

[aead]
key_env = "{key_env}"

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "{principal_id}"
upstream_kind = "anthropic_key"
"#,
        proxy = addrs.proxy,
        admin = addrs.admin,
        metrics = addrs.metrics,
        data_dir = data_dir.display(),
        storage_path = storage_path.display(),
    )
}

#[derive(Clone, Copy, Debug)]
pub enum Signal {
    Terminate,
    Hangup,
}

impl Signal {
    const fn kill_flag(self) -> &'static str {
        match self {
            Self::Terminate => "-TERM",
            Self::Hangup => "-HUP",
        }
    }
}

#[derive(Debug)]
pub struct CapturedOutput {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

struct CapturedPipe {
    bytes: Arc<Mutex<Vec<u8>>>,
    reader: Option<ThreadJoinHandle<()>>,
}

impl CapturedPipe {
    fn spawn<R>(mut pipe: R) -> Self
    where
        R: Read + Send + 'static,
    {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&bytes);
        let reader = std::thread::spawn(move || {
            let mut buffer = [0_u8; 4096];
            loop {
                match pipe.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(read) => {
                        if let Ok(mut captured) = sink.lock() {
                            captured.extend_from_slice(&buffer[..read]);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => return,
                }
            }
        });
        Self {
            bytes,
            reader: Some(reader),
        }
    }

    fn snapshot(&self) -> String {
        self.bytes
            .lock()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default()
    }

    fn finish(&mut self) -> String {
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
        self.snapshot()
    }
}

pub struct ChildProcess {
    child: Child,
    stdout: CapturedPipe,
    stderr: CapturedPipe,
}

impl ChildProcess {
    pub fn spawn(mut command: Command) -> std::io::Result<Self> {
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdout = CapturedPipe::spawn(
            child
                .stdout
                .take()
                .expect("piped child stdout is available"),
        );
        let stderr = CapturedPipe::spawn(
            child
                .stderr
                .take()
                .expect("piped child stderr is available"),
        );
        Ok(Self {
            child,
            stdout,
            stderr,
        })
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    pub fn assert_running(&mut self, context: &str) {
        if let Some(status) = self.try_wait().expect("poll child process") {
            panic!(
                "{context}: child exited early with {status}\nstdout:\n{}\nstderr:\n{}",
                self.stdout_snapshot(),
                self.stderr_snapshot()
            );
        }
    }

    pub fn send_signal(&mut self, signal: Signal) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.assert_running("deliver Unix signal");
            let output = Command::new("kill")
                .args([signal.kill_flag(), &self.pid().to_string()])
                .output()?;
            if output.status.success() {
                Ok(())
            } else {
                Err(std::io::Error::other(format!(
                    "kill {} {} failed with {}: {}",
                    signal.kill_flag(),
                    self.pid(),
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                )))
            }
        }
        #[cfg(not(unix))]
        {
            let _ = signal;
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix signals are unavailable on this platform",
            ))
        }
    }

    pub fn stdout_snapshot(&self) -> String {
        self.stdout.snapshot()
    }

    pub fn stderr_snapshot(&self) -> String {
        self.stderr.snapshot()
    }

    pub fn finish_after_exit(&mut self, status: ExitStatus) -> CapturedOutput {
        CapturedOutput {
            status,
            stdout: self.stdout.finish(),
            stderr: self.stderr.finish(),
        }
    }

    pub async fn wait_for_exit(&mut self, timeout: Duration) -> Result<CapturedOutput, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if let Some(status) = self
                .try_wait()
                .map_err(|error| format!("poll child process: {error}"))?
            {
                return Ok(self.finish_after_exit(status));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "child did not exit within {timeout:?}\nstdout:\n{}\nstderr:\n{}",
                    self.stdout_snapshot(),
                    self.stderr_snapshot()
                ));
            }
            poll.tick().await;
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        let _ = self.stdout.finish();
        let _ = self.stderr.finish();
    }
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

pub fn http_request(
    method: &str,
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Vec<u8> {
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    request.push_str(body);
    request.into_bytes()
}

pub async fn raw_http(
    addr: SocketAddr,
    request: &[u8],
    timeout: Duration,
) -> std::io::Result<HttpResponse> {
    tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_all(request).await?;
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await?;
        let response = String::from_utf8_lossy(&bytes);
        let status = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| std::io::Error::other("HTTP response has no status code"))?;
        let body = response.split_once("\r\n\r\n").map_or("", |(_, body)| body);
        Ok(HttpResponse {
            status,
            body: body.to_owned(),
        })
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "HTTP request timed out"))?
}

pub async fn wait_http_status(
    child: &mut ChildProcess,
    addr: SocketAddr,
    request: &[u8],
    expected_status: u16,
    timeout: Duration,
) -> Result<HttpResponse, String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(20));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = "request not attempted".to_owned();
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("poll child while waiting for HTTP: {error}"))?
        {
            return Err(format!(
                "child exited before HTTP {expected_status}: {status}\nstdout:\n{}\nstderr:\n{}",
                child.stdout_snapshot(),
                child.stderr_snapshot()
            ));
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(format!(
                "HTTP endpoint {addr} did not return {expected_status} within {timeout:?}; last {last}\nstdout:\n{}\nstderr:\n{}",
                child.stdout_snapshot(),
                child.stderr_snapshot()
            ));
        }
        let request_timeout = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(500));
        match raw_http(addr, request, request_timeout).await {
            Ok(response) if response.status == expected_status => return Ok(response),
            Ok(response) => {
                last = format!("status={} body={}", response.status, response.body);
            }
            Err(error) => last = format!("error={error}"),
        }
        poll.tick().await;
    }
}

pub async fn wait_stdout_contains(
    child: &mut ChildProcess,
    needle: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(10));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if child.stdout_snapshot().contains(needle) {
            return Ok(());
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("poll child while waiting for stdout: {error}"))?
        {
            return Err(format!(
                "child exited before stdout contained {needle:?}: {status}\nstdout:\n{}\nstderr:\n{}",
                child.stdout_snapshot(),
                child.stderr_snapshot()
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "stdout did not contain {needle:?} within {timeout:?}\nstdout:\n{}\nstderr:\n{}",
                child.stdout_snapshot(),
                child.stderr_snapshot()
            ));
        }
        poll.tick().await;
    }
}

pub struct RunningUpstream {
    pub addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for RunningUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn spawn_fake_anthropic() -> RunningUpstream {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake Anthropic listener");
    let addr = listener.local_addr().expect("fake Anthropic address");
    let task = tokio::spawn(async move {
        axum::serve(listener, fake_anthropic_app(AppConfig::default()))
            .await
            .expect("serve fake Anthropic");
    });
    RunningUpstream { addr, task }
}

pub async fn open_initialized_sqlite(
    path: &Path,
    backend_kind: BackendKind,
) -> Result<SqliteStorage, cc_lb_storage_api::StorageError> {
    let database_url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&database_url, cc_lb_testkit::fixed_clock(1_700_000_000)).await?;
    storage.initialize(backend_kind).await?;
    Ok(storage)
}
