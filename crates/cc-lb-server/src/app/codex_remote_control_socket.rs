use std::{collections::VecDeque, future::Future, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration};

use axum::{extract::ws::{Message, WebSocket}, http::HeaderMap};
use cc_lb_engine::Authenticated;
use futures_util::{Sink, SinkExt, Stream, StreamExt, stream::{SplitSink, SplitStream}};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_tungstenite::tungstenite::Message as ProviderMessage;
use tokio_util::sync::CancellationToken;

use super::super::ProxyState;
use super::handshake::ProviderSocket;
use super::transport::{fenced_relay, same_proof};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

enum FrameKind { Data, Control, Close(Option<CloseData>) }

#[derive(Clone, Debug, PartialEq, Eq)]
struct CloseData { code: u16, reason: bytes::Bytes }

#[derive(Debug)]
enum Incoming<M> { Data(M), Close(Option<CloseData>) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StopReason { Authentication, OwnerChanged, Overloaded, Transport, WriteTimeout, Draining, DrainTimeout, PeerErrorClose(u16) }

impl StopReason {
    fn close(self) -> CloseData {
        let (code, reason): (_, &'static [u8]) = match self {
            Self::Authentication | Self::OwnerChanged => (1008, b"remote control authorization changed"),
            Self::Overloaded => (1013, b"remote control relay buffer exhausted"),
            Self::Draining => (1001, b"remote control server draining"),
            Self::Transport | Self::WriteTimeout | Self::DrainTimeout => (1011, b"remote control relay interrupted"),
            Self::PeerErrorClose(code) => (code, b"remote control peer closed with an error"),
        };
        CloseData { code, reason: bytes::Bytes::from_static(reason) }
    }
}

#[derive(Default)]
struct ShutdownState { deadline: Option<Instant>, urgent: Option<StopReason>, error_close: Option<u16> }

#[derive(Clone, Default)]
struct Shutdown {
    state: Arc<parking_lot::Mutex<ShutdownState>>,
    closing: CancellationToken,
    urgent: CancellationToken,
}

impl Shutdown {
    fn begin_close(&self, frame: Option<&CloseData>) {
        let mut state = self.state.lock();
        state.deadline.get_or_insert_with(|| Instant::now() + CLOSE_TIMEOUT);
        // Defined protocol/data/policy/server/overload/gateway error codes.
        // Normal, going-away, restart and private/application codes carry no
        // inferred failure meaning. Never retain the peer's free-form reason.
        if let Some(frame) = frame {
            if matches!(frame.code, 1002 | 1003 | 1007 | 1008 | 1009 | 1010 | 1011 | 1013 | 1014) {
                state.error_close.get_or_insert(frame.code);
            }
        }
        drop(state);
        self.closing.cancel();
    }

    fn stop(&self, reason: StopReason) {
        self.state.lock().urgent.get_or_insert(reason);
        self.urgent.cancel();
    }

    fn reason(&self) -> Option<StopReason> {
        let state = self.state.lock();
        state.urgent.or_else(|| state.error_close.map(StopReason::PeerErrorClose))
    }

    fn cleanup_deadline(&self) -> Instant {
        self.state.lock().deadline.unwrap_or_else(|| Instant::now() + CLOSE_TIMEOUT)
    }

    async fn drain_deadline(&self) {
        self.closing.cancelled().await;
        let deadline = self.state.lock().deadline.expect("close establishes a deadline");
        tokio::time::sleep_until(deadline).await;
    }
}

// Publish Close while SplitStream still holds the socket's split lock. A
// concurrent sink must see this token before it can observe ClosedByPeer;
// publishing only after stream.next() returns leaves a cross-task race.
struct PeerSocket<S: Stream> {
    inner: S,
    peer_closed: CancellationToken,
    is_close: fn(&S::Item) -> bool,
}

impl<S: Stream + Unpin> Stream for PeerSocket<S> {
    type Item = S::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(message)) = &result {
            if (self.is_close)(message) { self.peer_closed.cancel(); }
        }
        result
    }
}

impl<S: Stream + Sink<M> + Unpin, M> Sink<M> for PeerSocket<S> {
    type Error = S::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, message: M) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(message)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

struct Reader<S: Stream> {
    task: JoinHandle<SplitStream<S>>,
    peer_closed: CancellationToken,
}

impl<S: Stream> Drop for Reader<S> {
    fn drop(&mut self) { self.task.abort(); }
}

// A close is an ordered end marker, not cancellation. The reader can observe
// hop-local control frames while a writer is blocked, but must hand off every
// earlier deliverable data frame before its end marker. Frames aimed at an
// already-closed destination are unsendable, not a failure of the other pump.
fn reader<S, M, E>(
    mut stream: SplitStream<S>,
    classify: fn(&M) -> FrameKind,
    shutdown: Shutdown,
    peer_closed: CancellationToken,
    capacity: usize,
) -> (Reader<S>, mpsc::Receiver<Incoming<M>>)
where S: Stream<Item = Result<M, E>> + Send + Unpin + 'static,
    M: Send + 'static, E: Send + 'static,
    SplitStream<S>: Send,
{
    let (tx, rx) = mpsc::channel(capacity);
    let observed_close = peer_closed.clone();
    let task = tokio::spawn(async move {
        let mut pending = VecDeque::new();
        loop {
            let message = tokio::select! {
                biased;
                _ = shutdown.urgent.cancelled() => break,
                permit = tx.reserve(), if !pending.is_empty() => {
                    let Ok(permit) = permit else { break; };
                    permit.send(Incoming::Data(pending.pop_front().expect("pending frame")));
                    continue;
                }
                message = stream.next() => message,
            };
            let Some(Ok(message)) = message else {
                shutdown.stop(StopReason::Transport);
                break;
            };
            match classify(&message) {
                FrameKind::Close(frame) => {
                    observed_close.cancel();
                    shutdown.begin_close(frame.as_ref());
                    let drain = async {
                        while let Some(message) = pending.pop_front() {
                            tx.send(Incoming::Data(message)).await.map_err(|_| ())?;
                        }
                        tx.send(Incoming::Close(frame)).await.map_err(|_| ())
                    };
                    tokio::select! {
                        biased;
                        _ = shutdown.urgent.cancelled() => {}
                        _ = drain => {}
                    }
                    break;
                }
                FrameKind::Control => continue,
                FrameKind::Data => {}
            }
            if pending.is_empty() {
                match tx.try_send(Incoming::Data(message)) {
                    Ok(()) => continue,
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                    Err(mpsc::error::TrySendError::Full(Incoming::Data(message))) => pending.push_back(message),
                    Err(mpsc::error::TrySendError::Full(Incoming::Close(_))) => unreachable!(),
                }
            } else if pending.len() < capacity {
                pending.push_back(message);
            } else {
                shutdown.stop(StopReason::Overloaded);
                break;
            }
        }
        stream
    });
    (Reader { task, peer_closed }, rx)
}

async fn write<S, M>(
    sink: &mut S,
    message: M,
    peer_closed: &CancellationToken,
) -> Result<(), StopReason>
where S: Sink<M> + Unpin,
{
    let result = tokio::select! {
        biased;
        _ = peer_closed.cancelled() => return Ok(()),
        result = tokio::time::timeout(WRITE_TIMEOUT, sink.send(message)) => result,
    };
    // Close may be observed while the sink returns SendAfterClosing. This
    // destination can no longer accept application data, but the other pump
    // must still drain the final frames that preceded that peer's Close.
    if peer_closed.is_cancelled() { return Ok(()); }
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(StopReason::Transport),
        Err(_) => Err(StopReason::WriteTimeout),
    }
}

async fn pump<S, I, O, Check, Checked>(
    sink: &mut S,
    messages: &mut mpsc::Receiver<Incoming<I>>,
    peer_closed: &CancellationToken,
    shutdown: &Shutdown,
    data: fn(I) -> O,
    close: fn(Option<CloseData>) -> O,
    mut check: Check,
)
where S: Sink<O> + Unpin,
    Check: FnMut() -> Checked,
    Checked: Future<Output = Result<(), StopReason>>,
{
    while let Some(message) = messages.recv().await {
        match message {
            Incoming::Data(message) => {
                if peer_closed.is_cancelled() { continue; }
                let checked = tokio::select! {
                    biased;
                    _ = peer_closed.cancelled() => continue,
                    checked = check() => checked,
                };
                if let Err(reason) = checked {
                    shutdown.stop(reason);
                    return;
                }
                if let Err(reason) = write(sink, data(message), peer_closed).await {
                    shutdown.stop(reason);
                    return;
                }
            }
            Incoming::Close(frame) => {
                // The library has already queued the hop-local close reply if
                // this destination also received Close. Never send it twice.
                if !peer_closed.is_cancelled() {
                    if let Err(reason) = write(sink, close(frame), peer_closed).await {
                        shutdown.stop(reason);
                    }
                }
                return;
            }
        }
    }
    shutdown.stop(StopReason::Transport);
}

async fn coordinate(
    upstream: impl Future<Output = ()>,
    downstream: impl Future<Output = ()>,
    draining: impl Future<Output = ()>,
    shutdown: &Shutdown,
) -> Option<StopReason> {
    tokio::select! {
        biased;
        _ = shutdown.urgent.cancelled() => {}
        _ = draining => shutdown.stop(StopReason::Draining),
        _ = shutdown.drain_deadline() => shutdown.stop(StopReason::DrainTimeout),
        _ = async { tokio::join!(upstream, downstream); } => {}
    }
    shutdown.reason()
}

async fn finish<S, M>(
    sink: SplitSink<S, M>,
    mut reader: Reader<S>,
    close: M,
    urgent: bool,
    deadline: Instant,
)
where S: Sink<M> + Stream + Unpin,
{
    if let Ok(Ok(stream)) = tokio::time::timeout_at(deadline, &mut reader.task).await {
        // Peer close or urgent cancellation can leave an unsendable application
        // frame in SplitSink's slot. Reunite discards only that destination's
        // slot before flushing its automatic reply, as in #1296.
        if let Ok(mut socket) = sink.reunite(stream) {
            if urgent && !reader.peer_closed.is_cancelled() {
                let _ = tokio::time::timeout_at(deadline, socket.send(close)).await;
            } else {
                let _ = tokio::time::timeout_at(deadline, socket.flush()).await;
            }
        }
    }
}

fn downstream_kind(message: &Message) -> FrameKind {
    match message {
        Message::Text(_) | Message::Binary(_) => FrameKind::Data,
        Message::Close(frame) => FrameKind::Close(frame.as_ref().map(|frame| CloseData {
            code: frame.code, reason: bytes::Bytes::from(frame.reason.clone()),
        })),
        Message::Ping(_) | Message::Pong(_) => FrameKind::Control,
    }
}

fn provider_kind(message: &ProviderMessage) -> FrameKind {
    match message {
        ProviderMessage::Text(_) | ProviderMessage::Binary(_) => FrameKind::Data,
        ProviderMessage::Close(frame) => FrameKind::Close(frame.as_ref().map(|frame| CloseData {
            code: frame.code.into(), reason: bytes::Bytes::from(frame.reason.clone()),
        })),
        _ => FrameKind::Control,
    }
}

fn provider_data(message: Message) -> ProviderMessage {
    match message {
        Message::Text(value) => ProviderMessage::Text(bytes::Bytes::from(value).try_into().expect("valid client text")),
        Message::Binary(value) => ProviderMessage::Binary(value),
        _ => unreachable!("reader only forwards data"),
    }
}

fn downstream_data(message: ProviderMessage) -> Message {
    match message {
        ProviderMessage::Text(value) => Message::Text(bytes::Bytes::from(value).try_into().expect("valid provider text")),
        ProviderMessage::Binary(value) => Message::Binary(value),
        _ => unreachable!("reader only forwards data"),
    }
}

fn downstream_close(frame: Option<CloseData>) -> Message {
    Message::Close(frame.map(|frame| axum::extract::ws::CloseFrame {
        code: frame.code, reason: frame.reason.try_into().expect("valid close reason"),
    }))
}

fn provider_close(frame: Option<CloseData>) -> ProviderMessage {
    ProviderMessage::Close(frame.map(|frame| tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code: frame.code.into(), reason: frame.reason.try_into().expect("valid close reason"),
    }))
}
async fn relay_sockets<D, P, Check, Checked>(
    downstream: D,
    provider: P,
    capacity: usize,
    check: Check,
    draining: impl Future<Output = ()>,
) -> Option<StopReason>
where D: Stream<Item = Result<Message, axum::Error>> + Sink<Message> + Send + Unpin + 'static,
    P: Stream<Item = Result<ProviderMessage, tokio_tungstenite::tungstenite::Error>>
        + Sink<ProviderMessage> + Send + Unpin + 'static,
    Check: FnMut() -> Checked,
    Checked: Future<Output = Result<(), StopReason>>,
{
    let downstream_closed = CancellationToken::new();
    let provider_closed = CancellationToken::new();
    let (mut downstream_sink, downstream_stream) = PeerSocket {
        inner: downstream, peer_closed: downstream_closed.clone(),
        is_close: |message| matches!(message, Ok(Message::Close(_))),
    }.split();
    let (mut provider_sink, provider_stream) = PeerSocket {
        inner: provider, peer_closed: provider_closed.clone(),
        is_close: |message| matches!(message, Ok(ProviderMessage::Close(_))),
    }.split();
    let shutdown = Shutdown::default();
    let (downstream_reader, mut downstream_rx) = reader(downstream_stream, downstream_kind,
        shutdown.clone(), downstream_closed, capacity);
    let (provider_reader, mut provider_rx) = reader(provider_stream, provider_kind,
        shutdown.clone(), provider_closed, capacity);
    let result = coordinate(
        pump(&mut provider_sink, &mut downstream_rx, &provider_reader.peer_closed, &shutdown,
            provider_data, provider_close, check),
        pump(&mut downstream_sink, &mut provider_rx, &downstream_reader.peer_closed, &shutdown,
            downstream_data, downstream_close, || std::future::ready(Ok(()))),
        draining, &shutdown,
    ).await;
    let deadline = shutdown.cleanup_deadline();
    // An error Close still drains gracefully and preserves its actual frame.
    // Only an urgent local failure may cancel drain or choose a proxy close.
    let urgent = shutdown.urgent.is_cancelled();
    let close = if urgent { result.map(StopReason::close) } else { None };
    tokio::join!(
        finish(downstream_sink, downstream_reader, downstream_close(close.clone()), urgent, deadline),
        finish(provider_sink, provider_reader, provider_close(close), urgent, deadline),
    );
    result
}

pub(super) async fn relay(
    downstream: WebSocket,
    provider: ProviderSocket,
    state: &ProxyState,
    auth: &Authenticated,
    headers: &HeaderMap,
) -> Option<StopReason> {
    let capacity = usize::try_from(state.websocket_bulkhead_config.semaphore_permits)
        .unwrap_or(usize::MAX).max(1);
    relay_sockets(downstream, provider, capacity, || async {
        let current = state.lifecycle.authenticate(headers).await.map_err(|_| StopReason::Authentication)?;
        if !same_proof(auth, &current) { return Err(StopReason::Authentication); }
        fenced_relay(state, &current, cc_lb_clock::unix_secs(state.clock.now())).await
            .map_err(|_| StopReason::OwnerChanged)?;
        Ok(())
    }, state.drain_controller.wait_for_draining()).await
}

#[cfg(test)]
#[path = "codex_remote_control_socket_tests.rs"]
mod tests;
