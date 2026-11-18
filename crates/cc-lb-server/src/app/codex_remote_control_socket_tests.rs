use super::*;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use futures_util::task::AtomicWaker;
use tokio::{io::{AsyncRead, AsyncWrite}, net::{TcpListener, TcpStream}, sync::oneshot};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

// QA: a blocked SplitSink slot, both bounded queues, and the ordered Close must
// survive together. Real WebSockets exercise both conversions, automatic pong,
// distinctive close metadata, and urgent server drain without timing sleeps.
#[derive(Default)]
struct WriteControl {
    blocked: CancellationToken,
    paused: AtomicBool,
    waker: AtomicWaker,
}

impl WriteControl {
    fn pause(&self) { self.paused.store(true, Ordering::SeqCst); }
    fn release(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.waker.wake();
    }
    fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        self.waker.register(cx.waker());
        if self.paused.load(Ordering::SeqCst) {
            self.blocked.cancel();
            Poll::Pending
        } else { Poll::Ready(()) }
    }
}

struct Controlled<S: Stream> {
    inner: S,
    writes: Arc<WriteControl>,
    closed: CancellationToken,
    is_close: fn(&S::Item) -> bool,
}

impl<S: Stream + Unpin> Stream for Controlled<S> {
    type Item = S::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(message)) = &result {
            if (self.is_close)(message) { self.closed.cancel(); }
        }
        result
    }
}

impl<S: Stream + Sink<M> + Unpin, M> Sink<M> for Controlled<S> {
    type Error = S::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.writes.poll_ready(cx).is_pending() { return Poll::Pending; }
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestMessage { Data(u8), Control, Close(Option<CloseData>) }

struct TestSocket {
    incoming: mpsc::UnboundedReceiver<TestMessage>,
    written: Arc<parking_lot::Mutex<Vec<TestMessage>>>,
}

impl Stream for TestSocket {
    type Item = Result<TestMessage, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.incoming.poll_recv(cx).map(|message| message.map(Ok))
    }
}

impl Sink<TestMessage> for TestSocket {
    type Error = Infallible;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
    fn start_send(self: Pin<&mut Self>, message: TestMessage) -> Result<(), Self::Error> {
        self.written.lock().push(message);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
}

fn test_socket() -> (mpsc::UnboundedSender<TestMessage>, TestSocket) {
    let (tx, incoming) = mpsc::unbounded_channel();
    (tx, TestSocket { incoming, written: Arc::new(parking_lot::Mutex::new(Vec::new())) })
}

fn test_kind(message: &TestMessage) -> FrameKind {
    match message {
        TestMessage::Data(_) => FrameKind::Data,
        TestMessage::Control => FrameKind::Control,
        TestMessage::Close(frame) => FrameKind::Close(frame.clone()),
    }
}

fn distinctive_close() -> CloseData {
    CloseData { code: 4013, reason: bytes::Bytes::from_static(b"all five frames precede this close") }
}

#[tokio::test]
async fn blocked_inflight_and_both_queues_drain_before_distinctive_close() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (_, source_stream) = source.split();
    let (mut sink, _) = Controlled {
        inner: destination, writes: control.clone(), closed: CancellationToken::new(), is_close: |_| false,
    }.split();
    let (_reader, mut messages) = reader(source_stream, test_kind, shutdown.clone(), CancellationToken::new(), 2);
    let peer_closed = CancellationToken::new();
    let expected_close = distinctive_close();
    let controller = async {
        tx.send(TestMessage::Data(1)).unwrap();
        control.blocked.cancelled().await;
        for n in 2..=5 { tx.send(TestMessage::Data(n)).unwrap(); }
        tx.send(TestMessage::Control).unwrap();
        tx.send(TestMessage::Close(Some(expected_close.clone()))).unwrap();
        shutdown.closing.cancelled().await;
        assert_eq!(shutdown.reason(), None, "Close must not cancel the blocked writer");
        assert!(written.lock().is_empty(), "the first frame is still in SplitSink's slot");
        control.release();
    };
    let (outcome, ()) = tokio::join!(
        coordinate(pump(&mut sink, &mut messages, &peer_closed, &shutdown, |m| m, TestMessage::Close,
            || std::future::ready(Ok(()))), std::future::ready(()), std::future::pending(), &shutdown),
        controller,
    );
    assert_eq!(outcome, None);
    let mut expected: Vec<_> = (1..=5).map(TestMessage::Data).collect();
    expected.push(TestMessage::Close(Some(expected_close)));
    assert_eq!(*written.lock(), expected, "neither pending nor in-flight data may disappear");
}

#[tokio::test]
async fn overload_is_urgent_instead_of_a_graceful_end_marker() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, stream) = source.split();
    let (_reader, _messages) = reader(stream, test_kind, shutdown.clone(), CancellationToken::new(), 1);
    for n in 1..=3 { tx.send(TestMessage::Data(n)).unwrap(); }
    tx.send(TestMessage::Close(Some(distinctive_close()))).unwrap();
    shutdown.urgent.cancelled().await;
    assert_eq!(shutdown.reason(), Some(StopReason::Overloaded));
    assert!(!shutdown.closing.is_cancelled());
}

#[tokio::test]
async fn transport_eof_is_urgent_instead_of_a_graceful_close() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, stream) = source.split();
    let (_reader, _messages) = reader(stream, test_kind, shutdown.clone(), CancellationToken::new(), 2);
    drop(tx);
    shutdown.urgent.cancelled().await;
    assert_eq!(shutdown.reason(), Some(StopReason::Transport));
    assert!(!shutdown.closing.is_cancelled());
}

#[tokio::test]
async fn every_data_frame_rechecks_authorization_and_owner_failure_is_urgent() {
    for failure in [StopReason::Authentication, StopReason::OwnerChanged] {
        let shutdown = Shutdown::default();
        let (_, mut sink) = test_socket();
        let written = sink.written.clone();
        let (tx, mut messages) = mpsc::channel(3);
        tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
        tx.send(Incoming::Data(TestMessage::Data(2))).await.unwrap();
        tx.send(Incoming::Close(Some(distinctive_close()))).await.unwrap();
        let mut checked = 0;
        let peer_closed = CancellationToken::new();
        let result = coordinate(
            pump(&mut sink, &mut messages, &peer_closed, &shutdown, |m| m, TestMessage::Close, || {
                checked += 1;
                std::future::ready(if checked == 1 { Ok(()) } else { Err(failure) })
            }),
            std::future::ready(()), std::future::pending(), &shutdown,
        ).await;
        assert_eq!(checked, 2);
        assert_eq!(result, Some(failure));
        assert_eq!(*written.lock(), [TestMessage::Data(1)]);
    }
}

#[tokio::test(start_paused = true)]
async fn one_total_drain_deadline_bounds_an_already_blocked_write() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (_, stream) = source.split();
    let (_reader, mut messages) = reader(stream, test_kind, shutdown.clone(), CancellationToken::new(), 2);
    let (mut sink, _) = Controlled {
        inner: destination, writes: control.clone(), closed: CancellationToken::new(), is_close: |_| false,
    }.split();
    let peer_closed = CancellationToken::new();
    let controller = async {
        tx.send(TestMessage::Data(1)).unwrap();
        control.blocked.cancelled().await;
        tx.send(TestMessage::Close(Some(distinctive_close()))).unwrap();
        shutdown.closing.cancelled().await;
        let began = Instant::now();
        tokio::time::advance(CLOSE_TIMEOUT).await;
        began
    };
    let (result, began) = tokio::join!(
        coordinate(pump(&mut sink, &mut messages, &peer_closed, &shutdown, |m| m, TestMessage::Close,
            || std::future::ready(Ok(()))), std::future::pending(), std::future::pending(), &shutdown),
        controller,
    );
    assert_eq!(result, Some(StopReason::DrainTimeout));
    assert_eq!(Instant::now(), began + CLOSE_TIMEOUT);
    assert_eq!(shutdown.cleanup_deadline(), began + CLOSE_TIMEOUT, "cleanup cannot start a second drain budget");
    assert!(written.lock().is_empty());
}

#[test]
fn text_and_binary_conversions_retain_the_original_bytes() {
    let bytes = bytes::Bytes::from_static(b"unchanged payload");
    let pointer = bytes.as_ptr();
    for message in [Message::Text(bytes.clone().try_into().unwrap()), Message::Binary(bytes.clone())] {
        let provider = provider_data(message);
        let view = match &provider {
            ProviderMessage::Text(value) => value.as_bytes(),
            ProviderMessage::Binary(value) => value.as_ref(),
            _ => unreachable!(),
        };
        assert_eq!(view.as_ptr(), pointer);
        let downstream = downstream_data(provider);
        let view = match &downstream {
            Message::Text(value) => value.as_bytes(),
            Message::Binary(value) => value.as_ref(),
            _ => unreachable!(),
        };
        assert_eq!(view.as_ptr(), pointer);
    }
}

#[test]
fn runtime_authentication_rail_stays_on_every_downstream_data_frame() {
    let source = include_str!("codex_remote_control_socket.rs");
    let rail = source.split("pub(super) async fn relay(").nth(1).unwrap();
    let authenticate = rail.find(".authenticate(headers)").unwrap();
    let proof = rail.find("same_proof(auth, &current)").unwrap();
    let fence = rail.find("fenced_relay(state, &current,").unwrap();
    assert!(authenticate < proof && proof < fence);
    assert!(rail.contains("relay_sockets("));
    assert!(rail.contains("downstream,"));
    assert!(source.contains("use super::handshake::ProviderSocket;"));
}

type NetworkClient = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct NetworkFixture {
    client: NetworkClient,
    provider: WebSocketStream<TcpStream>,
    downstream_writes: Arc<WriteControl>,
    provider_writes: Arc<WriteControl>,
    downstream_closed: CancellationToken,
    provider_closed: CancellationToken,
    draining: CancellationToken,
    completed: oneshot::Receiver<Option<StopReason>>,
    stop_server: CancellationToken,
    server: JoinHandle<()>,
}

impl NetworkFixture {
    async fn start() -> Self {
        let provider_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_url = format!("ws://{}/ws", provider_listener.local_addr().unwrap());
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("ws://{}/ws", proxy_listener.local_addr().unwrap());
        let downstream_writes = Arc::new(WriteControl::default());
        let provider_writes = Arc::new(WriteControl::default());
        let downstream_closed = CancellationToken::new();
        let provider_closed = CancellationToken::new();
        let draining = CancellationToken::new();
        let (completed_tx, completed) = oneshot::channel();
        let completed_tx = Arc::new(parking_lot::Mutex::new(Some(completed_tx)));
        let router = axum::Router::new().route("/ws", axum::routing::get({
            let downstream_writes = downstream_writes.clone();
            let provider_writes = provider_writes.clone();
            let downstream_closed = downstream_closed.clone();
            let provider_closed = provider_closed.clone();
            let draining = draining.clone();
            move |upgrade: axum::extract::ws::WebSocketUpgrade| {
                let provider_url = provider_url.clone();
                let downstream_writes = downstream_writes.clone();
                let provider_writes = provider_writes.clone();
                let downstream_closed = downstream_closed.clone();
                let provider_closed = provider_closed.clone();
                let draining = draining.clone();
                let completed_tx = completed_tx.clone();
                async move {
                    let (provider, _) = tokio_tungstenite::connect_async(provider_url).await.unwrap();
                    let completed_tx = completed_tx.lock().take().unwrap();
                    upgrade.on_upgrade(move |downstream| async move {
                        let downstream = Controlled {
                            inner: downstream, writes: downstream_writes, closed: downstream_closed,
                            is_close: |message| matches!(message, Ok(Message::Close(_))),
                        };
                        let provider = Controlled {
                            inner: provider, writes: provider_writes, closed: provider_closed,
                            is_close: |message| matches!(message, Ok(ProviderMessage::Close(_))),
                        };
                        let result = relay_sockets(downstream, provider, 2,
                            || std::future::ready(Ok(())), draining.cancelled_owned()).await;
                        completed_tx.send(result).unwrap();
                    })
                }
            }
        }));
        let stop_server = CancellationToken::new();
        let stop = stop_server.clone();
        let server = tokio::spawn(async move {
            axum::serve(proxy_listener, router).with_graceful_shutdown(stop.cancelled_owned()).await.unwrap();
        });
        let (client, provider) = tokio::join!(
            tokio_tungstenite::connect_async(proxy_url),
            async {
                let (socket, _) = provider_listener.accept().await.unwrap();
                tokio_tungstenite::accept_async(socket).await.unwrap()
            },
        );
        Self { client: client.unwrap().0, provider, downstream_writes, provider_writes,
            downstream_closed, provider_closed, draining, completed, stop_server, server }
    }

    async fn stop(self) {
        self.stop_server.cancel();
        self.server.await.unwrap();
    }
}

async fn assert_network_fifo_close<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    destination_writes: &WriteControl,
    source_closed: &CancellationToken,
    reason: &'static str,
) where S: AsyncRead + AsyncWrite + Unpin, D: AsyncRead + AsyncWrite + Unpin {
    let ping = bytes::Bytes::from_static(b"one hop-local pong");
    source.send(ProviderMessage::Ping(ping.clone())).await.unwrap();
    assert_eq!(source.next().await.unwrap().unwrap(), ProviderMessage::Pong(ping));
    destination_writes.pause();
    source.send(ProviderMessage::Text("first in-flight frame".into())).await.unwrap();
    destination_writes.blocked.cancelled().await;
    let blocked_ping = bytes::Bytes::from_static(b"pong while application writer is blocked");
    destination.send(ProviderMessage::Ping(blocked_ping.clone())).await.unwrap();
    assert_eq!(destination.next().await.unwrap().unwrap(), ProviderMessage::Pong(blocked_ping),
        "the blocked destination's reader must still flush its automatic pong");
    destination.send(ProviderMessage::Text("independent reverse-direction data".into())).await.unwrap();
    assert_eq!(source.next().await.unwrap().unwrap(),
        ProviderMessage::Text("independent reverse-direction data".into()),
        "the opposite application pump must progress while this writer is blocked");
    let frames = [
        ProviderMessage::Text("first in-flight frame".into()),
        ProviderMessage::Binary(bytes::Bytes::from_static(b"second queued frame")),
        ProviderMessage::Text("third queued frame".into()),
        ProviderMessage::Binary(bytes::Bytes::from_static(b"fourth pending frame")),
        ProviderMessage::Text("fifth pending frame".into()),
    ];
    for frame in &frames[1..] { source.feed(frame.clone()).await.unwrap(); }
    let close = ProviderMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code: 4013.into(), reason: reason.into(),
    }));
    source.feed(close.clone()).await.unwrap();
    source.flush().await.unwrap();
    source_closed.cancelled().await;
    destination_writes.release();
    for expected in frames {
        assert_eq!(destination.next().await.unwrap().unwrap(), expected,
            "every pre-Close frame must arrive, including both pending frames");
    }
    assert_eq!(destination.next().await.unwrap().unwrap(), close);
    destination.flush().await.unwrap();
    assert_eq!(source.next().await.unwrap().unwrap(), close,
        "the automatic close reply must preserve code/reason without an extra pong");
}

#[tokio::test]
async fn real_sockets_drain_both_directions_under_controlled_backpressure_and_reply_once() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_closes in [false, true] {
            let mut fixture = NetworkFixture::start().await;
            if provider_closes {
                assert_network_fifo_close(&mut fixture.provider, &mut fixture.client,
                    &fixture.downstream_writes, &fixture.provider_closed, "provider FIFO close").await;
            } else {
                assert_network_fifo_close(&mut fixture.client, &mut fixture.provider,
                    &fixture.provider_writes, &fixture.downstream_closed, "downstream FIFO close").await;
            }
            assert_eq!((&mut fixture.completed).await.unwrap(), None);
            fixture.stop().await;
        }
    }).await.expect("real socket close and all queued data complete within the bounded relay lifetime");
}

async fn assert_network_drain<S, D>(
    source: &mut WebSocketStream<S>, destination: &mut WebSocketStream<D>,
    destination_writes: &WriteControl, draining: &CancellationToken,
) where S: AsyncRead + AsyncWrite + Unpin, D: AsyncRead + AsyncWrite + Unpin {
    destination_writes.pause();
    source.send(ProviderMessage::Text("cancelled slot must not be retried".into())).await.unwrap();
    destination_writes.blocked.cancelled().await;
    draining.cancel();
    let expected = provider_close(Some(StopReason::Draining.close()));
    assert_eq!(source.next().await.unwrap().unwrap(), expected);
    source.flush().await.unwrap();
    destination_writes.release();
    assert_eq!(destination.next().await.unwrap().unwrap(), expected,
        "urgent drain discards the cancelled application slot before sending Close");
    destination.flush().await.unwrap();
}

#[tokio::test]
async fn real_server_drain_is_urgent_in_both_blocked_directions() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_blocked in [true, false] {
            let mut fixture = NetworkFixture::start().await;
            if provider_blocked {
                assert_network_drain(&mut fixture.client, &mut fixture.provider,
                    &fixture.provider_writes, &fixture.draining).await;
            } else {
                assert_network_drain(&mut fixture.provider, &mut fixture.client,
                    &fixture.downstream_writes, &fixture.draining).await;
            }
            assert_eq!((&mut fixture.completed).await.unwrap(), Some(StopReason::Draining));
            fixture.stop().await;
        }
    }).await.expect("server drain must cancel either blocked writer and close both hops");
}

struct CloseRacingSink {
    peer_closed: CancellationToken,
    attempted: usize,
}

impl Sink<TestMessage> for CloseRacingSink {
    type Error = &'static str;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, _: TestMessage) -> Result<(), Self::Error> {
        self.attempted += 1;
        self.peer_closed.cancel();
        Err("peer closed concurrently with this send")
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn send_failure_racing_peer_close_only_ceases_that_write_direction() {
    let shutdown = Shutdown::default();
    let peer_closed = CancellationToken::new();
    let mut sink = CloseRacingSink { peer_closed: peer_closed.clone(), attempted: 0 };
    let (tx, mut messages) = mpsc::channel(3);
    tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
    tx.send(Incoming::Data(TestMessage::Data(2))).await.unwrap();
    tx.send(Incoming::Close(Some(distinctive_close()))).await.unwrap();
    let mut checked = 0;
    let outcome = coordinate(
        pump(&mut sink, &mut messages, &peer_closed, &shutdown, |m| m, TestMessage::Close, || {
            checked += 1;
            std::future::ready(Ok(()))
        }),
        std::future::ready(()), std::future::pending(), &shutdown,
    ).await;
    assert_eq!(outcome, None, "peer close is not a global transport failure");
    assert_eq!(sink.attempted, 1, "queued data must not be sent after destination Close");
    assert_eq!(checked, 1, "no authorization operation is needed for an unsendable frame");
}

#[tokio::test(start_paused = true)]
async fn destination_close_cancels_its_blocked_send_without_waiting_for_write_timeout() {
    let shutdown = Shutdown::default();
    let peer_closed = CancellationToken::new();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (mut sink, _) = Controlled {
        inner: destination, writes: control.clone(), closed: CancellationToken::new(), is_close: |_| false,
    }.split();
    let (tx, mut messages) = mpsc::channel(2);
    tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
    tx.send(Incoming::Close(Some(distinctive_close()))).await.unwrap();
    let began = Instant::now();
    let controller = async {
        control.blocked.cancelled().await;
        peer_closed.cancel();
    };
    let (outcome, ()) = tokio::join!(
        coordinate(pump(&mut sink, &mut messages, &peer_closed, &shutdown, |m| m, TestMessage::Close,
            || std::future::ready(Ok(()))), std::future::ready(()), std::future::pending(), &shutdown),
        controller,
    );
    assert_eq!(outcome, None);
    assert_eq!(Instant::now(), began, "destination Close must wake the blocked send immediately");
    assert!(written.lock().is_empty());
}

async fn assert_duplex_peer_close<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    source_writes: &WriteControl,
    destination_writes: &WriteControl,
    source_closed: &CancellationToken,
    reason: &'static str,
) where S: AsyncRead + AsyncWrite + Unpin, D: AsyncRead + AsyncWrite + Unpin {
    source_writes.pause();
    destination_writes.pause();
    source.send(ProviderMessage::Text("final direction first frame".into())).await.unwrap();
    destination_writes.blocked.cancelled().await;
    destination.send(ProviderMessage::Text("opposite data cannot follow peer Close".into())).await.unwrap();
    source_writes.blocked.cancelled().await;
    let binary = ProviderMessage::Binary(bytes::Bytes::from_static(b"final binary before close"));
    let text = ProviderMessage::Text("final text before close".into());
    source.feed(binary.clone()).await.unwrap();
    source.feed(text.clone()).await.unwrap();
    let close = ProviderMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code: 4014.into(), reason: reason.into(),
    }));
    source.feed(close.clone()).await.unwrap();
    source.flush().await.unwrap();
    source_closed.cancelled().await;
    destination_writes.release();
    for expected in [ProviderMessage::Text("final direction first frame".into()), binary, text, close.clone()] {
        assert_eq!(destination.next().await.unwrap().unwrap(), expected,
            "an opposite in-flight application frame must not cancel the final-data FIFO");
    }
    destination.flush().await.unwrap();
    assert_eq!(source.next().await.unwrap().unwrap(), close,
        "the unsendable opposite slot must be discarded before the automatic close reply");
}

#[tokio::test]
async fn real_duplex_opposite_inflight_data_does_not_abort_final_text_binary_close() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_closes in [false, true] {
            let mut fixture = NetworkFixture::start().await;
            if provider_closes {
                assert_duplex_peer_close(&mut fixture.provider, &mut fixture.client,
                    &fixture.provider_writes, &fixture.downstream_writes, &fixture.provider_closed,
                    "provider closes with opposite data in flight").await;
            } else {
                assert_duplex_peer_close(&mut fixture.client, &mut fixture.provider,
                    &fixture.downstream_writes, &fixture.provider_writes, &fixture.downstream_closed,
                    "downstream closes with opposite data in flight").await;
            }
            assert_eq!((&mut fixture.completed).await.unwrap(), None,
                "peer Close must cease only its destination write direction");
            fixture.stop().await;
        }
    }).await.expect("duplex final-data drain must survive opposite unsendable application data");
}

#[tokio::test(start_paused = true)]
async fn peer_close_is_published_before_the_split_socket_can_be_reused() {
    let (tx, socket) = test_socket();
    let written = socket.written.clone();
    let peer_closed = CancellationToken::new();
    let (mut sink, mut stream) = PeerSocket {
        inner: socket, peer_closed: peer_closed.clone(),
        is_close: |message| matches!(message, Ok(TestMessage::Close(_))),
    }.split();
    tx.send(TestMessage::Close(Some(distinctive_close()))).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), TestMessage::Close(Some(distinctive_close())));
    assert!(peer_closed.is_cancelled(), "Close must be published inside the split socket lock");
    let began = Instant::now();
    assert_eq!(write(&mut sink, TestMessage::Data(1), &peer_closed).await, Ok(()));
    assert_eq!(Instant::now(), began);
    assert!(written.lock().is_empty(), "no application send may follow observed Close");
}

use super::*;
use futures_util::task::AtomicWaker;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

// QA: a blocked SplitSink slot, both bounded queues, and the ordered Close must
// survive together. Real WebSockets exercise both conversions, automatic pong,
// distinctive close metadata, and urgent server drain without timing sleeps.
#[derive(Default)]
struct WriteControl {
    blocked: CancellationToken,
    paused: AtomicBool,
    waker: AtomicWaker,
}

impl WriteControl {
    fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }
    fn release(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.waker.wake();
    }
    fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        self.waker.register(cx.waker());
        if self.paused.load(Ordering::SeqCst) {
            self.blocked.cancel();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

struct Controlled<S: Stream> {
    inner: S,
    writes: Arc<WriteControl>,
    closed: CancellationToken,
    is_close: fn(&S::Item) -> bool,
}

impl<S: Stream + Unpin> Stream for Controlled<S> {
    type Item = S::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(message)) = &result {
            if (self.is_close)(message) {
                self.closed.cancel();
            }
        }
        result
    }
}

impl<S: Stream + Sink<M> + Unpin, M> Sink<M> for Controlled<S> {
    type Error = S::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.writes.poll_ready(cx).is_pending() {
            return Poll::Pending;
        }
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestMessage {
    Data(u8),
    Control,
    Close(Option<CloseData>),
}

struct TestSocket {
    incoming: mpsc::UnboundedReceiver<TestMessage>,
    written: Arc<parking_lot::Mutex<Vec<TestMessage>>>,
}

impl Stream for TestSocket {
    type Item = Result<TestMessage, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.incoming.poll_recv(cx).map(|message| message.map(Ok))
    }
}

impl Sink<TestMessage> for TestSocket {
    type Error = Infallible;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, message: TestMessage) -> Result<(), Self::Error> {
        self.written.lock().push(message);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

fn test_socket() -> (mpsc::UnboundedSender<TestMessage>, TestSocket) {
    let (tx, incoming) = mpsc::unbounded_channel();
    (
        tx,
        TestSocket {
            incoming,
            written: Arc::new(parking_lot::Mutex::new(Vec::new())),
        },
    )
}

fn test_kind(message: &TestMessage) -> FrameKind {
    match message {
        TestMessage::Data(_) => FrameKind::Data,
        TestMessage::Control => FrameKind::Control,
        TestMessage::Close(frame) => FrameKind::Close(frame.clone()),
    }
}

fn distinctive_close() -> CloseData {
    CloseData {
        code: 4013,
        reason: bytes::Bytes::from_static(b"all five frames precede this close"),
    }
}

#[tokio::test]
async fn blocked_inflight_and_both_queues_drain_before_distinctive_close() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (_, source_stream) = source.split();
    let (mut sink, _) = Controlled {
        inner: destination,
        writes: control.clone(),
        closed: CancellationToken::new(),
        is_close: |_| false,
    }
    .split();
    let (_reader, mut messages) = reader(
        source_stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        2,
    );
    let peer_closed = CancellationToken::new();
    let expected_close = distinctive_close();
    let controller = async {
        tx.send(TestMessage::Data(1)).unwrap();
        control.blocked.cancelled().await;
        for n in 2..=5 {
            tx.send(TestMessage::Data(n)).unwrap();
        }
        tx.send(TestMessage::Control).unwrap();
        tx.send(TestMessage::Close(Some(expected_close.clone())))
            .unwrap();
        shutdown.closing.cancelled().await;
        assert_eq!(
            shutdown.reason(),
            None,
            "Close must not cancel the blocked writer"
        );
        assert!(
            written.lock().is_empty(),
            "the first frame is still in SplitSink's slot"
        );
        control.release();
    };
    let (outcome, ()) = tokio::join!(
        coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || std::future::ready(Ok(()))
            ),
            std::future::ready(()),
            std::future::pending(),
            &shutdown
        ),
        controller,
    );
    assert_eq!(outcome, None);
    let mut expected: Vec<_> = (1..=5).map(TestMessage::Data).collect();
    expected.push(TestMessage::Close(Some(expected_close)));
    assert_eq!(
        *written.lock(),
        expected,
        "neither pending nor in-flight data may disappear"
    );
}

#[tokio::test]
async fn overload_is_urgent_instead_of_a_graceful_end_marker() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, stream) = source.split();
    let (_reader, _messages) = reader(
        stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        1,
    );
    for n in 1..=3 {
        tx.send(TestMessage::Data(n)).unwrap();
    }
    tx.send(TestMessage::Close(Some(distinctive_close())))
        .unwrap();
    shutdown.urgent.cancelled().await;
    assert_eq!(shutdown.reason(), Some(StopReason::Overloaded));
    assert!(!shutdown.closing.is_cancelled());
}

#[tokio::test]
async fn transport_eof_is_urgent_instead_of_a_graceful_close() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, stream) = source.split();
    let (_reader, _messages) = reader(
        stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        2,
    );
    drop(tx);
    shutdown.urgent.cancelled().await;
    assert_eq!(shutdown.reason(), Some(StopReason::Transport));
    assert!(!shutdown.closing.is_cancelled());
}

#[tokio::test]
async fn every_data_frame_rechecks_authorization_and_owner_failure_is_urgent() {
    for failure in [StopReason::Authentication, StopReason::OwnerChanged] {
        let shutdown = Shutdown::default();
        let (_, mut sink) = test_socket();
        let written = sink.written.clone();
        let (tx, mut messages) = mpsc::channel(3);
        tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
        tx.send(Incoming::Data(TestMessage::Data(2))).await.unwrap();
        tx.send(Incoming::Close(Some(distinctive_close())))
            .await
            .unwrap();
        let mut checked = 0;
        let peer_closed = CancellationToken::new();
        let result = coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || {
                    checked += 1;
                    std::future::ready(if checked == 1 { Ok(()) } else { Err(failure) })
                },
            ),
            std::future::ready(()),
            std::future::pending(),
            &shutdown,
        )
        .await;
        assert_eq!(checked, 2);
        assert_eq!(result, Some(failure));
        assert_eq!(*written.lock(), [TestMessage::Data(1)]);
    }
}

#[tokio::test(start_paused = true)]
async fn one_total_drain_deadline_bounds_an_already_blocked_write() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (_, stream) = source.split();
    let (_reader, mut messages) = reader(
        stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        2,
    );
    let (mut sink, _) = Controlled {
        inner: destination,
        writes: control.clone(),
        closed: CancellationToken::new(),
        is_close: |_| false,
    }
    .split();
    let peer_closed = CancellationToken::new();
    let controller = async {
        tx.send(TestMessage::Data(1)).unwrap();
        control.blocked.cancelled().await;
        tx.send(TestMessage::Close(Some(distinctive_close())))
            .unwrap();
        shutdown.closing.cancelled().await;
        let began = Instant::now();
        tokio::time::advance(CLOSE_TIMEOUT).await;
        began
    };
    let (result, began) = tokio::join!(
        coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || std::future::ready(Ok(()))
            ),
            std::future::pending(),
            std::future::pending(),
            &shutdown
        ),
        controller,
    );
    assert_eq!(result, Some(StopReason::DrainTimeout));
    assert_eq!(Instant::now(), began + CLOSE_TIMEOUT);
    assert_eq!(
        shutdown.cleanup_deadline(),
        began + CLOSE_TIMEOUT,
        "cleanup cannot start a second drain budget"
    );
    assert!(written.lock().is_empty());
}

#[test]
fn text_and_binary_conversions_retain_the_original_bytes() {
    let bytes = bytes::Bytes::from_static(b"unchanged payload");
    let pointer = bytes.as_ptr();
    for message in [
        Message::Text(bytes.clone().try_into().unwrap()),
        Message::Binary(bytes.clone()),
    ] {
        let provider = provider_data(message);
        let view = match &provider {
            ProviderMessage::Text(value) => value.as_bytes(),
            ProviderMessage::Binary(value) => value.as_ref(),
            _ => unreachable!(),
        };
        assert_eq!(view.as_ptr(), pointer);
        let downstream = downstream_data(provider);
        let view = match &downstream {
            Message::Text(value) => value.as_bytes(),
            Message::Binary(value) => value.as_ref(),
            _ => unreachable!(),
        };
        assert_eq!(view.as_ptr(), pointer);
    }
}

#[test]
fn runtime_authentication_rail_stays_on_every_downstream_data_frame() {
    let source = include_str!("codex_remote_control_socket.rs");
    let rail = source.split("pub(super) async fn relay(").nth(1).unwrap();
    let authenticate = rail.find(".authenticate(headers)").unwrap();
    let proof = rail.find("same_proof(auth, &current)").unwrap();
    let fence = rail.find("fenced_relay(state, &current,").unwrap();
    assert!(authenticate < proof && proof < fence);
    assert!(rail.contains("relay_sockets("));
    assert!(rail.contains("downstream,"));
    assert!(source.contains("use super::handshake::ProviderSocket;"));
}

type NetworkClient = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct NetworkFixture {
    client: NetworkClient,
    provider: WebSocketStream<TcpStream>,
    downstream_writes: Arc<WriteControl>,
    provider_writes: Arc<WriteControl>,
    downstream_closed: CancellationToken,
    provider_closed: CancellationToken,
    draining: CancellationToken,
    completed: oneshot::Receiver<Option<StopReason>>,
    stop_server: CancellationToken,
    server: JoinHandle<()>,
}

impl NetworkFixture {
    async fn start() -> Self {
        let provider_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_url = format!("ws://{}/ws", provider_listener.local_addr().unwrap());
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("ws://{}/ws", proxy_listener.local_addr().unwrap());
        let downstream_writes = Arc::new(WriteControl::default());
        let provider_writes = Arc::new(WriteControl::default());
        let downstream_closed = CancellationToken::new();
        let provider_closed = CancellationToken::new();
        let draining = CancellationToken::new();
        let (completed_tx, completed) = oneshot::channel();
        let completed_tx = Arc::new(parking_lot::Mutex::new(Some(completed_tx)));
        let router = axum::Router::new().route(
            "/ws",
            axum::routing::get({
                let downstream_writes = downstream_writes.clone();
                let provider_writes = provider_writes.clone();
                let downstream_closed = downstream_closed.clone();
                let provider_closed = provider_closed.clone();
                let draining = draining.clone();
                move |upgrade: axum::extract::ws::WebSocketUpgrade| {
                    let provider_url = provider_url.clone();
                    let downstream_writes = downstream_writes.clone();
                    let provider_writes = provider_writes.clone();
                    let downstream_closed = downstream_closed.clone();
                    let provider_closed = provider_closed.clone();
                    let draining = draining.clone();
                    let completed_tx = completed_tx.clone();
                    async move {
                        let (provider, _) = tokio_tungstenite::connect_async(provider_url)
                            .await
                            .unwrap();
                        let completed_tx = completed_tx.lock().take().unwrap();
                        upgrade.on_upgrade(move |downstream| async move {
                            let downstream = Controlled {
                                inner: downstream,
                                writes: downstream_writes,
                                closed: downstream_closed,
                                is_close: |message| matches!(message, Ok(Message::Close(_))),
                            };
                            let provider = Controlled {
                                inner: provider,
                                writes: provider_writes,
                                closed: provider_closed,
                                is_close: |message| {
                                    matches!(message, Ok(ProviderMessage::Close(_)))
                                },
                            };
                            let result = relay_sockets(
                                downstream,
                                provider,
                                2,
                                || std::future::ready(Ok(())),
                                draining.cancelled_owned(),
                            )
                            .await;
                            completed_tx.send(result).unwrap();
                        })
                    }
                }
            }),
        );
        let stop_server = CancellationToken::new();
        let stop = stop_server.clone();
        let server = tokio::spawn(async move {
            axum::serve(proxy_listener, router)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .unwrap();
        });
        let (client, provider) = tokio::join!(tokio_tungstenite::connect_async(proxy_url), async {
            let (socket, _) = provider_listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(socket).await.unwrap()
        },);
        Self {
            client: client.unwrap().0,
            provider,
            downstream_writes,
            provider_writes,
            downstream_closed,
            provider_closed,
            draining,
            completed,
            stop_server,
            server,
        }
    }

    async fn stop(self) {
        self.stop_server.cancel();
        self.server.await.unwrap();
    }
}

async fn assert_network_fifo_close<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    destination_writes: &WriteControl,
    source_closed: &CancellationToken,
    reason: &'static str,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let ping = bytes::Bytes::from_static(b"one hop-local pong");
    source
        .send(ProviderMessage::Ping(ping.clone()))
        .await
        .unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        ProviderMessage::Pong(ping)
    );
    destination_writes.pause();
    source
        .send(ProviderMessage::Text("first in-flight frame".into()))
        .await
        .unwrap();
    destination_writes.blocked.cancelled().await;
    let blocked_ping = bytes::Bytes::from_static(b"pong while application writer is blocked");
    destination
        .send(ProviderMessage::Ping(blocked_ping.clone()))
        .await
        .unwrap();
    assert_eq!(
        destination.next().await.unwrap().unwrap(),
        ProviderMessage::Pong(blocked_ping),
        "the blocked destination's reader must still flush its automatic pong"
    );
    destination
        .send(ProviderMessage::Text(
            "independent reverse-direction data".into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        ProviderMessage::Text("independent reverse-direction data".into()),
        "the opposite application pump must progress while this writer is blocked"
    );
    let frames = [
        ProviderMessage::Text("first in-flight frame".into()),
        ProviderMessage::Binary(bytes::Bytes::from_static(b"second queued frame")),
        ProviderMessage::Text("third queued frame".into()),
        ProviderMessage::Binary(bytes::Bytes::from_static(b"fourth pending frame")),
        ProviderMessage::Text("fifth pending frame".into()),
    ];
    for frame in &frames[1..] {
        source.feed(frame.clone()).await.unwrap();
    }
    let close =
        ProviderMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: 4013.into(),
            reason: reason.into(),
        }));
    source.feed(close.clone()).await.unwrap();
    source.flush().await.unwrap();
    source_closed.cancelled().await;
    destination_writes.release();
    for expected in frames {
        assert_eq!(
            destination.next().await.unwrap().unwrap(),
            expected,
            "every pre-Close frame must arrive, including both pending frames"
        );
    }
    assert_eq!(destination.next().await.unwrap().unwrap(), close);
    destination.flush().await.unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        close,
        "the automatic close reply must preserve code/reason without an extra pong"
    );
}

#[tokio::test]
async fn real_sockets_drain_both_directions_under_controlled_backpressure_and_reply_once() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_closes in [false, true] {
            let mut fixture = NetworkFixture::start().await;
            if provider_closes {
                assert_network_fifo_close(
                    &mut fixture.provider,
                    &mut fixture.client,
                    &fixture.downstream_writes,
                    &fixture.provider_closed,
                    "provider FIFO close",
                )
                .await;
            } else {
                assert_network_fifo_close(
                    &mut fixture.client,
                    &mut fixture.provider,
                    &fixture.provider_writes,
                    &fixture.downstream_closed,
                    "downstream FIFO close",
                )
                .await;
            }
            assert_eq!((&mut fixture.completed).await.unwrap(), None);
            fixture.stop().await;
        }
    })
    .await
    .expect("real socket close and all queued data complete within the bounded relay lifetime");
}

async fn assert_network_drain<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    destination_writes: &WriteControl,
    draining: &CancellationToken,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    destination_writes.pause();
    source
        .send(ProviderMessage::Text(
            "cancelled slot must not be retried".into(),
        ))
        .await
        .unwrap();
    destination_writes.blocked.cancelled().await;
    draining.cancel();
    let expected = provider_close(Some(StopReason::Draining.close()));
    assert_eq!(source.next().await.unwrap().unwrap(), expected);
    source.flush().await.unwrap();
    destination_writes.release();
    assert_eq!(
        destination.next().await.unwrap().unwrap(),
        expected,
        "urgent drain discards the cancelled application slot before sending Close"
    );
    destination.flush().await.unwrap();
}

#[tokio::test]
async fn real_server_drain_is_urgent_in_both_blocked_directions() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_blocked in [true, false] {
            let mut fixture = NetworkFixture::start().await;
            if provider_blocked {
                assert_network_drain(
                    &mut fixture.client,
                    &mut fixture.provider,
                    &fixture.provider_writes,
                    &fixture.draining,
                )
                .await;
            } else {
                assert_network_drain(
                    &mut fixture.provider,
                    &mut fixture.client,
                    &fixture.downstream_writes,
                    &fixture.draining,
                )
                .await;
            }
            assert_eq!(
                (&mut fixture.completed).await.unwrap(),
                Some(StopReason::Draining)
            );
            fixture.stop().await;
        }
    })
    .await
    .expect("server drain must cancel either blocked writer and close both hops");
}

struct CloseRacingSink {
    peer_closed: CancellationToken,
    attempted: usize,
}

impl Sink<TestMessage> for CloseRacingSink {
    type Error = &'static str;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, _: TestMessage) -> Result<(), Self::Error> {
        self.attempted += 1;
        self.peer_closed.cancel();
        Err("peer closed concurrently with this send")
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn send_failure_racing_peer_close_only_ceases_that_write_direction() {
    let shutdown = Shutdown::default();
    let peer_closed = CancellationToken::new();
    let mut sink = CloseRacingSink {
        peer_closed: peer_closed.clone(),
        attempted: 0,
    };
    let (tx, mut messages) = mpsc::channel(3);
    tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
    tx.send(Incoming::Data(TestMessage::Data(2))).await.unwrap();
    tx.send(Incoming::Close(Some(distinctive_close())))
        .await
        .unwrap();
    let mut checked = 0;
    let outcome = coordinate(
        pump(
            &mut sink,
            &mut messages,
            &peer_closed,
            &shutdown,
            |m| m,
            TestMessage::Close,
            || {
                checked += 1;
                std::future::ready(Ok(()))
            },
        ),
        std::future::ready(()),
        std::future::pending(),
        &shutdown,
    )
    .await;
    assert_eq!(
        outcome, None,
        "peer close is not a global transport failure"
    );
    assert_eq!(
        sink.attempted, 1,
        "queued data must not be sent after destination Close"
    );
    assert_eq!(
        checked, 1,
        "no authorization operation is needed for an unsendable frame"
    );
}

#[tokio::test(start_paused = true)]
async fn destination_close_cancels_its_blocked_send_without_waiting_for_write_timeout() {
    let shutdown = Shutdown::default();
    let peer_closed = CancellationToken::new();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (mut sink, _) = Controlled {
        inner: destination,
        writes: control.clone(),
        closed: CancellationToken::new(),
        is_close: |_| false,
    }
    .split();
    let (tx, mut messages) = mpsc::channel(2);
    tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
    tx.send(Incoming::Close(Some(distinctive_close())))
        .await
        .unwrap();
    let began = Instant::now();
    let controller = async {
        control.blocked.cancelled().await;
        peer_closed.cancel();
    };
    let (outcome, ()) = tokio::join!(
        coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || std::future::ready(Ok(()))
            ),
            std::future::ready(()),
            std::future::pending(),
            &shutdown
        ),
        controller,
    );
    assert_eq!(outcome, None);
    assert_eq!(
        Instant::now(),
        began,
        "destination Close must wake the blocked send immediately"
    );
    assert!(written.lock().is_empty());
}

async fn assert_duplex_peer_close<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    source_writes: &WriteControl,
    destination_writes: &WriteControl,
    source_closed: &CancellationToken,
    reason: &'static str,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    source_writes.pause();
    destination_writes.pause();
    source
        .send(ProviderMessage::Text("final direction first frame".into()))
        .await
        .unwrap();
    destination_writes.blocked.cancelled().await;
    destination
        .send(ProviderMessage::Text(
            "opposite data cannot follow peer Close".into(),
        ))
        .await
        .unwrap();
    source_writes.blocked.cancelled().await;
    let binary = ProviderMessage::Binary(bytes::Bytes::from_static(b"final binary before close"));
    let text = ProviderMessage::Text("final text before close".into());
    source.feed(binary.clone()).await.unwrap();
    source.feed(text.clone()).await.unwrap();
    let close =
        ProviderMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: 4014.into(),
            reason: reason.into(),
        }));
    source.feed(close.clone()).await.unwrap();
    source.flush().await.unwrap();
    source_closed.cancelled().await;
    destination_writes.release();
    for expected in [
        ProviderMessage::Text("final direction first frame".into()),
        binary,
        text,
        close.clone(),
    ] {
        assert_eq!(
            destination.next().await.unwrap().unwrap(),
            expected,
            "an opposite in-flight application frame must not cancel the final-data FIFO"
        );
    }
    destination.flush().await.unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        close,
        "the unsendable opposite slot must be discarded before the automatic close reply"
    );
}

#[tokio::test]
async fn real_duplex_opposite_inflight_data_does_not_abort_final_text_binary_close() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_closes in [false, true] {
            let mut fixture = NetworkFixture::start().await;
            if provider_closes {
                assert_duplex_peer_close(
                    &mut fixture.provider,
                    &mut fixture.client,
                    &fixture.provider_writes,
                    &fixture.downstream_writes,
                    &fixture.provider_closed,
                    "provider closes with opposite data in flight",
                )
                .await;
            } else {
                assert_duplex_peer_close(
                    &mut fixture.client,
                    &mut fixture.provider,
                    &fixture.downstream_writes,
                    &fixture.provider_writes,
                    &fixture.downstream_closed,
                    "downstream closes with opposite data in flight",
                )
                .await;
            }
            assert_eq!(
                (&mut fixture.completed).await.unwrap(),
                None,
                "peer Close must cease only its destination write direction"
            );
            fixture.stop().await;
        }
    })
    .await
    .expect("duplex final-data drain must survive opposite unsendable application data");
}

#[tokio::test(start_paused = true)]
async fn peer_close_is_published_before_the_split_socket_can_be_reused() {
    let (tx, socket) = test_socket();
    let written = socket.written.clone();
    let peer_closed = CancellationToken::new();
    let (mut sink, mut stream) = PeerSocket {
        inner: socket,
        peer_closed: peer_closed.clone(),
        is_close: |message| matches!(message, Ok(TestMessage::Close(_))),
    }
    .split();
    tx.send(TestMessage::Close(Some(distinctive_close())))
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        TestMessage::Close(Some(distinctive_close()))
    );
    assert!(
        peer_closed.is_cancelled(),
        "Close must be published inside the split socket lock"
    );
    let began = Instant::now();
    assert_eq!(
        write(&mut sink, TestMessage::Data(1), &peer_closed).await,
        Ok(())
    );
    assert_eq!(Instant::now(), began);
    assert!(
        written.lock().is_empty(),
        "no application send may follow observed Close"
    );
}

use super::*;
use futures_util::task::AtomicWaker;
use std::{
    convert::Infallible,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

// QA: a blocked SplitSink slot, both bounded queues, and the ordered Close must
// survive together. Real WebSockets exercise both conversions, automatic pong,
// distinctive close metadata, and urgent server drain without timing sleeps.
#[derive(Default)]
struct WriteControl {
    blocked: CancellationToken,
    paused: AtomicBool,
    waker: AtomicWaker,
}

impl WriteControl {
    fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }
    fn release(&self) {
        self.paused.store(false, Ordering::SeqCst);
        self.waker.wake();
    }
    fn poll_ready(&self, cx: &Context<'_>) -> Poll<()> {
        self.waker.register(cx.waker());
        if self.paused.load(Ordering::SeqCst) {
            self.blocked.cancel();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

struct Controlled<S: Stream> {
    inner: S,
    writes: Arc<WriteControl>,
    closed: CancellationToken,
    is_close: fn(&S::Item) -> bool,
}

impl<S: Stream + Unpin> Stream for Controlled<S> {
    type Item = S::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(message)) = &result {
            if (self.is_close)(message) {
                self.closed.cancel();
            }
        }
        result
    }
}

impl<S: Stream + Sink<M> + Unpin, M> Sink<M> for Controlled<S> {
    type Error = S::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.writes.poll_ready(cx).is_pending() {
            return Poll::Pending;
        }
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum TestMessage {
    Data(u8),
    Control,
    Close(Option<CloseData>),
}

struct TestSocket {
    incoming: mpsc::UnboundedReceiver<TestMessage>,
    written: Arc<parking_lot::Mutex<Vec<TestMessage>>>,
}

impl Stream for TestSocket {
    type Item = Result<TestMessage, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.incoming.poll_recv(cx).map(|message| message.map(Ok))
    }
}

impl Sink<TestMessage> for TestSocket {
    type Error = Infallible;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, message: TestMessage) -> Result<(), Self::Error> {
        self.written.lock().push(message);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

fn test_socket() -> (mpsc::UnboundedSender<TestMessage>, TestSocket) {
    let (tx, incoming) = mpsc::unbounded_channel();
    (
        tx,
        TestSocket {
            incoming,
            written: Arc::new(parking_lot::Mutex::new(Vec::new())),
        },
    )
}

fn test_kind(message: &TestMessage) -> FrameKind {
    match message {
        TestMessage::Data(_) => FrameKind::Data,
        TestMessage::Control => FrameKind::Control,
        TestMessage::Close(frame) => FrameKind::Close(frame.clone()),
    }
}

fn distinctive_close() -> CloseData {
    CloseData {
        code: 4013,
        reason: bytes::Bytes::from_static(b"all five frames precede this close"),
    }
}

#[tokio::test]
async fn blocked_inflight_and_both_queues_drain_before_distinctive_close() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (_, source_stream) = source.split();
    let (mut sink, _) = Controlled {
        inner: destination,
        writes: control.clone(),
        closed: CancellationToken::new(),
        is_close: |_| false,
    }
    .split();
    let (_reader, mut messages) = reader(
        source_stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        2,
    );
    let peer_closed = CancellationToken::new();
    let expected_close = distinctive_close();
    let controller = async {
        tx.send(TestMessage::Data(1)).unwrap();
        control.blocked.cancelled().await;
        for n in 2..=5 {
            tx.send(TestMessage::Data(n)).unwrap();
        }
        tx.send(TestMessage::Control).unwrap();
        tx.send(TestMessage::Close(Some(expected_close.clone())))
            .unwrap();
        shutdown.closing.cancelled().await;
        assert_eq!(
            shutdown.reason(),
            None,
            "Close must not cancel the blocked writer"
        );
        assert!(
            written.lock().is_empty(),
            "the first frame is still in SplitSink's slot"
        );
        control.release();
    };
    let (outcome, ()) = tokio::join!(
        coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || std::future::ready(Ok(()))
            ),
            std::future::ready(()),
            std::future::pending(),
            &shutdown
        ),
        controller,
    );
    assert_eq!(outcome, None);
    let mut expected: Vec<_> = (1..=5).map(TestMessage::Data).collect();
    expected.push(TestMessage::Close(Some(expected_close)));
    assert_eq!(
        *written.lock(),
        expected,
        "neither pending nor in-flight data may disappear"
    );
}

#[tokio::test]
async fn overload_is_urgent_instead_of_a_graceful_end_marker() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, stream) = source.split();
    let (_reader, _messages) = reader(
        stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        1,
    );
    for n in 1..=3 {
        tx.send(TestMessage::Data(n)).unwrap();
    }
    tx.send(TestMessage::Close(Some(distinctive_close())))
        .unwrap();
    shutdown.urgent.cancelled().await;
    assert_eq!(shutdown.reason(), Some(StopReason::Overloaded));
    assert!(!shutdown.closing.is_cancelled());
}

#[tokio::test]
async fn transport_eof_is_urgent_instead_of_a_graceful_close() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, stream) = source.split();
    let (_reader, _messages) = reader(
        stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        2,
    );
    drop(tx);
    shutdown.urgent.cancelled().await;
    assert_eq!(shutdown.reason(), Some(StopReason::Transport));
    assert!(!shutdown.closing.is_cancelled());
}

#[tokio::test]
async fn every_data_frame_rechecks_authorization_and_owner_failure_is_urgent() {
    for failure in [StopReason::Authentication, StopReason::OwnerChanged] {
        let shutdown = Shutdown::default();
        let (_, mut sink) = test_socket();
        let written = sink.written.clone();
        let (tx, mut messages) = mpsc::channel(3);
        tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
        tx.send(Incoming::Data(TestMessage::Data(2))).await.unwrap();
        tx.send(Incoming::Close(Some(distinctive_close())))
            .await
            .unwrap();
        let mut checked = 0;
        let peer_closed = CancellationToken::new();
        let result = coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || {
                    checked += 1;
                    std::future::ready(if checked == 1 { Ok(()) } else { Err(failure) })
                },
            ),
            std::future::ready(()),
            std::future::pending(),
            &shutdown,
        )
        .await;
        assert_eq!(checked, 2);
        assert_eq!(result, Some(failure));
        assert_eq!(*written.lock(), [TestMessage::Data(1)]);
    }
}

#[tokio::test(start_paused = true)]
async fn one_total_drain_deadline_bounds_an_already_blocked_write() {
    let shutdown = Shutdown::default();
    let (tx, source) = test_socket();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (_, stream) = source.split();
    let (_reader, mut messages) = reader(
        stream,
        test_kind,
        shutdown.clone(),
        CancellationToken::new(),
        2,
    );
    let (mut sink, _) = Controlled {
        inner: destination,
        writes: control.clone(),
        closed: CancellationToken::new(),
        is_close: |_| false,
    }
    .split();
    let peer_closed = CancellationToken::new();
    let controller = async {
        tx.send(TestMessage::Data(1)).unwrap();
        control.blocked.cancelled().await;
        tx.send(TestMessage::Close(Some(distinctive_close())))
            .unwrap();
        shutdown.closing.cancelled().await;
        let began = Instant::now();
        tokio::time::advance(CLOSE_TIMEOUT).await;
        began
    };
    let (result, began) = tokio::join!(
        coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || std::future::ready(Ok(()))
            ),
            std::future::pending(),
            std::future::pending(),
            &shutdown
        ),
        controller,
    );
    assert_eq!(result, Some(StopReason::DrainTimeout));
    assert_eq!(Instant::now(), began + CLOSE_TIMEOUT);
    assert_eq!(
        shutdown.cleanup_deadline(),
        began + CLOSE_TIMEOUT,
        "cleanup cannot start a second drain budget"
    );
    assert!(written.lock().is_empty());
}

#[test]
fn text_and_binary_conversions_retain_the_original_bytes() {
    let bytes = bytes::Bytes::from_static(b"unchanged payload");
    let pointer = bytes.as_ptr();
    for message in [
        Message::Text(bytes.clone().try_into().unwrap()),
        Message::Binary(bytes.clone()),
    ] {
        let provider = provider_data(message);
        let view = match &provider {
            ProviderMessage::Text(value) => value.as_bytes(),
            ProviderMessage::Binary(value) => value.as_ref(),
            _ => unreachable!(),
        };
        assert_eq!(view.as_ptr(), pointer);
        let downstream = downstream_data(provider);
        let view = match &downstream {
            Message::Text(value) => value.as_bytes(),
            Message::Binary(value) => value.as_ref(),
            _ => unreachable!(),
        };
        assert_eq!(view.as_ptr(), pointer);
    }
}

#[test]
fn runtime_authentication_rail_stays_on_every_downstream_data_frame() {
    let source = include_str!("codex_remote_control_socket.rs");
    let rail = source.split("pub(super) async fn relay(").nth(1).unwrap();
    let authenticate = rail.find(".authenticate(headers)").unwrap();
    let proof = rail.find("same_proof(auth, &current)").unwrap();
    let fence = rail.find("fenced_relay(state, &current,").unwrap();
    assert!(authenticate < proof && proof < fence);
    assert!(rail.contains("relay_sockets("));
    assert!(rail.contains("downstream,"));
    assert!(source.contains("use super::handshake::ProviderSocket;"));
}

type NetworkClient = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct NetworkFixture {
    client: NetworkClient,
    provider: WebSocketStream<TcpStream>,
    downstream_writes: Arc<WriteControl>,
    provider_writes: Arc<WriteControl>,
    downstream_closed: CancellationToken,
    provider_closed: CancellationToken,
    draining: CancellationToken,
    completed: oneshot::Receiver<Option<StopReason>>,
    stop_server: CancellationToken,
    server: JoinHandle<()>,
}

impl NetworkFixture {
    async fn start() -> Self {
        let provider_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_url = format!("ws://{}/ws", provider_listener.local_addr().unwrap());
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("ws://{}/ws", proxy_listener.local_addr().unwrap());
        let downstream_writes = Arc::new(WriteControl::default());
        let provider_writes = Arc::new(WriteControl::default());
        let downstream_closed = CancellationToken::new();
        let provider_closed = CancellationToken::new();
        let draining = CancellationToken::new();
        let (completed_tx, completed) = oneshot::channel();
        let completed_tx = Arc::new(parking_lot::Mutex::new(Some(completed_tx)));
        let router = axum::Router::new().route(
            "/ws",
            axum::routing::get({
                let downstream_writes = downstream_writes.clone();
                let provider_writes = provider_writes.clone();
                let downstream_closed = downstream_closed.clone();
                let provider_closed = provider_closed.clone();
                let draining = draining.clone();
                move |upgrade: axum::extract::ws::WebSocketUpgrade| {
                    let provider_url = provider_url.clone();
                    let downstream_writes = downstream_writes.clone();
                    let provider_writes = provider_writes.clone();
                    let downstream_closed = downstream_closed.clone();
                    let provider_closed = provider_closed.clone();
                    let draining = draining.clone();
                    let completed_tx = completed_tx.clone();
                    async move {
                        let (provider, _) = tokio_tungstenite::connect_async(provider_url)
                            .await
                            .unwrap();
                        let completed_tx = completed_tx.lock().take().unwrap();
                        upgrade.on_upgrade(move |downstream| async move {
                            let downstream = Controlled {
                                inner: downstream,
                                writes: downstream_writes,
                                closed: downstream_closed,
                                is_close: |message| matches!(message, Ok(Message::Close(_))),
                            };
                            let provider = Controlled {
                                inner: provider,
                                writes: provider_writes,
                                closed: provider_closed,
                                is_close: |message| {
                                    matches!(message, Ok(ProviderMessage::Close(_)))
                                },
                            };
                            let result = relay_sockets(
                                downstream,
                                provider,
                                2,
                                || std::future::ready(Ok(())),
                                draining.cancelled_owned(),
                            )
                            .await;
                            completed_tx.send(result).unwrap();
                        })
                    }
                }
            }),
        );
        let stop_server = CancellationToken::new();
        let stop = stop_server.clone();
        let server = tokio::spawn(async move {
            axum::serve(proxy_listener, router)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .unwrap();
        });
        let (client, provider) = tokio::join!(tokio_tungstenite::connect_async(proxy_url), async {
            let (socket, _) = provider_listener.accept().await.unwrap();
            tokio_tungstenite::accept_async(socket).await.unwrap()
        },);
        Self {
            client: client.unwrap().0,
            provider,
            downstream_writes,
            provider_writes,
            downstream_closed,
            provider_closed,
            draining,
            completed,
            stop_server,
            server,
        }
    }

    async fn stop(self) {
        self.stop_server.cancel();
        self.server.await.unwrap();
    }
}

async fn assert_network_fifo_close<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    destination_writes: &WriteControl,
    source_closed: &CancellationToken,
    reason: &'static str,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let ping = bytes::Bytes::from_static(b"one hop-local pong");
    source
        .send(ProviderMessage::Ping(ping.clone()))
        .await
        .unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        ProviderMessage::Pong(ping)
    );
    destination_writes.pause();
    source
        .send(ProviderMessage::Text("first in-flight frame".into()))
        .await
        .unwrap();
    destination_writes.blocked.cancelled().await;
    let blocked_ping = bytes::Bytes::from_static(b"pong while application writer is blocked");
    destination
        .send(ProviderMessage::Ping(blocked_ping.clone()))
        .await
        .unwrap();
    assert_eq!(
        destination.next().await.unwrap().unwrap(),
        ProviderMessage::Pong(blocked_ping),
        "the blocked destination's reader must still flush its automatic pong"
    );
    destination
        .send(ProviderMessage::Text(
            "independent reverse-direction data".into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        ProviderMessage::Text("independent reverse-direction data".into()),
        "the opposite application pump must progress while this writer is blocked"
    );
    let frames = [
        ProviderMessage::Text("first in-flight frame".into()),
        ProviderMessage::Binary(bytes::Bytes::from_static(b"second queued frame")),
        ProviderMessage::Text("third queued frame".into()),
        ProviderMessage::Binary(bytes::Bytes::from_static(b"fourth pending frame")),
        ProviderMessage::Text("fifth pending frame".into()),
    ];
    for frame in &frames[1..] {
        source.feed(frame.clone()).await.unwrap();
    }
    let close =
        ProviderMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: 4013.into(),
            reason: reason.into(),
        }));
    source.feed(close.clone()).await.unwrap();
    source.flush().await.unwrap();
    source_closed.cancelled().await;
    destination_writes.release();
    for expected in frames {
        assert_eq!(
            destination.next().await.unwrap().unwrap(),
            expected,
            "every pre-Close frame must arrive, including both pending frames"
        );
    }
    assert_eq!(destination.next().await.unwrap().unwrap(), close);
    destination.flush().await.unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        close,
        "the automatic close reply must preserve code/reason without an extra pong"
    );
}

#[tokio::test]
async fn real_sockets_drain_both_directions_under_controlled_backpressure_and_reply_once() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_closes in [false, true] {
            let mut fixture = NetworkFixture::start().await;
            if provider_closes {
                assert_network_fifo_close(
                    &mut fixture.provider,
                    &mut fixture.client,
                    &fixture.downstream_writes,
                    &fixture.provider_closed,
                    "provider FIFO close",
                )
                .await;
            } else {
                assert_network_fifo_close(
                    &mut fixture.client,
                    &mut fixture.provider,
                    &fixture.provider_writes,
                    &fixture.downstream_closed,
                    "downstream FIFO close",
                )
                .await;
            }
            assert_eq!((&mut fixture.completed).await.unwrap(), None);
            fixture.stop().await;
        }
    })
    .await
    .expect("real socket close and all queued data complete within the bounded relay lifetime");
}

async fn assert_network_drain<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    destination_writes: &WriteControl,
    draining: &CancellationToken,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    destination_writes.pause();
    source
        .send(ProviderMessage::Text(
            "cancelled slot must not be retried".into(),
        ))
        .await
        .unwrap();
    destination_writes.blocked.cancelled().await;
    draining.cancel();
    let expected = provider_close(Some(StopReason::Draining.close()));
    assert_eq!(source.next().await.unwrap().unwrap(), expected);
    source.flush().await.unwrap();
    destination_writes.release();
    assert_eq!(
        destination.next().await.unwrap().unwrap(),
        expected,
        "urgent drain discards the cancelled application slot before sending Close"
    );
    destination.flush().await.unwrap();
}

#[tokio::test]
async fn real_server_drain_is_urgent_in_both_blocked_directions() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_blocked in [true, false] {
            let mut fixture = NetworkFixture::start().await;
            if provider_blocked {
                assert_network_drain(
                    &mut fixture.client,
                    &mut fixture.provider,
                    &fixture.provider_writes,
                    &fixture.draining,
                )
                .await;
            } else {
                assert_network_drain(
                    &mut fixture.provider,
                    &mut fixture.client,
                    &fixture.downstream_writes,
                    &fixture.draining,
                )
                .await;
            }
            assert_eq!(
                (&mut fixture.completed).await.unwrap(),
                Some(StopReason::Draining)
            );
            fixture.stop().await;
        }
    })
    .await
    .expect("server drain must cancel either blocked writer and close both hops");
}

struct CloseRacingSink {
    peer_closed: CancellationToken,
    attempted: usize,
}

impl Sink<TestMessage> for CloseRacingSink {
    type Error = &'static str;
    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(mut self: Pin<&mut Self>, _: TestMessage) -> Result<(), Self::Error> {
        self.attempted += 1;
        self.peer_closed.cancel();
        Err("peer closed concurrently with this send")
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn send_failure_racing_peer_close_only_ceases_that_write_direction() {
    let shutdown = Shutdown::default();
    let peer_closed = CancellationToken::new();
    let mut sink = CloseRacingSink {
        peer_closed: peer_closed.clone(),
        attempted: 0,
    };
    let (tx, mut messages) = mpsc::channel(3);
    tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
    tx.send(Incoming::Data(TestMessage::Data(2))).await.unwrap();
    tx.send(Incoming::Close(Some(distinctive_close())))
        .await
        .unwrap();
    let mut checked = 0;
    let outcome = coordinate(
        pump(
            &mut sink,
            &mut messages,
            &peer_closed,
            &shutdown,
            |m| m,
            TestMessage::Close,
            || {
                checked += 1;
                std::future::ready(Ok(()))
            },
        ),
        std::future::ready(()),
        std::future::pending(),
        &shutdown,
    )
    .await;
    assert_eq!(
        outcome, None,
        "peer close is not a global transport failure"
    );
    assert_eq!(
        sink.attempted, 1,
        "queued data must not be sent after destination Close"
    );
    assert_eq!(
        checked, 1,
        "no authorization operation is needed for an unsendable frame"
    );
}

#[tokio::test(start_paused = true)]
async fn destination_close_cancels_its_blocked_send_without_waiting_for_write_timeout() {
    let shutdown = Shutdown::default();
    let peer_closed = CancellationToken::new();
    let (_, destination) = test_socket();
    let written = destination.written.clone();
    let control = Arc::new(WriteControl::default());
    control.pause();
    let (mut sink, _) = Controlled {
        inner: destination,
        writes: control.clone(),
        closed: CancellationToken::new(),
        is_close: |_| false,
    }
    .split();
    let (tx, mut messages) = mpsc::channel(2);
    tx.send(Incoming::Data(TestMessage::Data(1))).await.unwrap();
    tx.send(Incoming::Close(Some(distinctive_close())))
        .await
        .unwrap();
    let began = Instant::now();
    let controller = async {
        control.blocked.cancelled().await;
        peer_closed.cancel();
    };
    let (outcome, ()) = tokio::join!(
        coordinate(
            pump(
                &mut sink,
                &mut messages,
                &peer_closed,
                &shutdown,
                |m| m,
                TestMessage::Close,
                || std::future::ready(Ok(()))
            ),
            std::future::ready(()),
            std::future::pending(),
            &shutdown
        ),
        controller,
    );
    assert_eq!(outcome, None);
    assert_eq!(
        Instant::now(),
        began,
        "destination Close must wake the blocked send immediately"
    );
    assert!(written.lock().is_empty());
}

async fn assert_duplex_peer_close<S, D>(
    source: &mut WebSocketStream<S>,
    destination: &mut WebSocketStream<D>,
    source_writes: &WriteControl,
    destination_writes: &WriteControl,
    source_closed: &CancellationToken,
    reason: &'static str,
) where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    source_writes.pause();
    destination_writes.pause();
    source
        .send(ProviderMessage::Text("final direction first frame".into()))
        .await
        .unwrap();
    destination_writes.blocked.cancelled().await;
    destination
        .send(ProviderMessage::Text(
            "opposite data cannot follow peer Close".into(),
        ))
        .await
        .unwrap();
    source_writes.blocked.cancelled().await;
    let binary = ProviderMessage::Binary(bytes::Bytes::from_static(b"final binary before close"));
    let text = ProviderMessage::Text("final text before close".into());
    source.feed(binary.clone()).await.unwrap();
    source.feed(text.clone()).await.unwrap();
    let close =
        ProviderMessage::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
            code: 4014.into(),
            reason: reason.into(),
        }));
    source.feed(close.clone()).await.unwrap();
    source.flush().await.unwrap();
    source_closed.cancelled().await;
    destination_writes.release();
    for expected in [
        ProviderMessage::Text("final direction first frame".into()),
        binary,
        text,
        close.clone(),
    ] {
        assert_eq!(
            destination.next().await.unwrap().unwrap(),
            expected,
            "an opposite in-flight application frame must not cancel the final-data FIFO"
        );
    }
    destination.flush().await.unwrap();
    assert_eq!(
        source.next().await.unwrap().unwrap(),
        close,
        "the unsendable opposite slot must be discarded before the automatic close reply"
    );
}

#[tokio::test]
async fn real_duplex_opposite_inflight_data_does_not_abort_final_text_binary_close() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for provider_closes in [false, true] {
            let mut fixture = NetworkFixture::start().await;
            if provider_closes {
                assert_duplex_peer_close(
                    &mut fixture.provider,
                    &mut fixture.client,
                    &fixture.provider_writes,
                    &fixture.downstream_writes,
                    &fixture.provider_closed,
                    "provider closes with opposite data in flight",
                )
                .await;
            } else {
                assert_duplex_peer_close(
                    &mut fixture.client,
                    &mut fixture.provider,
                    &fixture.downstream_writes,
                    &fixture.provider_writes,
                    &fixture.downstream_closed,
                    "downstream closes with opposite data in flight",
                )
                .await;
            }
            assert_eq!(
                (&mut fixture.completed).await.unwrap(),
                None,
                "peer Close must cease only its destination write direction"
            );
            fixture.stop().await;
        }
    })
    .await
    .expect("duplex final-data drain must survive opposite unsendable application data");
}

#[tokio::test(start_paused = true)]
async fn peer_close_is_published_before_the_split_socket_can_be_reused() {
    let (tx, socket) = test_socket();
    let written = socket.written.clone();
    let peer_closed = CancellationToken::new();
    let (mut sink, mut stream) = PeerSocket {
        inner: socket,
        peer_closed: peer_closed.clone(),
        is_close: |message| matches!(message, Ok(TestMessage::Close(_))),
    }
    .split();
    tx.send(TestMessage::Close(Some(distinctive_close())))
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        TestMessage::Close(Some(distinctive_close()))
    );
    assert!(
        peer_closed.is_cancelled(),
        "Close must be published inside the split socket lock"
    );
    let began = Instant::now();
    assert_eq!(
        write(&mut sink, TestMessage::Data(1), &peer_closed).await,
        Ok(())
    );
    assert_eq!(Instant::now(), began);
    assert!(
        written.lock().is_empty(),
        "no application send may follow observed Close"
    );
}
