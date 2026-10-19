//! One independent WebSocket reader/writer per provider connection. Audio consumers
//! never own the socket, so slow playback cannot stop Ping/Pong handling.
use crate::{Error, Result, config::ApiKey};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{self, Duration, Instant},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

/// Data/control events from a single socket epoch. Never includes auth headers.
#[derive(Debug)]
pub enum WireEvent {
    Text(String),
    Binary(Vec<u8>),
    Closed,
    Failed(Error),
}
struct Command {
    message: Message,
    ack: oneshot::Sender<Result<()>>,
}

/// Cloneable ordered writer. Completion means the WebSocket send completed.
#[derive(Clone)]
pub struct Writer {
    commands: mpsc::Sender<Command>,
}
impl Writer {
    /// Wait for queue admission and acknowledgement of the underlying WebSocket send.
    /// This does not acknowledge provider processing or caller playback.
    pub async fn send(&self, message: Message) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command { message, ack: tx })
            .await
            .map_err(|_| Error::new("socket_closed"))?;
        rx.await.map_err(|_| Error::new("socket_closed"))?
    }
}

/// Socket ownership and incoming stream. Drop terminates the I/O task.
pub struct Socket {
    pub writer: Writer,
    pub incoming: mpsc::UnboundedReceiver<WireEvent>,
    task: JoinHandle<()>,
}
impl Drop for Socket {
    fn drop(&mut self) {
        self.task.abort();
    }
}
/// Only handshake/network failures are replayable; active audio packets are never retried.
pub fn retryable(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "provider_connect_failed"
            | "provider_connect_timeout"
            | "provider_http_408"
            | "provider_http_429"
    ) || error
        .code
        .strip_prefix("provider_http_")
        .and_then(|s| s.parse::<u16>().ok())
        .is_some_and(|n| n >= 500)
}
impl Socket {
    /// Retry connection establishment before any audio is sent; caller cancellation stops backoff.
    pub async fn connect_retry(
        endpoint: &str,
        query: &[(String, String)],
        key: &ApiKey,
        policy: &vogon_resilience::Policy,
    ) -> Result<Self> {
        policy
            .run(|| Self::connect(endpoint, query, key), retryable)
            .await
    }

    /// Authenticated connection, with query encoding and repeated keys handled by URL.
    pub async fn connect(endpoint: &str, query: &[(String, String)], key: &ApiKey) -> Result<Self> {
        Self::connect_with_auth(endpoint, query, key, false).await
    }
    /// Live uses Bearer auth; Deepgram uses Token. Both redact header values.
    pub(crate) async fn connect_with_auth(
        endpoint: &str,
        query: &[(String, String)],
        key: &ApiKey,
        bearer: bool,
    ) -> Result<Self> {
        let mut url = url::Url::parse(endpoint).map_err(|_| Error::new("invalid_provider_url"))?;
        if url.scheme() != "wss"
            && !(url.scheme() == "ws"
                && matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")))
        {
            return Err(Error::new("provider_requires_tls"));
        }
        if !query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(query.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        }
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|_| Error::new("invalid_provider_request"))?;
        request.headers_mut().insert(
            "Authorization",
            if bearer {
                key.auth_header("Bearer")?
            } else {
                key.header()?
            },
        );
        let (mut socket, _) = time::timeout(Duration::from_secs(15), connect_async(request))
            .await
            .map_err(|_| Error::new("provider_connect_timeout"))?
            .map_err(|error| match error {
                tokio_tungstenite::tungstenite::Error::Http(response) => {
                    Error::new(&format!("provider_http_{}", response.status().as_u16()))
                }
                _ => Error::new("provider_connect_failed"),
            })?;
        let (tx, mut rx) = mpsc::channel::<Command>(64);
        let (events, incoming) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut interval = time::interval_at(
                Instant::now() + Duration::from_secs(20),
                Duration::from_secs(20),
            );
            let mut pong_pending = false;
            loop {
                tokio::select! {
                    command=rx.recv()=>{
                        let Some(command)=command else{let _=socket.close(None).await;break;};
                        let closing=matches!(command.message,Message::Close(_));
                        let result=time::timeout(Duration::from_secs(5),socket.send(command.message)).await;
                        let ok=matches!(result,Ok(Ok(())));
                        let _=command.ack.send(if ok{Ok(())}else{Err(Error::new("provider_send_failed"))});
                        if !ok{let _=events.send(WireEvent::Failed(Error::new("provider_send_failed")));break;}
                        if closing{break;}
                    },
                    message=socket.next()=>match message {
                        Some(Ok(Message::Text(text)))=>{if events.send(WireEvent::Text(text.to_string())).is_err(){break;}},
                        Some(Ok(Message::Binary(bytes)))=>{if events.send(WireEvent::Binary(bytes.to_vec())).is_err(){break;}},
                        Some(Ok(Message::Ping(_)))=>{if !matches!(time::timeout(Duration::from_secs(5),socket.flush()).await,Ok(Ok(()))){break;}},
                        Some(Ok(Message::Pong(_)))=>{pong_pending=false;},
                        Some(Ok(Message::Close(_)))|None=>break,
                        Some(Err(_))=>{let _=events.send(WireEvent::Failed(Error::new("provider_receive_failed")));break;},
                        _=>{},
                    },
                    _=interval.tick()=>{
                        if pong_pending{let _=events.send(WireEvent::Failed(Error::new("provider_keepalive_timeout")));break;}
                        if !matches!(time::timeout(Duration::from_secs(5),socket.send(Message::Ping(vec![1].into()))).await,Ok(Ok(()))){break;}
                        pong_pending=true;
                    }
                }
            }
            let _ = events.send(WireEvent::Closed);
        });
        Ok(Self {
            writer: Writer { commands: tx },
            incoming,
            task,
        })
    }
}
