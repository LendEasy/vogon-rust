//! HTTP/1.1 keep-alive fixture with explicit response gates and TCP connection IDs.
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
};

pub struct Request {
    pub connection: usize,
    pub head: String,
    pub body: Value,
    pub reply: oneshot::Sender<()>,
}
pub struct Server {
    pub url: String,
    pub requests: mpsc::UnboundedReceiver<Request>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            let mut connection = 0;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                connection += 1;
                let tx = tx.clone();
                children.spawn(async move {
                    let mut bytes = Vec::new();
                    loop {
                        let split = loop {
                            if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") { break i + 4; }
                            let mut buffer = [0; 4096];
                            let Ok(n) = socket.read(&mut buffer).await else { return; };
                            if n == 0 { return; }
                            bytes.extend_from_slice(&buffer[..n]);
                        };
                        let head = String::from_utf8(bytes[..split].to_vec()).unwrap();
                        let len: usize = head.lines().find_map(|l| l.to_lowercase().strip_prefix("content-length:").map(|s| s.trim().parse().unwrap())).unwrap_or(0);
                        while bytes.len() < split + len {
                            let mut buffer = [0; 4096];
                            let Ok(n) = socket.read(&mut buffer).await else { return; };
                            if n == 0 { return; }
                            bytes.extend_from_slice(&buffer[..n]);
                        }
                        let body = if len == 0 { Value::Null } else { serde_json::from_slice(&bytes[split..split+len]).unwrap() };
                        bytes.drain(..split+len);
                        let (reply, wait) = oneshot::channel();
                        if tx.send(Request { connection, head, body, reply }).is_err() || wait.await.is_err() { return; }
                        if socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}").await.is_err() { return; }
                    }
                });
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }
    pub async fn next(&mut self) -> Request {
        tokio::time::timeout(std::time::Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
