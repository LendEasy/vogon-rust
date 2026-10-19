//! Loopback-only, externally gated peers. Gates prove progress without timing benchmarks.
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use vogon_transport::wire::{Frame, Sink};

pub struct Request {
    pub operation: String,
    pub body: Value,
    pub reply: oneshot::Sender<Value>,
}
/// Concurrent HTTP acceptor; each connection waits for an explicit test response.
pub struct Http {
    pub url: String,
    pub requests: mpsc::UnboundedReceiver<Request>,
    task: JoinHandle<()>,
}
impl Drop for Http {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Http {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/turn/v1", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut stream, _) = accepted.unwrap(); let tx=tx.clone();
                        tasks.spawn(async move {
                            let mut bytes=vec![];
                            let split=loop { let mut b=[0;4096]; let n=stream.read(&mut b).await.unwrap(); if n==0{return;} bytes.extend_from_slice(&b[..n]); if let Some(i)=bytes.windows(4).position(|s|s==b"\r\n\r\n"){break i+4;} };
                            let head=String::from_utf8(bytes[..split].to_vec()).unwrap();
                            let len:usize=head.lines().find_map(|l| l.to_lowercase().strip_prefix("content-length:").map(|s|s.trim().parse().unwrap())).unwrap();
                            while bytes.len()<split+len { let mut b=[0;4096]; let n=stream.read(&mut b).await.unwrap(); if n==0{return;} bytes.extend_from_slice(&b[..n]); }
                            let operation=head.split_whitespace().nth(1).unwrap().rsplit('/').next().unwrap().into();
                            let body=serde_json::from_slice(&bytes[split..split+len]).unwrap(); let (reply,rx)=oneshot::channel();
                            if tx.send(Request{operation,body,reply}).is_err(){return;}
                            if let Ok(body)=rx.await {let body=body.to_string();let header=format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());let _=stream.write_all(header.as_bytes()).await;}
                        });
                    },
                    _=tasks.join_next(),if !tasks.is_empty()=>{}
                }
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }
}
/// STT socket exposes independent send and receive channels, like a streaming provider.
pub struct Stt {
    pub url: String,
    pub emit: mpsc::UnboundedSender<Value>,
    pub audio: mpsc::UnboundedReceiver<Vec<u8>>,
    pub task: JoinHandle<()>,
}
impl Stt {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (emit, mut events) = mpsc::unbounded_channel::<Value>();
        let (tx, audio) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            loop {
                tokio::select! {
                    Some(v)=events.recv()=>{if ws.send(Message::Text(v.to_string().into())).await.is_err(){break;}},
                    message=ws.next()=>match message {
                        Some(Ok(Message::Binary(v)))=>{let _=tx.send(v.to_vec());},
                        Some(Ok(Message::Text(v))) if v.contains("CloseStream")=>{let _=ws.close(None).await;break;},
                        None|Some(Err(_))|Some(Ok(Message::Close(_)))=>break,
                        _=>{}
                    }
                }
            }
        });
        Self {
            url,
            emit,
            audio,
            task,
        }
    }
    pub fn turn(&self, event: &str, text: &str) {
        self.emit.send(json!({"type":"TurnInfo","event":event,"transcript":text,"words":[{"word":text,"confidence":0.99,"start":0.0}]})).unwrap();
    }
}
/// Every Flush is gated independently; Clear returns deliberately late bytes before Cleared.
pub struct Tts {
    pub url: String,
    pub spoken: mpsc::UnboundedReceiver<String>,
    pub commands: mpsc::UnboundedReceiver<String>,
    pub emit: mpsc::UnboundedSender<Message>,
    pub task: JoinHandle<()>,
}
impl Tts {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (emit, mut output) = mpsc::unbounded_channel();
        let (say, spoken) = mpsc::unbounded_channel();
        let (tx, commands) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            loop {
                tokio::select! {
                    Some(v)=output.recv()=>{if ws.send(v).await.is_err(){break;}},
                    message=ws.next()=>match message {
                        Some(Ok(Message::Text(v)))=>{let v:Value=serde_json::from_str(&v).unwrap(); let kind=v["type"].as_str().unwrap();let _=tx.send(kind.to_owned());
                            if kind=="Speak" {let _=say.send(v["text"].as_str().unwrap().into());}
                            if kind=="Clear" {let _=ws.send(Message::Binary(vec![77;160].into())).await;let _=ws.send(Message::Text(json!({"type":"Cleared"}).to_string().into())).await;}
                        },
                        None|Some(Err(_))|Some(Ok(Message::Close(_)))=>break,
                        _=>{}
                    }
                }
            }
        });
        Self {
            url,
            spoken,
            commands,
            emit,
            task,
        }
    }
    pub fn finish(&self, byte: u8) {
        self.emit
            .send(Message::Binary(vec![byte; 640].into()))
            .unwrap();
        self.emit
            .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
            .unwrap();
    }
}
pub struct Capture(pub Arc<Mutex<Vec<Frame>>>);
impl Sink for Capture {
    fn send(
        &mut self,
        frame: Frame,
    ) -> Pin<Box<dyn Future<Output = vogon_speech_providers::Result<()>> + Send + '_>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(frame);
            Ok(())
        })
    }
}
