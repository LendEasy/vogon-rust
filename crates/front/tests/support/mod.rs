//! Local HTTP fixture: request capture and deliberate status/body/latency failures.
#![allow(dead_code)]
use serde_json::Value;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
    task::JoinHandle,
    time::{Duration, sleep},
};
use vogon_front::{
    http::{Http, Key},
    jev, judge,
};
#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub body: String,
    pub delay: u64,
    pub body_delay: u64,
}
impl Reply {
    pub fn json(value: Value) -> Self {
        Self {
            status: 200,
            body: value.to_string(),
            delay: 0,
            body_delay: 0,
        }
    }
}
pub struct Server {
    pub url: String,
    pub requests: Arc<Mutex<Vec<(String, Value)>>>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/test", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let capture = requests.clone();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            for reply in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let capture = capture.clone();
                children.spawn(async move {
                    let mut bytes=vec![];let split=loop {let mut b=[0;4096];let n=stream.read(&mut b).await.unwrap();if n==0{return;}bytes.extend_from_slice(&b[..n]);if let Some(i)=bytes.windows(4).position(|s|s==b"\r\n\r\n"){break i+4;}};
                    let head=String::from_utf8(bytes[..split].to_vec()).unwrap();let len:usize=head.lines().find_map(|l|l.to_lowercase().strip_prefix("content-length:").map(|s|s.trim().parse().unwrap())).unwrap_or(0);
                    while bytes.len()<split+len{let mut b=[0;4096];let n=stream.read(&mut b).await.unwrap();if n==0{return;}bytes.extend_from_slice(&b[..n]);}
                    capture.lock().await.push((head,serde_json::from_slice(&bytes[split..split+len]).unwrap()));
                    sleep(Duration::from_millis(reply.delay)).await;
                    let header=format!("HTTP/1.1 {} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",reply.status,reply.body.len());
                    if stream.write_all(header.as_bytes()).await.is_err(){return;}
                    sleep(Duration::from_millis(reply.body_delay)).await;let _=stream.write_all(reply.body.as_bytes()).await;
                });
            }
            while children.join_next().await.is_some() {}
        });
        Self {
            url,
            requests,
            task,
        }
    }
    pub fn http(&self) -> Http {
        Http::new(&self.url, Key::new("fixture-secret".into()).unwrap()).unwrap()
    }
    pub fn jev(&self) -> jev::Client {
        jev::Client {
            http: self.http(),
            model: "fixture-jev".into(),
            timeout_ms: 1000,
            tracer: None,
        }
    }
    pub fn judge(&self) -> judge::Client {
        judge::Client {
            http: self.http(),
            model: "fixture-judge".into(),
            timeout_ms: 1000,
        }
    }
    pub async fn received(&self, n: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.requests.lock().await.len() < n {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
pub fn questions() -> Value {
    serde_json::json!({"recording":jev::noul("recording?","yes","no")})
}
pub fn answer() -> Value {
    serde_json::json!({"answers":{"recording":{"type":"noul","noul":0.8}},"model":"fixture-jev","id":"id","usage":{"input_tokens":10,"output_tokens":2,"cost":0.01}})
}
