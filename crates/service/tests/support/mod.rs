#![allow(dead_code)]
use axum::{
    Json, Router,
    extract::{Path, State},
    routing::post,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use vogon_service::{bootstrap::Config, host::Service, routes, store::Memory};
pub struct Request {
    pub operation: String,
    pub body: Value,
    pub reply: oneshot::Sender<Value>,
}
pub struct Turn {
    pub url: String,
    pub requests: mpsc::UnboundedReceiver<Request>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Turn {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Turn {
    pub async fn start() -> Self {
        async fn handle(
            State(tx): State<mpsc::UnboundedSender<Request>>,
            Path(operation): Path<String>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            let (reply, rx) = oneshot::channel();
            tx.send(Request {
                operation,
                body,
                reply,
            })
            .unwrap();
            Json(rx.await.unwrap_or(json!({})))
        }
        let (tx, requests) = mpsc::unbounded_channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/turn/v1", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/turn/v1/{operation}", post(handle))
            .with_state(tx);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            url,
            requests,
            task,
        }
    }
}
pub struct Gateway {
    pub url: String,
    pub service: Arc<Service>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Gateway {
    pub async fn start(config: Config) -> Self {
        let service = Service::new(config, Arc::new(Memory::default())).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = routes::router(service.clone());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, service, task }
    }
    pub async fn create(&self, id: &str, mode: &str, app: &str) -> Value {
        let r=reqwest::Client::new().post(format!("{}/v1/tenants/acme/calls",self.url)).json(&json!({"mode":mode,"call_id":id,"app":{"base_url":app},"app_payload":{"opaque":[1,2]},"route":"test"})).send().await.unwrap();
        assert_eq!(r.status(), 200);
        r.json().await.unwrap()
    }
}
pub fn config() -> Config {
    Config::resolve(
        &json!({"FRONT_MODEL_ENABLED":"false","DEEPGRAM_API_KEY":"local-test"}),
        &json!({}),
    )
    .unwrap()
}
pub async fn recv<T>(r: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(5), r.recv())
        .await
        .expect("fixture timeout")
        .expect("fixture closed")
}
pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
pub async fn message(ws: &mut Ws, kind: &str) -> Value {
    use futures_util::StreamExt;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(s))) = ws.next().await {
            let v: Value = serde_json::from_str(&s).unwrap();
            if v["type"] == kind {
                return v;
            }
        }
        panic!("socket closed before {kind}")
    })
    .await
    .unwrap()
}
pub async fn closed(ws: &mut Ws) -> u16 {
    use futures_util::StreamExt;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match ws.next().await {
                Some(Ok(tokio_tungstenite::tungstenite::Message::Close(Some(c)))) => {
                    return c.code.into();
                }
                None => panic!("no close frame"),
                _ => {}
            }
        }
    })
    .await
    .unwrap()
}
