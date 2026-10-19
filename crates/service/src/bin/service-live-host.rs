//! Live-validation launcher: production router/driver with local observation proxies.
//! Only the explicitly supplied dotenv contains secrets. Phone records represent a
//! local Twilio-protocol caller, not a placed telephone call. Never used by production main.
use serde_json::Value;
use std::{path::Path, sync::Arc};
use vogon_service::{
    bootstrap::Config,
    host::Service,
    record::Record,
    routes,
    store::{Change, Memory, SharedStore},
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) != Some("--live") {
        return Err("requires --live FIXTURE.json".into());
    }
    let fixture: Value = serde_json::from_slice(&std::fs::read(&args[2])?)?;
    let mut config = Config::load(Path::new(
        fixture["env_path"].as_str().ok_or("env path missing")?,
    ))?;
    for (key, value) in fixture["settings"].as_object().ok_or("settings missing")? {
        config.settings[key] = value.clone();
    }
    config.gateway["service_key"] = Value::String("live-fixture-pair".into());
    config.gateway["public_base_url"] = Value::String(String::new());
    config.gateway["session_store"] = Value::String("memory".into());
    config.profiles = serde_json::json!({});
    config.openrouter_fallback.clear();
    config.stt_endpoint = fixture["stt"].as_str().ok_or("stt missing")?.into();
    config.tts_endpoint = fixture["tts"].as_str().ok_or("tts missing")?.into();
    config.max_active = 4;
    config.shutdown_timeout = std::time::Duration::from_secs(25);
    let store: SharedStore = Arc::new(Memory::default());
    for record in fixture["phones"].as_array().ok_or("phones missing")? {
        let record: Record = serde_json::from_value(record.clone())?;
        let key = (record.tenant.clone(), record.call_id.clone());
        let sid = record.provider_call_id.clone();
        store.create(record).await?;
        store.change(key, Change::Provider(sid)).await?;
    }
    let service = Service::new(config, store)?;
    let listener =
        tokio::net::TcpListener::bind(fixture["bind"].as_str().ok_or("bind missing")?).await?;
    println!("live validation host ready");
    let stopping = service.clone();
    axum::serve(listener, routes::router(service.clone()))
        .with_graceful_shutdown(async move {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal");
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
            if stopping.shutdown().await.is_err() {
                eprintln!("live_host_shutdown_failed");
            }
        })
        .await?;
    service.shutdown().await?;
    Ok(())
}
