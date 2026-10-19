//! Standalone multithread gateway process; dotenv defaults to the existing Python gateway file.
use std::{path::PathBuf, sync::Arc};
use vogon_service::{
    bootstrap::Config,
    host::Service,
    routes,
    store::{Memory, Redis, SharedStore, Ttl},
};
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("gateway startup/shutdown failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|s| s == "--help") {
        println!(
            "vogon-gateway [--env PATH] [--check]\nDefault dotenv: voice-gateway/.env. GATEWAY_HOST/PORT control binding."
        );
        return Ok(());
    }
    let path = args
        .windows(2)
        .find(|a| a[0] == "--env")
        .map(|a| PathBuf::from(&a[1]))
        .unwrap_or_else(|| PathBuf::from("voice-gateway/.env"));
    let config = Config::load(&path)?;
    let store: SharedStore = if config.gateway["session_store"] == "redis" {
        Arc::new(
            Redis::connect(
                config.settings["redis_url"].as_str().unwrap(),
                Ttl::default(),
            )
            .await?,
        )
    } else {
        Arc::new(Memory::default())
    };
    let address = config.bind.clone();
    let service = Service::new(config, store)?;
    if args.iter().any(|s| s == "--check") {
        println!(
            "gateway configuration and session store ready; native capability acceptance remains module 13"
        );
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind(&address).await?;
    eprintln!("vogon gateway listening on {}", listener.local_addr()?);
    let stop = service.clone();
    axum::serve(listener, routes::router(service.clone()))
        .with_graceful_shutdown(async move {
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("install SIGTERM");
                tokio::select! {_=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            if let Err(error) = stop.shutdown().await {
                eprintln!("{error}");
            }
        })
        .await?;
    service.shutdown().await?;
    Ok(())
}
