#![feature(random)]
#![feature(if_let_guard)]

mod amf;
mod config;
mod handshake;
mod rtmp;
mod rtmp_codec;
mod server;
mod web;
mod forwarder;
mod stream_manager;
mod forwarder_manager;
mod flv_manager;

use crate::config::{AppConfig, GetForwarders};
use crate::stream_manager::StreamManager;
use crate::forwarder_manager::ForwarderManager;
use crate::flv_manager::FlvManager;

use anyhow::Result;
use clap::Parser;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tracing::{error, info};

/* ================= args ================= */

/// CLI: support `-l`/`--listen`, `-d`/`--dest`, and `-r`/`--relay`.
#[derive(Parser, Debug)]
#[command(about = "RTMP forwarder proxy")]
struct Cli {
    /// Local listen address (default 127.0.0.1:1935)
    #[arg(short = 'l', long = "listen")]
    listen: Option<String>,

    /// Destination RTMP server URL(s) (rtmp://host[:port]/app/stream)
    #[arg(short = 'd', long = "dest")]
    dest: Vec<String>,

    /// Relay host:port for original push (no rewrite)
    #[arg(short = 'r', long = "relay")]
    relay: Option<String>,

    /// Logging level (eg. info, debug). If omitted, defaults to info. Can be overridden by RUST_LOG env.
    #[arg(long = "log")]
    log: Option<String>,

    /// Web dashboard address (default 0.0.0.0:8080)
    #[arg(short = 'w', long = "web")]
    web: Option<String>,

    /// Sets a custom config file
    #[arg(short = 'c', long = "config", value_name = "FILE")]
    config: Option<String>,
}

fn parse_rtmp_url(url: &str) -> Result<(String, String, String)> {
    if !url.starts_with("rtmp://") {
        return Err(anyhow::anyhow!("invalid rtmp url"));
    }

    let s = &url[7..];
    let mut parts = s.splitn(2, '/');

    let host = parts.next().ok_or_else(|| anyhow::anyhow!("missing host"))?;
    let host = if host.contains(':') {
        host.to_string()
    } else {
        format!("{}:1935", host)
    };

    let path = parts.next().ok_or_else(|| anyhow::anyhow!("missing app"))?;
    let mut p = path.split('/');

    let app = p.next().unwrap().to_string();
    let stream = p.collect::<Vec<_>>().join("/");

    Ok((host, app, stream))
}

/* ================= main ================= */

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI using clap
    let cli = Cli::parse();
    let config_path = cli.config.as_deref().unwrap_or("config.json");
    let mut app_config = AppConfig::load(config_path)?;

    // 2. Overlays CLI arguments if provided
    if let Some(l) = cli.listen { app_config.listen_addr = l; }
    if let Some(log) = cli.log { app_config.log_level = log; }
    if let Some(w) = cli.web { app_config.web_addr = w; }
    if !cli.dest.is_empty() {
        app_config.forwarders = cli.dest.iter().filter_map(|u| {
            let (addr, app, stream) = parse_rtmp_url(u).ok()?;
            Some(server::ForwarderConfig { addr, app: Some(app), stream: Some(stream), enabled: true })
        }).collect();
    }
    if let Some(r) = cli.relay {
        let addr = if r.contains(':') { r } else { format!("{}:1935", r) };
        app_config.relay_addr = addr;
        app_config.relay_enabled = true;
    }

    // Initialize tracing subscriber
    let env_filter = tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&app_config.log_level));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let shared_config = Arc::new(RwLock::new(app_config));

    // 3. Create stream manager
    let stream_manager = Arc::new(StreamManager::new());
    
    // 4. Create forwarder manager
    let initial_forwarders = {
        let cfg = shared_config.read().unwrap();
        cfg.get_forwarders()
    };
    let (forwarder_mgr, forwarder_cmd_tx) = ForwarderManager::new(
        stream_manager.clone(),
        initial_forwarders,
    );
    let forwarder_handle = tokio::spawn(forwarder_mgr.run());
    
    // 5. Create FLV manager
    let flv_manager = Arc::new(FlvManager::new(stream_manager.clone()));
    let flv_mgr_clone = flv_manager.clone();
    tokio::spawn(async move { flv_mgr_clone.run().await });

    // 6. Start Web Server
    let web_conf = shared_config.clone();
    let web_flv_manager = flv_manager.clone();
    let web_forwarder_cmd = forwarder_cmd_tx.clone();
    let web_stream_manager = stream_manager.clone();
    tokio::spawn(async move {
        web::start_web_server(web_conf, web_flv_manager, web_forwarder_cmd, web_stream_manager).await;
    });

    // 7. Bind the listening socket based on current config
    let listen_addr = shared_config.read().unwrap().listen_addr.clone();
    let listener = TcpListener::bind(&listen_addr).await?;
    info!("RTMP listening on: {}", listen_addr);

    // 8. Setup graceful shutdown
    let shutdown_forwarder_cmd = forwarder_cmd_tx.clone();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal");
        shutdown_forwarder_cmd.send(crate::forwarder_manager::ForwarderCommand::Shutdown).ok();
        shutdown_tx.send(()).await.ok();
    });

    loop {
        tokio::select! {
            Ok((client, peer)) = listener.accept() => {
                let conf = shared_config.clone();
                let server_stream_manager = stream_manager.clone();

                tokio::spawn(async move {
                    info!("Received connection from client: {}", peer);
                    if let Err(e) = server::handle_client(client, conf, server_stream_manager).await {
                        error!(error = %e, "connection error");
                    }
                });
            }
            _ = shutdown_rx.recv() => {
                info!("Shutting down server");
                break;
            }
        }
    }
    
    forwarder_handle.await.ok();
    info!("Server stopped");
    Ok(())
}
