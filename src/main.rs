#![feature(random)]

mod amf;
mod config;
mod handshake;
mod rtmp;
mod server;
mod web;
mod forwarder;

use crate::web::FlvStreamManager;

use anyhow::Result;
use clap::Parser;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tracing::{error, info};

/* ================= args ================= */

/// CLI: support `-l`/`--listen` and `-u`/`--upstream`.
#[derive(Parser, Debug)]
#[command(about = "RTMP forwarder proxy")]
struct Cli {
    /// Local listen address (default 127.0.0.1:1935)
    #[arg(short = 'l', long = "listen")]
    listen: Option<String>,

    /// Upstream RTMP URL(s) (rtmp://host[:port]/app/stream)
    #[arg(short = 'u', long = "upstream")]
    upstream: Vec<String>,

    /// Relay host:port for original push (no rewrite)
    #[arg(short = 'r', long = "relay")]
    relay: Option<String>,

    /// Logging level (eg. info, debug). If omitted, defaults to info. Can be overridden by RUST_LOG env.
    #[arg(long = "log")]
    log: Option<String>,

    /// Web dashboard address (default 0.0.0.0:8080)
    #[arg(short = 'w', long = "web")]
    web: Option<String>,
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

    // 1. Load persistent config
    let mut app_config = config::load_config();

    // 2. Overlays CLI arguments if provided
    if let Some(l) = cli.listen { app_config.listen_addr = l; }
    if let Some(log) = cli.log { app_config.log_level = log; }
    if let Some(w) = cli.web { app_config.web_addr = w; }
    if !cli.upstream.is_empty() {
        app_config.upstreams = cli.upstream.iter().filter_map(|u| {
            let (addr, app, stream) = parse_rtmp_url(u).ok()?;
            Some(server::UpstreamConfig { addr, app: Some(app), stream: Some(stream), enabled: true })
        }).collect();
    }
    if let Some(r) = cli.relay {
        let addr = if r.contains(':') { r } else { format!("{}:1935", r) };
        app_config.relay_addr = Some(addr);
        app_config.relay_enabled = true;
    }

    // Initialize tracing subscriber
    let env_filter = tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&app_config.log_level));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let shared_config = Arc::new(RwLock::new(app_config));

    // 3. Create shared FLV stream manager
    let flv_manager = std::sync::Arc::new(FlvStreamManager::new());

    // 4. Start Web Server
    let web_conf = shared_config.clone();
    let web_flv_manager = flv_manager.clone();
    tokio::spawn(async move {
        web::start_web_server(web_conf, web_flv_manager).await;
    });

    // 5. Bind the listening socket based on current config
    let listen_addr = shared_config.read().unwrap().listen_addr.clone();
    let listener = TcpListener::bind(&listen_addr).await?;
    info!("RTMP listening on: {}", listen_addr);

    loop {
        let (client, peer) = listener.accept().await?;
        let conf = shared_config.clone();
        let server_flv_manager = flv_manager.clone();

        tokio::spawn(async move {
            info!("Received connection from client: {}", peer);
            if let Err(e) = server::handle_client(client, conf, server_flv_manager).await {
                error!(error = %e, "connection error");
            }
        });
    }
}
