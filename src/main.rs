#![feature(random)]

mod amf;
mod handshake;
mod rtmp;
mod server;

use anyhow::{anyhow, Result};
use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};

/* ================= args ================= */

/// CLI: support `-l`/`--listen` and `-u`/`--upstream`.
#[derive(Parser, Debug)]
#[command(about = "RTMP forwarder proxy")]
struct Cli {
    /// Local listen address (default 127.0.0.1:1935)
    #[arg(short = 'l', long = "listen", default_value = "127.0.0.1:1935")]
    listen: String,

    /// Upstream RTMP URL (rtmp://host[:port]/app/stream)
    #[arg(short = 'u', long = "upstream")]
    upstream: String,
    /// Logging level (eg. info, debug). If omitted, defaults to info. Can be overridden by RUST_LOG env.
    #[arg(long = "log")]
    log: Option<String>,
}

fn parse_rtmp_url(url: &str) -> Result<(String, String, String)> {
    if !url.starts_with("rtmp://") {
        return Err(anyhow!("invalid rtmp url"));
    }

    let s = &url[7..];
    let mut parts = s.splitn(2, '/');

    let host = parts.next().ok_or_else(|| anyhow!("missing host"))?;
    let host = if host.contains(':') {
        host.to_string()
    } else {
        format!("{}:1935", host)
    };

    let path = parts.next().ok_or_else(|| anyhow!("missing app"))?;
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

    // Initialize tracing subscriber: prefer CLI `--log` if provided, otherwise use RUST_LOG env, otherwise default to `info`
    let env_filter = if let Some(ref lvl) = cli.log {
        tracing_subscriber::EnvFilter::new(lvl.as_str())
    } else {
        tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
    let (upstream_addr, app, stream) = parse_rtmp_url(&cli.upstream)?;

    // Bind the listening socket and log the configured endpoints
    let listener = TcpListener::bind(&cli.listen).await?;
    info!("listening on: {}", cli.listen);
    info!("forward to: {}", cli.upstream);

    loop {
        let (client, peer) = listener.accept().await?;
        let upstream = upstream_addr.clone();
        let app = app.clone();
        let stream = stream.clone();

        tokio::spawn(async move {
            info!("Received connection from client: {}", peer);
            if let Err(e) = server::handle_client(client, &upstream, &app, &stream).await {
                error!(error = %e, "connection error");
            }
        });
    }
}
