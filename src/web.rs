use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router, Extension,
    http::{header, StatusCode, Uri},
    extract::Path,
    body::Body,
};
use std::net::SocketAddr;
use crate::config::{SharedConfig, AppConfig};
use crate::flv_manager::FlvManager;
use crate::forwarder_manager::ForwarderCommand;
use tokio::sync::mpsc;
use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::info;
use tower_http::cors::CorsLayer;
use rust_embed::RustEmbed;
use std::sync::Arc;

#[derive(RustEmbed)]
#[folder = "static/"]
struct Assets;

pub async fn start_web_server(
    config: SharedConfig,
    flv_manager: Arc<FlvManager>,
    forwarder_cmd_tx: mpsc::UnboundedSender<ForwarderCommand>,
) {
    let addr_str = config.read().unwrap().web_addr.clone();
    let addr: SocketAddr = addr_str.parse().unwrap_or(([0, 0, 0, 0], 8080).into());

    let app = Router::new()
        .route("/api/config", get(get_config))
        .route("/api/config", post(update_config))
        .route("/live/:stream_id.flv", get(handle_flv_stream))
        .fallback(static_handler)
        .layer(Extension(config))
        .layer(Extension(flv_manager))
        .layer(Extension(forwarder_cmd_tx))
        .layer(CorsLayer::permissive());

    info!("Web dashboard available at http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();

    if path.is_empty() {
        path = "index.html".to_string();
    }

    match Assets::get(&path) {
        Some(content) => {
            let content_type = if path.ends_with(".html") { "text/html" }
            else if path.ends_with(".js") { "application/javascript" }
            else if path.ends_with(".css") { "text/css" }
            else { "application/octet-stream" };

            Response::builder()
                .header(header::CONTENT_TYPE, content_type)
                .body(content.data.into())
                .unwrap()
        }
        None => {
            if let Some(content) = Assets::get("index.html") {
                return Response::builder()
                    .header(header::CONTENT_TYPE, "text/html")
                    .body(content.data.into())
                    .unwrap();
            }
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

async fn get_config(Extension(config): Extension<SharedConfig>) -> Json<AppConfig> {
    let c = config.read().unwrap();
    Json(c.clone())
}


async fn update_config(
    Extension(config): Extension<SharedConfig>,
    Extension(forwarder_cmd_tx): Extension<mpsc::UnboundedSender<ForwarderCommand>>,
    Json(new_config): Json<AppConfig>,
) -> Json<bool> {
    let forwarders = new_config.get_forwarders();
    let success= {
        let mut c = config.write().unwrap();
        *c = new_config;
        c.save().is_ok()
    };

    if success {
        info!("Config saved, notifying ForwarderManager with {} forwarders", forwarders.len());
        forwarder_cmd_tx.send(ForwarderCommand::UpdateConfig(forwarders)).ok();
    }
    
    Json(success)
}

pub async fn handle_flv_stream(
    Path(_stream_id): Path<String>,
    Extension(manager): Extension<Arc<FlvManager>>,
) -> impl IntoResponse {
    tracing::info!("HTTP-FLV: Request for stream");
    
    let rx = manager.subscribe_flv("stream").await;
    
    let stream = ReceiverStream::new(rx).map(|data| Ok::<Bytes, std::convert::Infallible>(data));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/x-flv")
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .header(header::CONNECTION, "keep-alive")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(Body::from_stream(stream))
        .unwrap()
}
