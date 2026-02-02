use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router, Extension,
    http::{header, StatusCode, Uri},
    extract::ws::{WebSocket, WebSocketUpgrade},
    body::Body,
};
use std::net::SocketAddr;
use crate::config::{GetForwarders, SharedConfig, WebConfig};
use crate::flv_manager::FlvManager;
use crate::forwarder_manager::ForwarderCommand;
use crate::stream_manager::{StreamManager, StreamMessage, StreamEvent};
use tokio::sync::mpsc;
use bytes::Bytes;
use tokio_stream::wrappers::BroadcastStream;
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
    stream_manager: Arc<StreamManager>,
) {
    let addr_str = config.read().unwrap().web_addr.clone();
    let addr: SocketAddr = addr_str.parse().unwrap_or(([0, 0, 0, 0], 8080).into());

    let app = Router::new()
        .route("/api/config", get(get_config))
        .route("/api/config", post(update_config))
        .route("/live/stream.flv", get(handle_flv_stream))
        .route("/ws/stream-status", get(ws_handler))
        .fallback(static_handler)
        .layer(Extension(config))
        .layer(Extension(flv_manager))
        .layer(Extension(forwarder_cmd_tx))
        .layer(Extension(stream_manager))
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

async fn get_config(Extension(config): Extension<SharedConfig>) -> Json<WebConfig> {
    let c = config.read().unwrap();
    Json(WebConfig::from(&*c))
}


async fn update_config(
    Extension(config): Extension<SharedConfig>,
    Extension(forwarder_cmd_tx): Extension<mpsc::UnboundedSender<ForwarderCommand>>,
    Json(web_config): Json<WebConfig>,
) -> Json<bool> {
    let success= {
        let mut c = config.write().unwrap();
        c.update_from_web_config(&web_config);
        c.save().is_ok()
    };

    if success {
        let forwarders = web_config.get_forwarders();
        info!("Config saved, notifying ForwarderManager with {} forwarders", forwarders.len());
        forwarder_cmd_tx.send(ForwarderCommand::UpdateConfig(forwarders)).ok();
    }
    
    Json(success)
}

pub async fn handle_flv_stream(
    Extension(manager): Extension<Arc<FlvManager>>,
) -> impl IntoResponse {
    tracing::info!("HTTP-FLV: Request for stream");
    
    // 获取广播通道订阅者和头部数据
    let (rx, header_data) = manager.subscribe_flv().await;
    // 创建一个流，先发送头部信息
    let header_stream = tokio_stream::iter(vec![Ok::<Bytes, std::convert::Infallible>(header_data)]);
    // 创建剩余数据流
    let remaining_stream =BroadcastStream::new(rx).map(|data| Ok::<Bytes, std::convert::Infallible>(data.unwrap()));
    let stream = header_stream.chain(remaining_stream);

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

async fn ws_handler(
    ws: WebSocketUpgrade,
    Extension(stream_manager): Extension<Arc<StreamManager>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, stream_manager))
}

async fn handle_socket(mut socket: WebSocket, stream_manager: Arc<StreamManager>) {
    let mut rx = stream_manager.subscribe();
    
    let snapshot = stream_manager.get_stream_snapshot().await;
    let status = if snapshot.is_some() { "publishing" } else { "idle" };
    let _ = socket.send(axum::extract::ws::Message::Text(
        serde_json::json!({"status": status}).to_string()
    )).await;
    
    loop {
        tokio::select! {
            Ok(msg) = rx.recv() => {
                if let StreamMessage::StateChanged(event) = msg {
                    let status = match event {
                        StreamEvent::StreamPublishing => "publishing",
                        StreamEvent::StreamIdle | StreamEvent::StreamClosed | StreamEvent::StreamDeleted => "idle",
                        _ => continue,
                    };
                    if socket.send(axum::extract::ws::Message::Text(
                        serde_json::json!({"status": status}).to_string()
                    )).await.is_err() {
                        break;
                    }
                }
            }
            msg = socket.recv() => {
                if msg.is_none() {
                    break;
                }
            }
        }
    }
}