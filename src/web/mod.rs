use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ws::{self, WebSocket, WebSocketUpgrade};
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use rust_embed::RustEmbed;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use tower_http::cors::CorsLayer;
use tracing::info;

use self::auth::BasicAuth;
use crate::config::{GetForwarders, SharedConfig, WebConfig};
use crate::forwarder::ForwarderManagerCommand;
use crate::stream::{FlvManager, StreamEvent, StreamManager, StreamMessage};

mod auth;

#[derive(RustEmbed)]
#[folder = "static/"]
struct Assets;

pub async fn start_web_server(
    config: SharedConfig,
    flv_manager: Arc<FlvManager>,
    forwarder_cmd_tx: mpsc::Sender<ForwarderManagerCommand>,
    stream_manager: Arc<StreamManager>,
) {
    let web_config = config.read().unwrap().web.clone();
    let addr: SocketAddr = web_config
        .addr
        .parse()
        .unwrap_or(([0, 0, 0, 0], 8080).into());

    let mut app = Router::new()
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
    if web_config.username.is_some() || web_config.password.is_some() {
        info!("Web dashboard requires authentication");
        let basic_auth = BasicAuth::new(web_config.username, web_config.password);
        app = app.layer(AsyncRequireAuthorizationLayer::new(basic_auth));
    }

    info!("Web dashboard available at http://{}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();

    if path.is_empty() {
        path = "index.html".to_string();
    }

    match Assets::get(&path) {
        Some(content) => {
            let content_type = if path.ends_with(".html") {
                "text/html"
            } else if path.ends_with(".js") {
                "application/javascript"
            } else if path.ends_with(".css") {
                "text/css"
            } else {
                "application/octet-stream"
            };

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
    Extension(forwarder_cmd_tx): Extension<mpsc::Sender<ForwarderManagerCommand>>,
    Json(web_config): Json<WebConfig>,
) -> Json<bool> {
    let success = {
        let mut c = config.write().unwrap();
        c.update_from_web_config(&web_config);
        c.save().is_ok()
    };

    if success {
        let forwarders = web_config.get_forwarders();
        info!(
            "Config saved, notifying ForwarderManager with {} forwarders",
            forwarders.len()
        );
        forwarder_cmd_tx
            .send(ForwarderManagerCommand::UpdateConfig(forwarders))
            .await
            .ok();
    }

    Json(success)
}

pub async fn handle_flv_stream(
    Extension(manager): Extension<Arc<FlvManager>>,
) -> impl IntoResponse {
    info!("HTTP-FLV: Request for stream");

    // 获取 flv 头部数据和流广播
    let (header, rx) = manager.subscribe_flv().await;
    let flv_stream = tokio_stream::once(Ok(header)).chain(BroadcastStream::new(rx));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "video/x-flv")
        .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
        .header(header::PRAGMA, "no-cache")
        .header(header::EXPIRES, "0")
        .header(header::CONNECTION, "keep-alive")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(Body::from_stream(flv_stream))
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
    let status = if snapshot.is_some() {
        "publishing"
    } else {
        "idle"
    };
    let _ = socket
        .send(ws::Message::text(json!({"status": status}).to_string()))
        .await;

    loop {
        tokio::select! {
            Ok(msg) = rx.recv() => {
                if let StreamMessage::StateChanged(event) = msg {
                    let status = match event {
                        StreamEvent::Publishing => "publishing",
                        StreamEvent::Idle | StreamEvent::Closed | StreamEvent::Deleted => "idle",
                        _ => continue,
                    };
                    if socket.send(ws::Message::text(
                        json!({"status": status}).to_string()
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
