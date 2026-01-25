use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router, Extension,
    http::{header, StatusCode, Uri},
};
use std::net::SocketAddr;
use crate::config::{SharedConfig, AppConfig, save_config};
use tracing::info;
use tower_http::cors::CorsLayer;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "static/"]
struct Assets;

pub async fn start_web_server(config: SharedConfig) {
    let addr_str = config.read().unwrap().web_addr.clone();
    let addr: SocketAddr = addr_str.parse().unwrap_or(([0, 0, 0, 0], 8080).into());

    let app = Router::new()
        .route("/api/config", get(get_config))
        .route("/api/config", post(update_config))
        .fallback(static_handler)
        .layer(Extension(config))
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
            // Fallback for SPA or not found
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
    Json(new_config): Json<AppConfig>,
) -> Json<bool> {
    let mut c = config.write().unwrap();
    *c = new_config.clone();
    
    // Persist to file
    let success = save_config(&c).is_ok();
    Json(success)
}
