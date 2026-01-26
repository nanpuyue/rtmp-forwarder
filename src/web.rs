use axum::{
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router, Extension,
    http::{header, StatusCode, Uri},
    extract::Path,
    body::Body,
};
use std::net::SocketAddr;
use crate::config::{SharedConfig, AppConfig, save_config};
use crate::rtmp::{RtmpMessage, PutU24};
use bytes::{Bytes, BytesMut};
use tokio::sync::{broadcast, RwLock};
use tracing::info;
use tower_http::cors::CorsLayer;
use rust_embed::RustEmbed;
use bytes::BufMut;
use tokio_stream::{StreamExt, once};

#[derive(RustEmbed)]
#[folder = "static/"]
struct Assets;

pub async fn start_web_server(config: SharedConfig, flv_manager: std::sync::Arc<FlvStreamManager>) {
    let addr_str = config.read().unwrap().web_addr.clone();
    let addr: SocketAddr = addr_str.parse().unwrap_or(([0, 0, 0, 0], 8080).into());

    let app = Router::new()
        .route("/api/config", get(get_config))
        .route("/api/config", post(update_config))
        .route("/api/streams", get(get_streams))
        .route("/live/:stream_id.flv", get(handle_flv_stream))
        .fallback(static_handler)
        .layer(Extension(config))
        .layer(Extension(flv_manager))
        .layer(CorsLayer::permissive());

    info!("Web dashboard available at http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches("/").to_string();

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

async fn get_streams(Extension(manager): Extension<std::sync::Arc<FlvStreamManager>>) -> Json<Vec<String>> {
    let streams = manager.streams.read().await;
    let stream_ids: Vec<String> = streams.keys().cloned().collect();
    Json(stream_ids)
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

// HTTP-FLV 实时预览功能

/// FLV流管理器
pub struct FlvStreamManager {
    streams: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<FlvStreamState>>>>,
}

/// 单个FLV流的状态
struct FlvStreamState {
    /// 广播通道，用于向多个HTTP客户端发送数据
    tx: broadcast::Sender<Bytes>,
    /// 流的元数据
    metadata: RwLock<Option<Bytes>>,
    /// 视频序列头
    video_seq_hdr: RwLock<Option<Bytes>>,
    /// 音频序列头
    audio_seq_hdr: RwLock<Option<Bytes>>,
    /// 是否支持H.264+AAC
    is_h264_aac: RwLock<bool>,
}

impl FlvStreamManager {
    pub fn new() -> Self {
        Self {
            streams: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// 获取或创建流状态
    async fn get_or_create_stream(&self, stream_id: &str) -> std::sync::Arc<FlvStreamState> {
        let mut streams = self.streams.write().await;
        
        if let Some(state) = streams.get(stream_id) {
            return state.clone();
        }

        let (tx, _) = broadcast::channel(1024);
        let state = std::sync::Arc::new(FlvStreamState {
            tx,
            metadata: RwLock::new(None),
            video_seq_hdr: RwLock::new(None),
            audio_seq_hdr: RwLock::new(None),
            is_h264_aac: RwLock::new(false),
        });

        streams.insert(stream_id.to_string(), state.clone());
        state
    }

    /// 处理RTMP消息并转发到FLV流
    pub async fn handle_rtmp_message(&self, stream_id: &str, msg: &RtmpMessage) {
        tracing::debug!("FLV Manager: Received RTMP message for stream: {}, type: {}, size: {}", 
                       stream_id, msg.msg_type, msg.payload.len());
        
        let state = self.get_or_create_stream(stream_id).await;
        
        // 检查编码格式
        if !*state.is_h264_aac.read().await {
            if !self.check_codec_support(msg) {
                tracing::debug!("FLV Manager: Unsupported codec for stream: {}", stream_id);
                return;
            }
            // 更新编码支持状态
            let mut streams = self.streams.write().await;
            if let Some(s) = streams.get_mut(stream_id) {
                *s.is_h264_aac.write().await = true;
                tracing::info!("FLV Manager: Stream {} now supports H.264+AAC", stream_id);
            }
        }

        // 转换为FLV格式并广播
        if let Some(flv_data) = self.convert_to_flv(msg) {
            if let Err(_) = state.tx.send(flv_data) {
                // 广播失败，可能是没有客户端连接
                tracing::debug!("FLV Manager: Broadcast failed for stream: {}", stream_id);
            } else {
                tracing::debug!("FLV Manager: Successfully broadcasted message for stream: {}", stream_id);
            }
        } else {
            tracing::debug!("FLV Manager: Message not converted to FLV for stream: {}", stream_id);
        }
    }

    /// 检查是否支持H.264+AAC编码
    fn check_codec_support(&self, msg: &RtmpMessage) -> bool {
        match msg.msg_type {
            // 视频序列头 (H.264)
            9 if msg.payload.len() >= 2 && msg.payload[0] == 0x17 && msg.payload[1] == 0 => true,
            // 音频序列头 (AAC)
            8 if msg.payload.len() >= 2 && (msg.payload[0] >> 4) == 10 && msg.payload[1] == 0 => true,
            _ => false,
        }
    }

    /// 转换RTMP消息为FLV格式
    fn convert_to_flv(&self, msg: &RtmpMessage) -> Option<Bytes> {
        match msg.msg_type {
            // 视频数据 (H.264)
            9 => {
                let mut flv_tag = BytesMut::new();
                
                // FLV Tag Header
                flv_tag.put_u8(0x09); // Tag Type: Video
                flv_tag.put_u24(msg.payload.len() as u32); // Data Size
                flv_tag.put_u24(msg.timestamp); // Timestamp
                flv_tag.put_u8((msg.timestamp >> 24) as u8); // Timestamp Extended
                flv_tag.put_u24(0); // StreamID
                
                // FLV Video Tag Body
                flv_tag.extend_from_slice(&msg.payload);
                
                // Previous Tag Size
                flv_tag.put_u32((msg.payload.len() + 11) as u32);
                
                Some(flv_tag.freeze())
            }
            // 音频数据 (AAC)
            8 => {
                let mut flv_tag = BytesMut::new();
                
                // FLV Tag Header
                flv_tag.put_u8(0x08); // Tag Type: Audio
                flv_tag.put_u24(msg.payload.len() as u32); // Data Size
                flv_tag.put_u24(msg.timestamp); // Timestamp
                flv_tag.put_u8((msg.timestamp >> 24) as u8); // Timestamp Extended
                flv_tag.put_u24(0); // StreamID
                
                // FLV Audio Tag Body
                flv_tag.extend_from_slice(&msg.payload);
                
                // Previous Tag Size
                flv_tag.put_u32((msg.payload.len() + 11) as u32);
                
                Some(flv_tag.freeze())
            }
            // 元数据
            18 | 15 => {
                let mut flv_tag = BytesMut::new();
                
                // FLV Tag Header
                flv_tag.put_u8(0x12); // Tag Type: Script
                flv_tag.put_u24(msg.payload.len() as u32); // Data Size
                flv_tag.put_u24(msg.timestamp); // Timestamp
                flv_tag.put_u8((msg.timestamp >> 24) as u8); // Timestamp Extended
                flv_tag.put_u24(0); // StreamID
                
                // FLV Script Tag Body
                flv_tag.extend_from_slice(&msg.payload);
                
                // Previous Tag Size
                flv_tag.put_u32((msg.payload.len() + 11) as u32);
                
                Some(flv_tag.freeze())
            }
            _ => None,
        }
    }
}

    /// HTTP-FLV流处理器
pub async fn handle_flv_stream(
    Path(stream_id): Path<String>,
    Extension(manager): Extension<std::sync::Arc<FlvStreamManager>>,
) -> impl IntoResponse {
    // 检查流是否存在且支持H.264+AAC
    let streams = manager.streams.read().await;
    let state = match streams.get(&stream_id) {
        Some(state) if *state.is_h264_aac.read().await => state.clone(),
        _ => {
            return StatusCode::NOT_FOUND.into_response();
        }
    };
    drop(streams);

    // 简化的FLV流响应
    let rx = state.tx.subscribe();
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
        .map(|result: Result<Bytes, _>| Ok::<Bytes, std::convert::Infallible>(result.unwrap_or_else(|_| Bytes::new())));

    // 发送FLV头部
    let flv_header = Bytes::from_static(b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00");
    
    let flv_stream = once(Ok::<Bytes, std::convert::Infallible>(flv_header)).chain(stream);

    Response::builder()
        .header(header::CONTENT_TYPE, "video/x-flv")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(flv_stream))
        .unwrap()
}

/// 创建FLV文件头部
fn create_flv_header() -> bool {
    false // 返回false表示需要发送FLV头部
}

/// 为Router添加FLV流路由
pub fn add_flv_routes(router: Router) -> Router {
    router.route("/live/:stream_id.flv", get(handle_flv_stream))
}