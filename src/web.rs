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
use tokio_stream::StreamExt;

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
    tracing::debug!("HTTP API: Stream list requested, found {} streams: {:?}", stream_ids.len(), stream_ids);
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
    /// 视频序列头
    video_seq_hdr: RwLock<Option<Bytes>>,
    /// 音频序列头
    audio_seq_hdr: RwLock<Option<Bytes>>,
    /// 最后一个关键帧
    last_keyframe: RwLock<Option<Bytes>>,
    /// 是否支持H.264+AAC
    is_h264_aac: RwLock<bool>,
    /// 是否已发送关键帧
    keyframe_sent: RwLock<bool>,
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
            video_seq_hdr: RwLock::new(None),
            audio_seq_hdr: RwLock::new(None),
            last_keyframe: RwLock::new(None),
            is_h264_aac: RwLock::new(false),
            keyframe_sent: RwLock::new(false),
        });

        streams.insert(stream_id.to_string(), state.clone());
        tracing::info!("FLV Manager: Created new stream state for: {}", stream_id);
        state
    }

    /// 处理RTMP消息并转发到FLV流
    pub async fn handle_rtmp_message(&self, stream_id: &str, msg: &RtmpMessage) {
        tracing::debug!("FLV Manager: Received RTMP message for stream: {}, type: {}, size: {}", 
                       stream_id, msg.msg_type, msg.payload.len());
        
        let state = self.get_or_create_stream(stream_id).await;
        
        // 检查并保存序列头
        if self.is_sequence_header(msg) {
            self.save_sequence_header(stream_id, msg).await;
            *state.is_h264_aac.write().await = true;
            // 重置关键帧状态，等待新的GOP
            *state.keyframe_sent.write().await = false;
            tracing::info!("FLV Manager: Stream {} sequence header updated, waiting for keyframe", stream_id);
            return;
        }

        // 只处理支持的编码格式
        if !*state.is_h264_aac.read().await {
            return;
        }

        // 检查是否为关键帧
        let is_keyframe = self.is_keyframe(msg);
        if is_keyframe {
            *state.keyframe_sent.write().await = true;
            // 保存关键帧但重置时间戳为0
            if let Some(flv_data) = self.convert_to_flv_with_timestamp(msg, 0) {
                *state.last_keyframe.write().await = Some(flv_data);
                tracing::info!("FLV Manager: Saved keyframe for stream: {}", stream_id);
            }
        }

        // 只有在发送过关键帧后才广播其他帧
        if !*state.keyframe_sent.read().await {
            return;
        }

        // 转换为FLV格式并广播
        if let Some(flv_data) = self.convert_to_flv(msg) {
            let _ = state.tx.send(flv_data);
        }
    }

    /// 检查是否为关键帧
    fn is_keyframe(&self, msg: &RtmpMessage) -> bool {
        if msg.msg_type == 9 && msg.payload.len() >= 1 {
            let frame_type = (msg.payload[0] >> 4) & 0x0F;
            frame_type == 1 // 1 = keyframe
        } else {
            false
        }
    }

    /// 检查是否为序列头
    fn is_sequence_header(&self, msg: &RtmpMessage) -> bool {
        match msg.msg_type {
            9 if msg.payload.len() >= 2 => {
                let frame_type = (msg.payload[0] >> 4) & 0x0F;
                let codec_id = msg.payload[0] & 0x0F;
                let avc_packet_type = msg.payload[1];
                frame_type == 1 && codec_id == 7 && avc_packet_type == 0
            },
            8 if msg.payload.len() >= 2 => {
                let sound_format = (msg.payload[0] >> 4) & 0x0F;
                let aac_packet_type = msg.payload[1];
                sound_format == 10 && aac_packet_type == 0
            },
            _ => false,
        }
    }

    /// 转换RTMP消息为FLV格式（带时间戳重写）
    fn convert_to_flv_with_timestamp(&self, msg: &RtmpMessage, timestamp: u32) -> Option<Bytes> {
        match msg.msg_type {
            9 => {
                let mut flv_tag = BytesMut::new();
                flv_tag.put_u8(0x09);
                flv_tag.put_u24(msg.payload.len() as u32);
                flv_tag.put_u24(timestamp & 0xFFFFFF);
                flv_tag.put_u8(((timestamp >> 24) & 0xFF) as u8);
                flv_tag.put_u24(0);
                flv_tag.extend_from_slice(&msg.payload);
                flv_tag.put_u32((msg.payload.len() + 11) as u32);
                Some(flv_tag.freeze())
            }
            8 => {
                let mut flv_tag = BytesMut::new();
                flv_tag.put_u8(0x08);
                flv_tag.put_u24(msg.payload.len() as u32);
                flv_tag.put_u24(timestamp & 0xFFFFFF);
                flv_tag.put_u8(((timestamp >> 24) & 0xFF) as u8);
                flv_tag.put_u24(0);
                flv_tag.extend_from_slice(&msg.payload);
                flv_tag.put_u32((msg.payload.len() + 11) as u32);
                Some(flv_tag.freeze())
            }
            _ => self.convert_to_flv(msg)
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
                
                // 时间戳处理 - FLV格式为大端序低24位
                flv_tag.put_u24(msg.timestamp & 0xFFFFFF);
                
                // 时间戳扩展 - 高8位
                flv_tag.put_u8(((msg.timestamp >> 24) & 0xFF) as u8);
                
                flv_tag.put_u24(0); // StreamID
                
                // FLV Video Tag Body - 直接使用RTMP payload
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
                
                // 时间戳处理 - FLV格式为大端序低24位
                flv_tag.put_u24(msg.timestamp & 0xFFFFFF);
                
                // 时间戳扩展 - 高8位
                flv_tag.put_u8(((msg.timestamp >> 24) & 0xFF) as u8);
                
                flv_tag.put_u24(0); // StreamID
                
                // FLV Audio Tag Body - 直接使用RTMP payload
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
                flv_tag.put_u24(msg.timestamp & 0xFFFFFF); // Timestamp
                flv_tag.put_u8(((msg.timestamp >> 24) & 0xFF) as u8); // Timestamp Extended
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

    /// 保存序列头
    async fn save_sequence_header(&self, stream_id: &str, msg: &RtmpMessage) {
        let state = self.get_or_create_stream(stream_id).await;
        
        match msg.msg_type {
            9 if msg.payload.len() >= 2 => {
                let frame_type = (msg.payload[0] >> 4) & 0x0F;
                let codec_id = msg.payload[0] & 0x0F;
                let avc_packet_type = msg.payload[1];
                
                if frame_type == 1 && codec_id == 7 && avc_packet_type == 0 {
                    // 序列头时间戳设为0
                    if let Some(flv_data) = self.convert_to_flv_with_timestamp(msg, 0) {
                        *state.video_seq_hdr.write().await = Some(flv_data);
                        tracing::info!("FLV Manager: Saved video sequence header for stream: {}", stream_id);
                    }
                }
            },
            8 if msg.payload.len() >= 2 => {
                let sound_format = (msg.payload[0] >> 4) & 0x0F;
                let aac_packet_type = msg.payload[1];
                
                if sound_format == 10 && aac_packet_type == 0 {
                    // 序列头时间戳设为0
                    if let Some(flv_data) = self.convert_to_flv_with_timestamp(msg, 0) {
                        *state.audio_seq_hdr.write().await = Some(flv_data);
                        tracing::info!("FLV Manager: Saved audio sequence header for stream: {}", stream_id);
                    }
                }
            },
            _ => {}
        }
    }

    /// 获取序列头数据
    pub async fn get_sequence_headers(&self, stream_id: &str) -> (Option<Bytes>, Option<Bytes>, Option<Bytes>) {
        let streams = self.streams.read().await;
        if let Some(state) = streams.get(stream_id) {
            let video_hdr = state.video_seq_hdr.read().await.clone();
            let audio_hdr = state.audio_seq_hdr.read().await.clone();
            let keyframe = state.last_keyframe.read().await.clone();
            (video_hdr, audio_hdr, keyframe)
        } else {
            (None, None, None)
        }
    }
}

/// HTTP-FLV流处理器
pub async fn handle_flv_stream(
    Path(_stream_id): Path<String>,
    Extension(manager): Extension<std::sync::Arc<FlvStreamManager>>,
) -> impl IntoResponse {
    tracing::info!("HTTP-FLV: Request for fixed stream: stream");
    
    // 固定使用 "stream" 作为流名称
    let stream_name = "stream".to_string();
    
    // 检查流是否存在且支持H.264+AAC
    let streams = manager.streams.read().await;
    let state = match streams.get(&stream_name) {
        Some(state) if *state.is_h264_aac.read().await => {
            tracing::info!("HTTP-FLV: Stream {} is available and supports H.264+AAC", stream_name);
            state.clone()
        },
        Some(_) => {
            tracing::warn!("HTTP-FLV: Stream {} exists but does not support H.264+AAC", stream_name);
            return StatusCode::NOT_FOUND.into_response();
        },
        None => {
            tracing::warn!("HTTP-FLV: Stream {} not found", stream_name);
            return StatusCode::NOT_FOUND.into_response();
        }
    };
    drop(streams);

    // 创建FLV头部
    let flv_header = create_flv_header();
    
    // 获取序列头和最后一个关键帧
    let (video_seq_hdr, audio_seq_hdr, last_keyframe) = manager.get_sequence_headers(&stream_name).await;
    
    // 订阅广播流
    let rx = state.tx.subscribe();
    let stream = tokio_stream::wrappers::BroadcastStream::new(rx)
        .map(|result: Result<Bytes, _>| {
            match result {
                Ok(data) => Ok::<Bytes, std::convert::Infallible>(data),
                Err(_) => Ok::<Bytes, std::convert::Infallible>(Bytes::new())
            }
        });

    // 构建初始数据流：FLV头部 + 序列头 + 关键帧 + 实时数据
    let mut initial_data = vec![Ok::<Bytes, std::convert::Infallible>(flv_header)];
    
    // 先发送视频序列头
    if let Some(video_hdr) = video_seq_hdr {
        initial_data.push(Ok(video_hdr));
        tracing::info!("HTTP-FLV: Added video sequence header for stream: {}", stream_name);
    }
    
    // 再发送音频序列头
    if let Some(audio_hdr) = audio_seq_hdr {
        initial_data.push(Ok(audio_hdr));
        tracing::info!("HTTP-FLV: Added audio sequence header for stream: {}", stream_name);
    }
    
    // 最后发送关键帧（时间戳为0，作为参考帧）
    if let Some(keyframe) = last_keyframe {
        initial_data.push(Ok(keyframe));
        tracing::info!("HTTP-FLV: Added reference keyframe for stream: {}", stream_name);
    }
    
    let flv_stream = tokio_stream::iter(initial_data).chain(stream);

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

/// 创建FLV文件头部
fn create_flv_header() -> Bytes {
    let mut header = BytesMut::with_capacity(13);
    header.put_slice(b"FLV\x01\x05\x00\x00\x00\x09");
    header.put_u32(0); // PreviousTagSize0
    header.freeze()
}