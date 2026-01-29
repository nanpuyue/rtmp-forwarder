use std::sync::Arc;
use tokio::sync::broadcast;
use bytes::{Bytes, BytesMut};
use crate::stream_manager::{StreamManager, StreamMessage};
use crate::rtmp::RtmpMessage;
use tracing::info;

pub struct FlvManager {
    stream_manager: Arc<StreamManager>,
    // 直接使用广播通道，无需HashMap和锁
    broadcast_tx: broadcast::Sender<Bytes>,
}

impl FlvManager {
    pub fn new(stream_manager: Arc<StreamManager>) -> Self {
        let (broadcast_tx, _) = broadcast::channel(1024);
        Self {
            stream_manager,
            broadcast_tx,
        }
    }
    
    pub async fn run(self: Arc<Self>) {
        let mut msg_rx = self.stream_manager.subscribe();
        info!("FlvManager started");
        
        while let Ok(stream_msg) = msg_rx.recv().await {
            if let StreamMessage::RtmpMessage(msg) = stream_msg {
                if let Some(flv_data) = self.rtmp_to_flv(&msg) {
                    self.broadcast_flv("stream", flv_data).await;
                }
            }
        }
        
        info!("FlvManager stopped");
    }
    
    async fn broadcast_flv(&self, _stream_name: &str, data: Bytes) {
        // 直接使用广播通道发送数据
        let _ = self.broadcast_tx.send(data);
    }
    
    pub async fn subscribe_flv(&self, _stream_name: &str) -> (broadcast::Receiver<Bytes>, Bytes) {
        // 使用 BytesMut 进行高效拼接
        let mut header_data = BytesMut::new();
        
        // 添加 FLV 文件头
        if let Some(header) = self.create_flv_header() {
            header_data.extend_from_slice(&header);
        }
        
        // 添加序列头信息
        if let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
            // 添加视频序列头
            if let Some(ref video_hdr) = snapshot.video_seq_hdr {
                if let Some(flv_tag) = self.create_flv_video_tag(video_hdr, 0) {
                    header_data.extend_from_slice(&flv_tag);
                }
            }
            
            // 添加音频序列头
            if let Some(ref audio_hdr) = snapshot.audio_seq_hdr {
                if let Some(flv_tag) = self.create_flv_audio_tag(audio_hdr, 0) {
                    header_data.extend_from_slice(&flv_tag);
                }
            }
        }
        
        // 返回广播通道订阅者和完整的头部数据
        (self.broadcast_tx.subscribe(), header_data.freeze())
    }
    
    fn rtmp_to_flv(&self, msg: &RtmpMessage) -> Option<Bytes> {
        match msg.msg_type {
            8 => self.create_flv_audio_tag(&msg.payload.clone().freeze(), msg.timestamp),
            9 => self.create_flv_video_tag(&msg.payload.clone().freeze(), msg.timestamp),
            18 => self.create_flv_script_tag(&msg.payload.clone().freeze(), msg.timestamp),
            _ => None,
        }
    }
    
    fn create_flv_header(&self) -> Option<Bytes> {
        let mut buf = Vec::with_capacity(13);
        buf.extend_from_slice(b"FLV");
        buf.push(1);
        buf.push(5);
        buf.extend_from_slice(&9u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        Some(Bytes::from(buf))
    }
    
    fn create_flv_audio_tag(&self, data: &Bytes, timestamp: u32) -> Option<Bytes> {
        self.create_flv_tag(8, data, timestamp)
    }
    
    fn create_flv_video_tag(&self, data: &Bytes, timestamp: u32) -> Option<Bytes> {
        self.create_flv_tag(9, data, timestamp)
    }
    
    fn create_flv_script_tag(&self, data: &Bytes, timestamp: u32) -> Option<Bytes> {
        self.create_flv_tag(18, data, timestamp)
    }
    
    fn create_flv_tag(&self, tag_type: u8, data: &Bytes, timestamp: u32) -> Option<Bytes> {
        let data_size = data.len() as u32;
        let mut buf = Vec::with_capacity(11 + data.len() + 4);
        
        buf.push(tag_type);
        buf.extend_from_slice(&data_size.to_be_bytes()[1..]);
        buf.extend_from_slice(&timestamp.to_be_bytes()[1..]);
        buf.push((timestamp >> 24) as u8);
        buf.extend_from_slice(&[0, 0, 0]);
        buf.extend_from_slice(data);
        buf.extend_from_slice(&(11 + data_size).to_be_bytes());
        
        Some(Bytes::from(buf))
    }
}
