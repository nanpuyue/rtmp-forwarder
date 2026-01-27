use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};
use bytes::Bytes;
use crate::stream_manager::{StreamManager, StreamMessage};
use crate::rtmp::RtmpMessage;
use tracing::info;

pub struct FlvManager {
    stream_manager: Arc<StreamManager>,
    subscribers: Arc<RwLock<HashMap<String, Vec<mpsc::Sender<Bytes>>>>>,
}

impl FlvManager {
    pub fn new(stream_manager: Arc<StreamManager>) -> Self {
        Self {
            stream_manager,
            subscribers: Arc::new(RwLock::new(HashMap::new())),
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
    
    async fn broadcast_flv(&self, stream_name: &str, data: Bytes) {
        let mut subs = self.subscribers.write().await;
        if let Some(clients) = subs.get_mut(stream_name) {
            clients.retain(|tx| tx.try_send(data.clone()).is_ok());
        }
    }
    
    pub async fn subscribe_flv(&self, stream_name: &str) -> mpsc::Receiver<Bytes> {
        let (tx, rx) = mpsc::channel(64);
        
        if let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
            if let Some(header) = self.create_flv_header() {
                tx.try_send(header).ok();
            }
            
            if let Some(ref video_hdr) = snapshot.video_seq_hdr {
                if let Some(flv_tag) = self.create_flv_video_tag(video_hdr, 0) {
                    tx.try_send(flv_tag).ok();
                }
            }
            
            if let Some(ref audio_hdr) = snapshot.audio_seq_hdr {
                if let Some(flv_tag) = self.create_flv_audio_tag(audio_hdr, 0) {
                    tx.try_send(flv_tag).ok();
                }
            }
        }
        
        self.subscribers.write().await
            .entry(stream_name.to_string())
            .or_default()
            .push(tx);
        
        rx
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
