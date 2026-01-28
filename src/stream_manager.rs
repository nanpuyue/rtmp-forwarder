use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, broadcast};
use bytes::Bytes;
use crate::rtmp::RtmpMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    Idle,
    Publishing,
    Closed,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    StreamCreated,
    StreamPublishing,
    StreamIdle,
    StreamClosed,
    StreamDeleted,
}

#[derive(Debug, Clone)]
pub enum StreamMessage {
    RtmpMessage(RtmpMessage),
    StateChanged(StreamEvent),
}

#[derive(Clone)]
pub struct StreamSnapshot {
    pub app_name: String,
    pub stream_key: String,
    pub metadata: Option<Bytes>,
    pub video_seq_hdr: Option<Bytes>,
    pub audio_seq_hdr: Option<Bytes>,
}

#[derive(Debug)]
pub struct StreamInfo {
    pub stream_id: u32,
    pub stream_key: String,
    pub app_name: String,
    pub state: StreamState,
    pub publishing_client: Option<u32>,
    pub last_active: Instant,
    pub metadata: Option<Bytes>,
    pub video_seq_hdr: Option<Bytes>,
    pub audio_seq_hdr: Option<Bytes>,
}

#[derive(Debug)]
pub enum StreamError {
    AlreadyPublishing,
    StreamNotFound,
    NotPublishingClient,
}

pub struct StreamManager {
    default_stream: Arc<RwLock<Option<StreamInfo>>>,
    message_tx: broadcast::Sender<StreamMessage>,
}

impl StreamManager {
    pub fn new() -> Self {
        let (msg_tx, _) = broadcast::channel(1024);
        Self {
            default_stream: Arc::new(RwLock::new(None)),
            message_tx: msg_tx,
        }
    }
    
    pub fn subscribe(&self) -> broadcast::Receiver<StreamMessage> {
        self.message_tx.subscribe()
    }
    
    pub async fn handle_rtmp_message(&self, msg: RtmpMessage) {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_mut() {
            if s.state == StreamState::Publishing {
                match msg.msg_type {
                    18 | 15 => {
                        s.metadata = Some(msg.payload.clone().freeze());
                    }
                    9 if msg.payload.len() >= 2 && msg.payload[0] == 0x17 && msg.payload[1] == 0 => {
                        s.video_seq_hdr = Some(msg.payload.clone().freeze());
                    }
                    8 if msg.payload.len() >= 2 && (msg.payload[0] >> 4) == 10 && msg.payload[1] == 0 => {
                        s.audio_seq_hdr = Some(msg.payload.clone().freeze());
                    }
                    _ => {}
                }
                s.last_active = Instant::now();
                drop(stream);
                
                self.message_tx.send(StreamMessage::RtmpMessage(msg)).ok();
            }
        }
    }
    
    pub async fn get_stream_snapshot(&self) -> Option<StreamSnapshot> {
        let stream = self.default_stream.read().await;
        stream.as_ref()
            .filter(|s| s.state == StreamState::Publishing)
            .map(|s| StreamSnapshot {
                app_name: s.app_name.clone(),
                stream_key: s.stream_key.clone(),
                metadata: s.metadata.clone(),
                video_seq_hdr: s.video_seq_hdr.clone(),
                audio_seq_hdr: s.audio_seq_hdr.clone(),
            })
    }

    pub async fn handle_connect(&self) -> Result<(), StreamError> {
        let stream = self.default_stream.read().await;
        if let Some(s) = stream.as_ref() {
            if s.state == StreamState::Publishing {
                return Err(StreamError::AlreadyPublishing);
            }
        }
        Ok(())
    }

    pub async fn handle_create_stream(&self, app_name: &str, _client_id: u32) -> Result<u32, StreamError> {
        let mut stream = self.default_stream.write().await;
        
        match stream.as_ref() {
            Some(s) if s.state == StreamState::Publishing => {
                Err(StreamError::AlreadyPublishing)
            }
            Some(s) if s.state == StreamState::Idle || s.state == StreamState::Closed => {
                Ok(s.stream_id)
            }
            None => {
                *stream = Some(StreamInfo {
                    stream_id: 1,
                    stream_key: "stream".to_string(),
                    app_name: app_name.to_string(),
                    state: StreamState::Idle,
                    publishing_client: None,
                    last_active: Instant::now(),
                    metadata: None,
                    video_seq_hdr: None,
                    audio_seq_hdr: None,
                });
                drop(stream);
                
                self.message_tx.send(StreamMessage::StateChanged(StreamEvent::StreamCreated)).ok();
                Ok(1)
            }
            _ => unreachable!(),
        }
    }

    pub async fn handle_publish(&self, _stream_id: u32, stream_key: &str, client_id: u32, app_name: &str) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        let s = stream.as_mut().ok_or(StreamError::StreamNotFound)?;
        
        match s.state {
            StreamState::Publishing => Err(StreamError::AlreadyPublishing),
            StreamState::Idle | StreamState::Closed => {
                s.state = StreamState::Publishing;
                s.publishing_client = Some(client_id);
                s.stream_key = stream_key.to_string();
                s.app_name = app_name.to_string();
                s.last_active = Instant::now();
                s.metadata = None;
                s.video_seq_hdr = None;
                s.audio_seq_hdr = None;
                drop(stream);
                
                self.message_tx.send(StreamMessage::StateChanged(StreamEvent::StreamPublishing)).ok();
                Ok(())
            }
        }
    }

    pub async fn handle_unpublish(&self, _stream_id: u32, client_id: u32) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_mut() {
            if s.state == StreamState::Publishing {
                if let Some(pub_client) = s.publishing_client {
                    if pub_client != client_id {
                        return Err(StreamError::NotPublishingClient);
                    }
                }
                
                s.state = StreamState::Idle;
                s.publishing_client = None;
                drop(stream);
                
                self.message_tx.send(StreamMessage::StateChanged(StreamEvent::StreamIdle)).ok();
            }
        }
        Ok(())
    }

    pub async fn handle_close_stream(&self, _stream_id: u32, client_id: u32) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_mut() {
            if s.state == StreamState::Publishing {
                if let Some(pub_client) = s.publishing_client {
                    if pub_client != client_id {
                        return Err(StreamError::NotPublishingClient);
                    }
                }
            }
            
            let old_state = s.state;
            if old_state != StreamState::Closed {
                s.state = StreamState::Closed;
                s.publishing_client = None;
                drop(stream);
                
                self.message_tx.send(StreamMessage::StateChanged(StreamEvent::StreamClosed)).ok();
            }
        }
        Ok(())
    }

    pub async fn handle_delete_stream(&self, _stream_id: u32, client_id: u32) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_mut() {
            match s.state {
                StreamState::Publishing | StreamState::Idle => {
                    if s.state == StreamState::Publishing {
                        if let Some(pub_client) = s.publishing_client {
                            if pub_client != client_id {
                                return Err(StreamError::NotPublishingClient);
                            }
                        }
                    }
                    
                    s.state = StreamState::Closed;
                    s.publishing_client = None;
                    drop(stream);
                    
                    self.message_tx.send(StreamMessage::StateChanged(StreamEvent::StreamClosed)).ok();
                }
                StreamState::Closed => {
                    *stream = None;
                    drop(stream);
                    
                    self.message_tx.send(StreamMessage::StateChanged(StreamEvent::StreamDeleted)).ok();
                }
            }
        }
        Ok(())
    }

    pub async fn handle_disconnect(&self, client_id: u32) {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_ref() {
            let is_publishing_client = s.publishing_client
                .map(|c| c == client_id)
                .unwrap_or(false);
            
            if is_publishing_client {
                *stream = None;
                self.message_tx.send(StreamMessage::StateChanged(StreamEvent::StreamDeleted)).ok();
            }
        }
    }
}
