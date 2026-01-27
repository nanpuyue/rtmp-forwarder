use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};
use bytes::Bytes;

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

#[derive(Debug)]
pub struct StreamInfo {
    pub stream_id: u32,
    pub stream_key: String,
    pub app_name: String,
    pub state: StreamState,
    pub publishing_client: Option<String>,
    pub created_at: Instant,
    pub last_active: Instant,
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
    state_change_tx: mpsc::UnboundedSender<StreamEvent>,
}

impl StreamManager {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<StreamEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self {
            default_stream: Arc::new(RwLock::new(None)),
            state_change_tx: tx,
        }, rx)
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

    pub async fn handle_create_stream(&self, app_name: &str, _client_id: &str) -> Result<u32, StreamError> {
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
                    created_at: Instant::now(),
                    last_active: Instant::now(),
                    video_seq_hdr: None,
                    audio_seq_hdr: None,
                });
                
                self.state_change_tx.send(StreamEvent::StreamCreated).ok();
                Ok(1)
            }
            _ => unreachable!(),
        }
    }

    pub async fn handle_publish(&self, _stream_id: u32, _stream_key: &str, client_id: &str) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        let s = stream.as_mut().ok_or(StreamError::StreamNotFound)?;
        
        match s.state {
            StreamState::Publishing => Err(StreamError::AlreadyPublishing),
            StreamState::Idle | StreamState::Closed => {
                s.state = StreamState::Publishing;
                s.publishing_client = Some(client_id.to_string());
                s.last_active = Instant::now();
                s.video_seq_hdr = None;
                s.audio_seq_hdr = None;
                drop(stream);
                
                self.state_change_tx.send(StreamEvent::StreamPublishing).ok();
                Ok(())
            }
        }
    }

    pub async fn handle_unpublish(&self, _stream_id: u32, client_id: &str) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_mut() {
            if s.state == StreamState::Publishing {
                if let Some(ref pub_client) = s.publishing_client {
                    if pub_client != client_id {
                        return Err(StreamError::NotPublishingClient);
                    }
                }
                
                s.state = StreamState::Idle;
                s.publishing_client = None;
                drop(stream);
                
                self.state_change_tx.send(StreamEvent::StreamIdle).ok();
            }
        }
        Ok(())
    }

    pub async fn handle_close_stream(&self, _stream_id: u32, client_id: &str) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_mut() {
            if s.state == StreamState::Publishing {
                if let Some(ref pub_client) = s.publishing_client {
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
                
                self.state_change_tx.send(StreamEvent::StreamClosed).ok();
            }
        }
        Ok(())
    }

    pub async fn handle_delete_stream(&self, _stream_id: u32, client_id: &str) -> Result<(), StreamError> {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_mut() {
            match s.state {
                StreamState::Publishing | StreamState::Idle => {
                    if s.state == StreamState::Publishing {
                        if let Some(ref pub_client) = s.publishing_client {
                            if pub_client != client_id {
                                return Err(StreamError::NotPublishingClient);
                            }
                        }
                    }
                    
                    s.state = StreamState::Closed;
                    s.publishing_client = None;
                    drop(stream);
                    
                    self.state_change_tx.send(StreamEvent::StreamClosed).ok();
                }
                StreamState::Closed => {
                    *stream = None;
                    drop(stream);
                    
                    self.state_change_tx.send(StreamEvent::StreamDeleted).ok();
                }
            }
        }
        Ok(())
    }

    pub async fn handle_disconnect(&self, client_id: &str) {
        let mut stream = self.default_stream.write().await;
        
        if let Some(s) = stream.as_ref() {
            let is_publishing_client = s.publishing_client.as_ref()
                .map(|c| c == client_id)
                .unwrap_or(false);
            
            if is_publishing_client {
                *stream = None;
                self.state_change_tx.send(StreamEvent::StreamDeleted).ok();
            }
        }
    }
}
