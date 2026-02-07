use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{RwLock, broadcast};

use crate::rtmp::RtmpMessage;

pub use self::flv::FlvManager;

mod flv;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    None,
    Idle,
    Publishing,
    Closed,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Created,
    Publishing,
    Idle,
    Closed,
    Deleted,
}

#[derive(Debug, Clone)]
pub enum StreamMessage {
    RtmpMessage(RtmpMessage),
    StateChanged(StreamEvent),
}

#[derive(Clone, Debug)]
pub struct StreamSnapshot {
    pub chunk_size: usize,
    pub app_name: Option<String>,
    pub stream_key: Option<String>,
    pub metadata: Option<RtmpMessage>,
    pub video_seq_hdr: Option<RtmpMessage>,
    pub audio_seq_hdr: Option<RtmpMessage>,
    pub orig_dest_addr: Option<String>,
}

#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub stream_id: u32,
    pub client_id: u32,
    pub app_name: Option<String>,
    pub stream_key: Option<String>,
    pub state: StreamState,
    pub last_active: Instant,
    pub chunk_szie: usize,
    pub metadata: Option<RtmpMessage>,
    pub video_seq_hdr: Option<RtmpMessage>,
    pub audio_seq_hdr: Option<RtmpMessage>,
    pub orig_dest_addr: Option<String>,
}

#[derive(Debug)]
pub enum StreamError {
    AlreadyPublishing,
    StreamNotFound,
    NotPublishingClient,
}

pub struct StreamManager {
    pub default_stream: Arc<RwLock<Option<StreamInfo>>>,
    pub message_tx: broadcast::Sender<StreamMessage>,
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

        if let Some(s) = stream.as_mut()
            && s.state == StreamState::Publishing
        {
            let payload = msg.first_chunk_payload();
            match msg.header().msg_type {
                18 | 15 => {
                    s.metadata = Some(msg.clone());
                }
                9 if payload.len() >= 2 && payload[0] == 0x17 && payload[1] == 0 => {
                    s.video_seq_hdr = Some(msg.clone());
                }
                8 if payload.len() >= 2 && (payload[0] >> 4) == 10 && payload[1] == 0 => {
                    s.audio_seq_hdr = Some(msg.clone());
                }
                _ => {}
            }
            s.last_active = Instant::now();
            drop(stream);

            self.message_tx.send(StreamMessage::RtmpMessage(msg)).ok();
        }
    }

    pub async fn get_stream_snapshot(&self) -> Option<StreamSnapshot> {
        let stream = self.default_stream.read().await;
        stream
            .as_ref()
            .filter(|s| s.state == StreamState::Publishing)
            .map(|s| StreamSnapshot {
                chunk_size: s.chunk_szie,
                app_name: s.app_name.clone(),
                stream_key: s.stream_key.clone(),
                metadata: s.metadata.clone(),
                video_seq_hdr: s.video_seq_hdr.clone(),
                audio_seq_hdr: s.audio_seq_hdr.clone(),
                orig_dest_addr: s.orig_dest_addr.clone(),
            })
    }

    async fn default_stream_state(&self) -> (u32, StreamState) {
        let stream = self.default_stream.read().await;
        let client_id = stream.as_ref().map_or(0, |s| s.client_id);
        let state = stream.as_ref().map_or(StreamState::None, |s| s.state);
        (client_id, state)
    }

    pub async fn handle_set_chunk_size(&self, client_id: u32, size: usize) {
        if self.default_stream_state().await.0 == client_id {
            let mut stream = self.default_stream.write().await;
            if let Some(s) = stream.as_mut() {
                s.chunk_szie = size;
            }
        };
    }

    pub async fn handle_connect(&self, _app: &Option<String>) -> Result<(), StreamError> {
        if let (_, StreamState::Publishing) = self.default_stream_state().await {
            return Err(StreamError::AlreadyPublishing);
        }
        Ok(())
    }

    pub async fn handle_create_stream(&self) -> Result<(), StreamError> {
        if let (_, StreamState::Publishing) = self.default_stream_state().await {
            return Err(StreamError::AlreadyPublishing);
        }
        self.message_tx
            .send(StreamMessage::StateChanged(StreamEvent::Created))
            .ok();
        Ok(())
    }

    pub async fn handle_publish(
        &self,
        stream_id: u32,
        mut stream: StreamInfo,
    ) -> Result<(), StreamError> {
        if stream_id != stream.stream_id {
            return Err(StreamError::StreamNotFound);
        }

        match self.default_stream_state().await.1 {
            StreamState::Publishing => Err(StreamError::AlreadyPublishing),
            StreamState::None | StreamState::Idle | StreamState::Closed => {
                stream.state = StreamState::Publishing;
                let mut default_stream = self.default_stream.write().await;
                drop(default_stream.replace(stream));
                self.message_tx
                    .send(StreamMessage::StateChanged(StreamEvent::Publishing))
                    .ok();
                Ok(())
            }
        }
    }

    pub async fn handle_unpublish(
        &self,
        _stream_id: u32,
        client_id: u32,
    ) -> Result<(), StreamError> {
        let state = self.default_stream_state().await;
        if state.0 != 0 && state.0 != client_id {
            return Err(StreamError::NotPublishingClient);
        }

        if state.1 == StreamState::Publishing {
            let mut stream = self.default_stream.write().await;
            if let Some(s) = stream.as_mut() {
                s.state = StreamState::Idle;
            }
            drop(stream);
            self.message_tx
                .send(StreamMessage::StateChanged(StreamEvent::Idle))
                .ok();
        }
        Ok(())
    }

    pub async fn handle_close_stream(
        &self,
        _stream_id: u32,
        client_id: u32,
    ) -> Result<(), StreamError> {
        let state = self.default_stream_state().await;
        if state.0 != 0 && state.0 != client_id {
            return Err(StreamError::NotPublishingClient);
        }

        if matches!(state.1, StreamState::Publishing | StreamState::Idle) {
            let mut stream = self.default_stream.write().await;
            if let Some(s) = stream.as_mut() {
                s.state = StreamState::Closed;
            }
            drop(stream);
            self.message_tx
                .send(StreamMessage::StateChanged(StreamEvent::Closed))
                .ok();
        }
        Ok(())
    }

    pub async fn handle_delete_stream(
        &self,
        _stream_id: u32,
        client_id: u32,
    ) -> Result<(), StreamError> {
        let state = self.default_stream_state().await;
        if state.0 != 0 && state.0 != client_id {
            return Err(StreamError::NotPublishingClient);
        }

        let mut stream = self.default_stream.write().await;
        if stream.as_mut().is_some() {
            drop(stream.take())
        }
        drop(stream);
        self.message_tx
            .send(StreamMessage::StateChanged(StreamEvent::Deleted))
            .ok();
        Ok(())
    }

    pub async fn handle_disconnect(&self, client_id: u32) {
        let state = self.default_stream_state().await;
        if state.0 != 0 && state.0 != client_id {
            return;
        }

        let mut stream = self.default_stream.write().await;
        if stream.as_mut().is_some() {
            drop(stream.take())
        }
        drop(stream);
        if state.1 != StreamState::None {
            self.message_tx
                .send(StreamMessage::StateChanged(StreamEvent::Closed))
                .ok();
        }
    }
}
