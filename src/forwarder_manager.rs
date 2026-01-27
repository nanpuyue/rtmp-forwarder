use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock};
use crate::stream_manager::{StreamManager, StreamMessage, StreamEvent, StreamSnapshot};
use crate::forwarder::{ForwardEvent, TargetActor};
use crate::server::UpstreamConfig;
use crate::rtmp::RtmpMessage;
use tracing::info;

pub enum ForwarderCommand {
    UpdateConfig(Vec<UpstreamConfig>),
    Shutdown,
}

pub struct ForwarderManager {
    stream_manager: Arc<StreamManager>,
    config: Arc<RwLock<Vec<UpstreamConfig>>>,
    command_rx: mpsc::UnboundedReceiver<ForwarderCommand>,
}

impl ForwarderManager {
    pub fn new(
        stream_manager: Arc<StreamManager>,
        initial_config: Vec<UpstreamConfig>,
    ) -> (Self, mpsc::UnboundedSender<ForwarderCommand>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self {
            stream_manager,
            config: Arc::new(RwLock::new(initial_config)),
            command_rx: rx,
        }, tx)
    }
    
    pub async fn run(mut self) {
        let mut msg_rx = self.stream_manager.subscribe();
        let mut forwarders: HashMap<String, mpsc::Sender<ForwardEvent>> = HashMap::new();
        let mut current_snapshot: Option<StreamSnapshot> = None;
        
        info!("ForwarderManager started");
        
        loop {
            tokio::select! {
                Ok(stream_msg) = msg_rx.recv() => {
                    match stream_msg {
                        StreamMessage::RtmpMessage(msg) => {
                            if should_forward(&msg) {
                                for tx in forwarders.values() {
                                    tx.try_send(ForwardEvent::Message(msg.clone())).ok();
                                }
                            }
                        }
                        StreamMessage::StateChanged(event) => {
                            match event {
                                StreamEvent::StreamPublishing => {
                                    if let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
                                        info!("Stream publishing, starting forwarders");
                                        current_snapshot = Some(snapshot.clone());
                                        self.start_forwarders(&mut forwarders, snapshot).await;
                                    }
                                }
                                StreamEvent::StreamIdle | StreamEvent::StreamDeleted => {
                                    info!("Stream stopped, stopping forwarders");
                                    self.stop_forwarders(&mut forwarders).await;
                                    current_snapshot = None;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        ForwarderCommand::UpdateConfig(new_config) => {
                            info!("Updating forwarder config");
                            *self.config.write().await = new_config;
                            if let Some(ref snapshot) = current_snapshot {
                                self.stop_forwarders(&mut forwarders).await;
                                self.start_forwarders(&mut forwarders, snapshot.clone()).await;
                            }
                        }
                        ForwarderCommand::Shutdown => {
                            info!("ForwarderManager shutting down");
                            break;
                        }
                    }
                }
            }
        }
        
        self.stop_forwarders(&mut forwarders).await;
        info!("ForwarderManager stopped");
    }
    
    async fn start_forwarders(
        &self,
        forwarders: &mut HashMap<String, mpsc::Sender<ForwardEvent>>,
        snapshot: StreamSnapshot,
    ) {
        let config = self.config.read().await;
        for upstream in config.iter().filter(|u| u.enabled) {
            let key = format!("{}/{}/{}", 
                upstream.addr, 
                upstream.app.as_deref().unwrap_or(""),
                upstream.stream.as_deref().unwrap_or(""));
            
            let (tx, rx) = mpsc::channel(128);
            
            let actor = TargetActor {
                config: upstream.clone(),
                rx,
                snapshot: crate::forwarder::ProtocolSnapshot {
                    metadata: None,
                    video_seq_hdr: None,
                    audio_seq_hdr: None,
                    client_app: Some(snapshot.app_name.clone()),
                    client_stream: Some(snapshot.stream_key.clone()),
                },
            };
            
            tokio::spawn(actor.run());
            forwarders.insert(key, tx);
        }
        
        info!("Started {} forwarders", forwarders.len());
    }
    
    async fn stop_forwarders(&self, forwarders: &mut HashMap<String, mpsc::Sender<ForwardEvent>>) {
        for tx in forwarders.values() {
            tx.send(ForwardEvent::Shutdown).await.ok();
        }
        info!("Stopped {} forwarders", forwarders.len());
        forwarders.clear();
    }
}

fn should_forward(msg: &RtmpMessage) -> bool {
    matches!(msg.msg_type, 8 | 9 | 18 | 15)
}
