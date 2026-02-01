use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use crate::rtmp_codec::RtmpMessageHeader;
use crate::stream_manager::{StreamManager, StreamMessage, StreamEvent, StreamSnapshot};
use crate::forwarder::{ForwardEvent, Forwarder};
use crate::server::ForwarderConfig;
use crate::rtmp_codec::RtmpMessage;
use tracing::{info, warn};

pub enum ForwarderCommand {
    UpdateConfig(Vec<ForwarderConfig>),
    Shutdown,
}

pub struct ForwarderManager {
    stream_manager: Arc<StreamManager>,
    config: Arc<RwLock<Vec<ForwarderConfig>>>,
    command_rx: mpsc::UnboundedReceiver<ForwarderCommand>,
    running_configs: Arc<RwLock<Vec<ForwarderConfig>>>,
}

impl ForwarderManager {
    pub fn new(
        stream_manager: Arc<StreamManager>,
        initial_config: Vec<ForwarderConfig>,
    ) -> (Self, mpsc::UnboundedSender<ForwarderCommand>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self {
            stream_manager,
            config: Arc::new(RwLock::new(initial_config)),
            command_rx: rx,
            running_configs: Arc::new(RwLock::new(Vec::new())),
        }, tx)
    }
    
    pub async fn run(mut self) {
        let mut msg_rx = self.stream_manager.subscribe();
        let mut forwarders: Vec<Option<mpsc::Sender<ForwardEvent>>> = Vec::new();
        
        info!("ForwarderManager started");
        
        loop {
            tokio::select! {
                Ok(stream_msg) = msg_rx.recv() => {
                    match stream_msg {
                        StreamMessage::RtmpMessage(msg) => {
                            if should_forward(&msg) {
                                for tx in forwarders.iter().flatten() {
                                    tx.try_send(ForwardEvent::Message(msg.clone())).ok();
                                }
                            }
                        }
                        StreamMessage::StateChanged(event) => {
                            match event {
                                StreamEvent::StreamPublishing => {
                                    if let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
                                        info!("Stream publishing");
                                        self.sync_forwarders(&mut forwarders, snapshot).await;
                                    }
                                }
                                StreamEvent::StreamIdle | StreamEvent::StreamClosed | StreamEvent::StreamDeleted => {
                                    info!("Stream stopped");
                                    self.stop_all_forwarders(&mut forwarders).await;
                                }
                                _ => {}
                            }
                        }
                    }
                }
                
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        ForwarderCommand::UpdateConfig(new_config) => {
                            info!("Received config update with {} forwarders", new_config.len());
                            *self.config.write().await = new_config;
                            
                            if let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
                                self.sync_forwarders(&mut forwarders, snapshot).await;
                            } else {
                                self.stop_all_forwarders(&mut forwarders).await;
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
        
        self.stop_all_forwarders(&mut forwarders).await;
        info!("ForwarderManager stopped");
    }
    
    async fn start_forwarder(
        &self,
        index: usize,
        forwarder: &ForwarderConfig,
        snapshot: &StreamSnapshot,
    ) -> mpsc::Sender<ForwardEvent> {
        let (tx, rx) = mpsc::channel(128);
        
        // let actor = Forwarder {
        //     config: forwarder.clone(),
        //     rx,
        //     snapshot: crate::forwarder::ProtocolSnapshot {
        //         metadata: snapshot.metadata.as_ref().map(|b| RtmpMessage {
        //             csid: 3,
        //             header: RtmpMessageHeader {
        //                 timestamp: 0,
        //                 msg_len: b.len(),
        //                 msg_type: 18,
        //                 stream_id: 1,
        //             },
        //             payload: bytes::BytesMut::from(b.as_ref()),
        //         }),
        //         video_seq_hdr: snapshot.video_seq_hdr.as_ref().map(|b| RtmpMessage {
        //             csid: 4,
        //             header: RtmpMessageHeader {
        //                 timestamp: 0,
        //                 msg_len: b.len(),

        //                 msg_type: 9,
        //                 stream_id: 1,
        //             },
        //             payload: bytes::BytesMut::from(b.as_ref()),
        //         }),
        //         audio_seq_hdr: snapshot.audio_seq_hdr.as_ref().map(|b| RtmpMessage {
        //             csid: 5,
        //             header: RtmpMessageHeader {
        //                 timestamp: 0,
        //                 msg_type: 8,
        //                 stream_id: 1,
        //             },
        //             payload: bytes::BytesMut::from(b.as_ref()),
        //         }),
        //         client_app: Some(snapshot.app_name.clone()),
        //         client_stream: Some(snapshot.stream_key.clone()),
        //     },
        // };
        
        // tokio::spawn(actor.run());
        info!("Started forwarder #{}: {}/{}/{}", index, forwarder.addr, 
            forwarder.app.as_deref().unwrap_or(""), forwarder.stream.as_deref().unwrap_or(""));
        tx
    }
    
    async fn stop_forwarder(&self, index: usize, tx: mpsc::Sender<ForwardEvent>) {
        tx.send(ForwardEvent::Shutdown).await.ok();
        info!("Stopped forwarder #{}", index);
    }
    
    async fn sync_forwarders(
        &self,
        forwarders: &mut Vec<Option<mpsc::Sender<ForwardEvent>>>,
        snapshot: StreamSnapshot,
    ) {
        let mut config = self.config.read().await.clone();
        let mut running_configs = self.running_configs.write().await;
        
        // 中继地址为空时使用原始地址
        if let Some(relay) = config.get_mut(0) {
            if relay.addr.is_empty() && relay.enabled {
                if let Some(ref addr) = snapshot.orig_dest_addr {
                    relay.addr = addr.clone();
                    info!("Using original destination for relay: {}", addr);
                } else {
                    warn!("Relay enabled but no address available");
                    relay.enabled = false;
                }
            }
        } else {
            warn!("Relay config not found at index 0");
        }
        
        let mut started = 0;
        let mut stopped = 0;
        let mut restarted = 0;
        
        let max_len = config.len().max(forwarders.len());
        
        for index in 0..max_len {
            let new_config = config.get(index);
            let old_config = running_configs.get(index);
            let forwarder = if index < forwarders.len() { &mut forwarders[index] } else { forwarders.push(None); &mut forwarders[index] };
            
            match (new_config, forwarder.as_ref()) {
                (Some(new), Some(_)) if !new.enabled => {
                    // Disabled, stop
                    let tx = forwarder.take().unwrap();
                    self.stop_forwarder(index, tx).await;
                    stopped += 1;
                }
                (Some(new), None) if new.enabled => {
                    // Enabled and not running, start
                    let tx = self.start_forwarder(index, new, &snapshot).await;
                    *forwarder = Some(tx);
                    started += 1;
                }
                (Some(new), Some(_)) if new.enabled => {
                    // Enabled and running, check if config changed
                    if let Some(old) = old_config {
                        if old.addr != new.addr || old.app != new.app || old.stream != new.stream {
                            // Restart
                            let tx = forwarder.take().unwrap();
                            self.stop_forwarder(index, tx).await;
                            let tx = self.start_forwarder(index, new, &snapshot).await;
                            *forwarder = Some(tx);
                            restarted += 1;
                        }
                    }
                }
                (None, Some(_)) => {
                    // Config removed, stop
                    let tx = forwarder.take().unwrap();
                    self.stop_forwarder(index, tx).await;
                    stopped += 1;
                }
                _ => {
                    // No change
                }
            }
        }
        
        // Truncate to config length
        forwarders.truncate(config.len());
        
        // Update running_configs
        *running_configs = config;
        
        info!("Forwarders synced: {} started, {} stopped, {} restarted", started, stopped, restarted);
    }
    
    async fn stop_all_forwarders(&self, forwarders: &mut Vec<Option<mpsc::Sender<ForwardEvent>>>) {
        for (index, tx) in forwarders.iter_mut().enumerate() {
            if let Some(tx) = tx.take() {
                self.stop_forwarder(index, tx).await;
            }
        }
    }
}

fn should_forward(msg: &RtmpMessage) -> bool {
    matches!(msg.header.msg_type, 8 | 9 | 18 | 15)
}
