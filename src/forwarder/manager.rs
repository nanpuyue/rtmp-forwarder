use std::sync::Arc;

use tokio::sync::{RwLock, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{Forwarder, ForwarderCommand, ForwarderEvent, ForwarderStatus};
use crate::config::ForwarderConfig;
use crate::rtmp::RtmpMessage;
use crate::stream::{StreamEvent, StreamManager, StreamMessage, StreamSnapshot, StreamState};

pub enum ForwarderManagerCommand {
    UpdateConfig(Vec<ForwarderConfig>),
    Shutdown,
}

pub struct ForwarderManager {
    stream_manager: Arc<StreamManager>,
    config: Arc<RwLock<Vec<ForwarderConfig>>>,
    command_rx: mpsc::Receiver<ForwarderManagerCommand>,
    event_tx: broadcast::Sender<ForwarderCommand>,
    event_rx: mpsc::Receiver<ForwarderEvent>,
    manager_tx: mpsc::Sender<ForwarderEvent>,
    running_configs: Vec<ForwarderConfig>,
    forwarder_tokens: Vec<Option<CancellationToken>>,
}

impl ForwarderManager {
    pub fn new(
        stream_manager: Arc<StreamManager>,
        initial_config: Vec<ForwarderConfig>,
    ) -> (Self, mpsc::Sender<ForwarderManagerCommand>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (event_tx, _) = broadcast::channel(128);
        let (manager_tx, event_rx) = mpsc::channel(32);
        (
            Self {
                stream_manager,
                config: Arc::new(RwLock::new(initial_config)),
                command_rx: cmd_rx,
                event_tx,
                event_rx,
                manager_tx,
                running_configs: Vec::new(),
                forwarder_tokens: Vec::new(),
            },
            cmd_tx,
        )
    }

    pub async fn run(mut self) {
        let mut msg_rx = self.stream_manager.subscribe();

        info!("ForwarderManager started");

        loop {
            tokio::select! {
                Ok(stream_msg) = msg_rx.recv() => {
                    match stream_msg {
                        StreamMessage::RtmpMessage(msg) => {
                            if should_forward(&msg) {
                                self.event_tx.send(ForwarderCommand::Message(msg)).ok();
                            }
                        }
                        StreamMessage::StateChanged(event) => {
                            match event {
                                StreamEvent::Publishing => {
                                    if let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
                                        info!("Stream publishing");
                                        self.sync_forwarders(snapshot).await;
                                    }
                                }
                                StreamEvent::Idle | StreamEvent::Closed | StreamEvent::Deleted => {
                                    if !self.running_configs.is_empty() {
                                        info!("Stream stopped");
                                        self.stop_all_forwarders();
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }

                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        ForwarderManagerCommand::UpdateConfig(new_config) => {
                            info!("Received config update with {} forwarders", new_config.len());
                            *self.config.write().await = new_config;

                            if let (_, StreamState::Publishing) = self.stream_manager.default_stream_state().await
                                && let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
                                    self.sync_forwarders(snapshot).await;
                                }
                        }
                        ForwarderManagerCommand::Shutdown => {
                            info!("ForwarderManager shutting down");
                            break;
                        }
                    }
                }

                Some(msg) = self.event_rx.recv() => {
                    match msg {
                        ForwarderEvent::Status(index, status) => {
                            match status {
                                ForwarderStatus::Connected=> info!("Forwarder #{} connected",index),
                                ForwarderStatus::Disconnected=> info!("Forwarder #{} disconnected",index),
                                ForwarderStatus::Stopped => info!("Forwarder #{} stopped",index),
                            }
                        }
                        ForwarderEvent::RequestSnapshot(index) => {
                            // 请求快照，发送当前快照给转发器
                            if let Some(snapshot) = self.stream_manager.get_stream_snapshot().await {
                                if let Err(e) = self.event_tx.send(ForwarderCommand::Snapshot(Box::new(snapshot))) {
                                    warn!("Failed to send snapshot to forwarder #{index}: {e}");
                                } else {
                                    info!("Sent snapshot to forwarder #{index}");
                                }
                            } else {
                                warn!("Stream not publishing, stop forwarder #{index}");
                                self.stop_forwarder(index);
                            }
                        }
                    }
                }
            }
        }

        self.stop_all_forwarders();
        info!("ForwarderManager stopped");
    }

    fn start_forwarder(
        &mut self,
        index: usize,
        config: &ForwarderConfig,
        snapshot: StreamSnapshot,
    ) {
        let chunk_size = snapshot.chunk_size;
        let rx = self.event_tx.subscribe();
        let cancel_token = CancellationToken::new();

        let forwarder = Forwarder {
            index,
            chunk_size,
            stream_id: 1,
            config: config.clone(),
            rx,
            snapshot,
            manager_tx: self.manager_tx.clone(),
            cancel_token: cancel_token.clone(),
        };

        tokio::spawn(forwarder.run());
        self.forwarder_tokens[index] = Some(cancel_token);
        info!("Started forwarder #{}: {}", index, config.addr);
    }

    fn stop_forwarder(&mut self, index: usize) {
        if let Some(token) = self.forwarder_tokens[index].take() {
            token.cancel();
            info!("Stop forwarder #{index}");
        }
    }

    async fn sync_forwarders(&mut self, snapshot: StreamSnapshot) {
        let mut config = self.config.read().await.clone();

        // 中继地址为空时使用原始地址
        if let Some(relay) = config.get_mut(0) {
            if relay.addr.is_empty() && relay.enabled {
                if let Some(ref addr) = snapshot.orig_dest_addr {
                    relay.addr = addr.clone();
                    info!("Using original destination for relay: {addr}");
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

        let max_len = config.len().max(self.running_configs.len());

        // 确保 forwarder_tokens 数组足够大
        if self.forwarder_tokens.len() < max_len {
            self.forwarder_tokens.resize(max_len, None);
        }

        for index in 0..max_len {
            let new_config = config.get(index);
            let old_config = self.running_configs.get(index);

            match (new_config, old_config) {
                (Some(new), Some(old)) if !new.enabled && old.enabled => {
                    // Disabled, stop
                    self.stop_forwarder(index);
                    stopped += 1;
                }
                (Some(new), None) if new.enabled => {
                    // Enabled and not running, start
                    self.start_forwarder(index, new, snapshot.clone());
                    started += 1;
                }
                (Some(new), Some(old)) if new.enabled && !old.enabled => {
                    // Was disabled, now enabled, start
                    self.start_forwarder(index, new, snapshot.clone());
                    started += 1;
                }
                (Some(new), Some(old)) if new.enabled && old.enabled => {
                    // Both enabled, check if config changed
                    if old.addr != new.addr || old.app != new.app || old.stream != new.stream {
                        // Restart with new config
                        self.stop_forwarder(index);
                        self.start_forwarder(index, new, snapshot.clone());
                        restarted += 1;
                    }
                }
                (None, Some(old)) if old.enabled => {
                    // Config removed, stop
                    self.stop_forwarder(index);
                    stopped += 1;
                }
                _ => {
                    // No change
                }
            }
        }

        // Update running_configs
        self.running_configs = config;
        self.forwarder_tokens.truncate(self.running_configs.len());

        info!("Forwarders synced: {started} started, {stopped} stopped, {restarted} restarted");
    }

    fn stop_all_forwarders(&mut self) {
        for (index, token) in self.forwarder_tokens.iter_mut().enumerate() {
            if let Some(token) = token {
                token.cancel();
                debug!("stop forwarder #{index}");
            }
        }
        info!("All forwarders stopped");
        self.running_configs.clear();
        self.forwarder_tokens.clear();
    }
}

fn should_forward(msg: &RtmpMessage) -> bool {
    matches!(msg.header().msg_type, 1 | 8 | 9 | 18 | 15)
}
