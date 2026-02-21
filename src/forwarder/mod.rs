use std::pin::pin;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use futures_util::sink::SinkExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, tcp};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, FramedWrite};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::ForwarderConfig;
use crate::error::Result;
use crate::rtmp::handshake_with_server;
use crate::rtmp::{RtmpCodec, RtmpCommand, RtmpMessage, RtmpMessageStream};
use crate::stream::StreamSnapshot;

pub use self::manager::{ForwarderManager, ForwarderManagerCommand};

mod manager;

const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub enum ForwarderCommand {
    Message(RtmpMessage),
    Snapshot(Box<StreamSnapshot>),
}

#[derive(Debug)]
pub enum ForwarderStatus {
    Connected,
    Disconnected,
    Stopped,
}

#[derive(Debug)]
pub enum ForwarderEvent {
    Status(usize, ForwarderStatus),
    RequestSnapshot(usize),
}

/// Retry state for connection attempts
struct ConnectionState {
    failure: u32,
    last_failed: Instant,
}

const MAX_FAILURE: u32 = 5;

impl ConnectionState {
    fn new() -> Self {
        Self {
            failure: 0,
            last_failed: Instant::now(),
        }
    }

    fn can_attempt(&mut self) -> bool {
        if self.last_failed.elapsed() >= Duration::from_mins(5) {
            self.failure = 0;
        }
        if self.failure >= MAX_FAILURE {
            return false;
        }

        let backoff_secs = match self.failure {
            0 => 0,
            1 => 5,
            2 => 10,
            3 => 20,
            4 => 30,
            _ => 30,
        };
        self.last_failed.elapsed() >= Duration::from_secs(backoff_secs)
    }
}

/// Forwarder handles forwarding to a single forwarder server.
/// It runs in its own task to isolate client from destination network issues.
pub struct Forwarder {
    pub index: usize,
    pub chunk_size: usize,
    pub stream_id: u32,
    pub config: ForwarderConfig,
    pub rx: broadcast::Receiver<ForwarderCommand>,
    pub snapshot: StreamSnapshot,
    pub manager_tx: mpsc::Sender<ForwarderEvent>,
    pub cancel_token: CancellationToken,
}

impl Forwarder {
    fn app(&self) -> String {
        self.config
            .app
            .clone()
            .or(self.snapshot.app_name.clone())
            .unwrap_or_default()
    }

    fn stream(&self) -> String {
        self.config
            .stream
            .clone()
            .or(self.snapshot.stream_key.clone())
            .unwrap_or_default()
    }

    async fn send_status(&self, status: ForwarderStatus) -> Result<()> {
        self.manager_tx
            .send(ForwarderEvent::Status(self.index, status))
            .await?;
        Ok(())
    }

    fn failure_reached(&self, s: &mut ConnectionState) -> bool {
        s.last_failed = Instant::now();
        s.failure += 1;
        let reached = s.failure >= MAX_FAILURE;
        if reached {
            warn!(
                "#{} [{}] Failed {} times, giving up",
                self.index, self.config.addr, s.failure
            )
        }
        reached
    }

    /// Main loop for the forwarder task.
    pub async fn run(mut self) {
        info!("#{} [{}] started", self.index, self.config.addr);
        let mut state = ConnectionState::new();
        let mut framed = None;
        let mut timeout = pin!(sleep(CONNECT_TIMEOUT));
        let cancel_token = self.cancel_token.clone();

        loop {
            if framed.is_none() {
                timeout
                    .as_mut()
                    .reset((Instant::now() + CONNECT_TIMEOUT).into());
            } else {
                timeout
                    .as_mut()
                    .reset((Instant::now() + SEND_TIMEOUT).into());
            }

            tokio::select! {
                // 检查是否取消
                _ = cancel_token.cancelled() => {
                    break;
                }
                // 发送超时处理
                _ = &mut timeout => {
                    if self.handle_timeout(&mut framed, &mut state).await {
                        break;
                    }
                }
                // 处理 rtmp 消息
                b = self.handle_rx(&mut framed, &mut state) => {
                    if b {
                        break;
                    }
                }
            }
        }

        if let Some(mut f) = framed {
            self.shutdown(&mut f).await;
        }
        let _ = self.send_status(ForwarderStatus::Stopped).await;
        info!("Forwarder [{}] stopped", self.config.addr);
    }

    // 返回 true 表示退出循环
    async fn handle_rx(
        &mut self,
        framed: &mut Option<FramedWrite<tcp::OwnedWriteHalf, RtmpCodec>>,
        state: &mut ConnectionState,
    ) -> bool {
        let mut msg = match self.rx.recv().await {
            Ok(ForwarderCommand::Message(msg)) => msg,
            Err(RecvError::Lagged(n)) => {
                warn!(
                    "#{} [{}] receiver lagged, dropped {} RTMP messages",
                    self.index, self.config.addr, n
                );
                return false;
            }
            Err(_) => return true,
            _ => return false,
        };

        // 若为 SetChunkSize 消息，当前消息转发后更新 chunk_size
        let set_chunk_size = msg.header().msg_type == 1 && msg.header().msg_len >= 4;
        if set_chunk_size {
            self.chunk_size = u32::from_be_bytes(msg.payload()[..4].try_into().unwrap()) as usize;
        };

        // 若未连接，尝试连接
        if framed.is_none() && state.can_attempt() {
            debug!(
                "[{}] Connecting (attempt #{})",
                self.config.addr,
                state.failure + 1
            );
            if let Some((w, codec)) = self.try_connect().await {
                info!(
                    "#{} [{}] entered forwarding state",
                    self.index, self.config.addr
                );
                // 创建 FramedWrite
                *framed = Some(FramedWrite::new(w, codec));

                if self.send_status(ForwarderStatus::Connected).await.is_err() {
                    return true;
                }
            } else {
                if self.failure_reached(state) {
                    return true;
                }
                return false;
            }
        }

        // 已连接则发送
        if let Some(f) = framed.as_mut() {
            msg.set_stream_id(self.stream_id);

            if let Err(e) = f.send(msg).await {
                warn!(
                    "#{} [{}] send error: {}, dropping connection",
                    self.index, self.config.addr, e
                );

                *framed = None;

                if self
                    .send_status(ForwarderStatus::Disconnected)
                    .await
                    .is_err()
                    || self.failure_reached(state)
                {
                    return true;
                }

                return false;
            } else if set_chunk_size {
                // 更新 chunk_size
                f.encoder_mut().set_chunk_size(self.chunk_size);
            }
        }

        false
    }

    // 返回 true 表示退出循环
    async fn handle_timeout(
        &mut self,
        framed: &mut Option<FramedWrite<tcp::OwnedWriteHalf, RtmpCodec>>,
        state: &mut ConnectionState,
    ) -> bool {
        warn!("#{} [{}] forward timeout", self.index, self.config.addr);

        if framed.is_some() {
            *framed = None;

            if self
                .send_status(ForwarderStatus::Disconnected)
                .await
                .is_err()
            {
                return true;
            }
        }

        self.failure_reached(state)
    }

    /// Attempt to connect and perform RTMP handshake + setup.
    async fn try_connect(&mut self) -> Option<(tcp::OwnedWriteHalf, RtmpCodec)> {
        // 请求快照
        if let Err(e) = self
            .manager_tx
            .send(ForwarderEvent::RequestSnapshot(self.index))
            .await
        {
            error!(
                "#{} [{}] Failed to send snapshot request: {}",
                self.index, self.config.addr, e
            );
            return None;
        } else {
            let now = Instant::now();
            while now.elapsed() < Duration::from_secs(1) {
                if self.cancel_token.is_cancelled() {
                    return None;
                }
                if let Ok(ForwarderCommand::Snapshot(snapshot)) = self.rx.recv().await {
                    self.snapshot = *snapshot;
                    debug!("#{} [{}] snapshot updated", self.index, self.config.addr);
                    break;
                }
            }
        }

        let mut addr = self.config.addr.clone();
        if !addr.contains(':') {
            addr.push_str(":1935");
        }

        // Connect to forwarder destination
        if let Ok(Ok(mut socket)) = timeout(Duration::from_secs(3), TcpStream::connect(&addr)).await
        {
            socket.set_nodelay(true).ok();

            // Step 0: C0+C1 -> S0+S1+S2 -> C2
            if handshake_with_server(&mut socket).await.is_ok() {
                let (r, mut w) = socket.into_split();

                // Spawn a background task to read server responses
                let response_rx = Self::spawn_response_reader(r);

                // Perform RTMP command sequence (Connect -> Publish)
                if let Ok(codec) = self.setup_protocol(&mut w, response_rx).await {
                    return Some((w, codec));
                }
            }
        }
        None
    }

    /// Spawn a background task to read and process server responses
    fn spawn_response_reader(r: tcp::OwnedReadHalf) -> mpsc::Receiver<RtmpMessage> {
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut msg_stream = RtmpMessageStream::new(r, 128);

            while let Some(msg) = msg_stream.next().await {
                match msg {
                    Ok(message) => {
                        // 检查是否是 SetChunkSize 消息 (msg_type = 1)
                        if message.header().msg_type == 1 && message.header().msg_len >= 4 {
                            let chunk_size =
                                u32::from_be_bytes(message.payload()[..4].try_into().unwrap())
                                    as usize;
                            msg_stream.set_chunk_size(chunk_size);
                            debug!("Server set chunk size to {}", chunk_size);
                        }

                        if let Err(TrySendError::Closed(_)) = tx.try_send(message) {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Error reading server response: {}", e);
                        break;
                    }
                }
            }
        });

        rx
    }

    /// Perform the standard RTMP publishing command sequence.
    async fn setup_protocol(
        &mut self,
        w: &mut tcp::OwnedWriteHalf,
        mut response_rx: mpsc::Receiver<RtmpMessage>,
    ) -> Result<RtmpCodec> {
        let app = self.app();
        let stream = self.stream();

        // 设置 chunk size
        self.chunk_size = self.snapshot.chunk_size;
        RtmpMessage::set_chunk_size(self.chunk_size as u32)
            .write_to(w)
            .await?;
        info!(
            "#{} [{}] set chunk size to {}",
            self.index, self.config.addr, self.chunk_size
        );

        // 1. connect
        info!(
            "#{} [{}] connect to {}",
            self.index,
            self.config.addr,
            self.config.rtmp_url(),
        );

        let tc_url = if self.index == 0
            && let Some(tc_url) = self.snapshot.tc_url.clone()
        {
            tc_url
        } else {
            format!("rtmp://{}/{}", self.config.addr, app)
        };
        RtmpCommand::connect(1.0, app, tc_url)
            .send(w, 3, 0, self.chunk_size)
            .await?;

        // 等待并处理 connect 响应
        timeout(
            Duration::from_secs(5),
            self.handle_server_response(&mut response_rx, 1.0),
        )
        .await??;

        // 2-3. releaseStream and FCPublish are required by many standard servers (like Nginx-RTMP)
        for (cmd, tx) in [("releaseStream", 2.0), ("FCPublish", 3.0)] {
            match cmd {
                "releaseStream" => {
                    RtmpCommand::release_stream(tx, &stream)
                        .send(w, 3, 0, self.chunk_size)
                        .await?;
                }
                "FCPublish" => {
                    RtmpCommand::fc_publish(tx, &stream)
                        .send(w, 3, 0, self.chunk_size)
                        .await?;
                }
                _ => {}
            }
        }

        // 4. Create the logical stream
        RtmpCommand::create_stream(4.0)
            .send(w, 3, 0, self.chunk_size)
            .await?;

        // 等待并处理 createStream 响应
        timeout(
            Duration::from_secs(5),
            self.handle_server_response(&mut response_rx, 4.0),
        )
        .await??;

        // 5. Start publishing as "live"
        info!(
            "#{} [{}] publish to stream=\"{}\"",
            self.index, self.config.addr, &stream
        );
        RtmpCommand::publish(5.0, &stream)
            .send(w, 3, self.stream_id, self.chunk_size)
            .await?;

        // TODO, 等待并处理 publish 响应
        // self.handle_server_response(&mut response_rx, 5.0).await?;

        // 6. Sync initial state (MetaData + Sequence Headers) so the stream can be decoded instantly
        let mut codec = RtmpCodec::new(self.chunk_size);
        for m in [
            &self.snapshot.metadata,
            &self.snapshot.video_seq_hdr,
            &self.snapshot.audio_seq_hdr,
        ]
        .into_iter()
        .flatten()
        {
            let m = &mut m.clone();
            m.set_stream_id(self.stream_id);

            let mut buf = BytesMut::new();
            codec.encode(m.clone(), &mut buf)?;
            w.write_all(&buf).await?;
        }
        Ok(codec)
    }

    /// 处理服务器响应
    async fn handle_server_response(
        &mut self,
        response_rx: &mut Receiver<RtmpMessage>,
        expect_tx_id: f64,
    ) -> Result<()> {
        while let Some(response) = timeout(Duration::from_secs(5), response_rx.recv()).await? {
            if let Ok(cmd) = RtmpMessage::command(&response) {
                let tx_id = cmd.transaction_id;

                match cmd.name.as_str() {
                    "_result" => {
                        // 处理 _result 响应
                        match tx_id {
                            1.0 => {
                                // connect 响应
                                debug!("#{} [{}] connect success", self.index, self.config.addr);
                            }
                            2.0 => {
                                // releaseStream 响应
                                debug!(
                                    "#{} [{}] releaseStream success",
                                    self.index, self.config.addr
                                );
                            }
                            3.0 => {
                                // FCPublish 响应
                                debug!("#{} [{}] FCPublish success", self.index, self.config.addr);
                            }
                            4.0 => {
                                // createStream 响应，获取 stream_id
                                if let Some(stream_id) = cmd.args.first().and_then(|v| v.num()) {
                                    self.stream_id = stream_id as u32;
                                    info!(
                                        "#{} [{}] createStream success, stream_id: {}",
                                        self.index, self.config.addr, self.stream_id
                                    );
                                } else {
                                    warn!(
                                        "#{} [{}] createStream success but no stream_id",
                                        self.index, self.config.addr
                                    );
                                }
                            }
                            _ => {
                                debug!(
                                    "#{} [{}] unknown _result tx_id: {}",
                                    self.index, self.config.addr, tx_id
                                );
                            }
                        }
                    }
                    "_error" => {
                        warn!(
                            "#{} [{}] received _error response tx_id: {}",
                            self.index, self.config.addr, tx_id
                        );
                        return Err(format!("server returned an error for tx_id {tx_id}").into());
                    }
                    "onStatus" => {
                        // TODO, 处理 onStatus 响应
                        debug!("#{} [{}] onStatus", self.index, self.config.addr);
                    }
                    _ => {
                        debug!(
                            "#{} [{}] unknown response: {}",
                            self.index, self.config.addr, cmd.name
                        );
                    }
                }

                if tx_id == expect_tx_id {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self, framed: &mut FramedWrite<tcp::OwnedWriteHalf, RtmpCodec>) {
        RtmpCommand::fc_unpublish(6.0, self.stream())
            .send(framed.get_mut(), 3, self.stream_id, self.chunk_size)
            .await
            .ok();

        RtmpCommand::delete_stream(7.0, self.stream_id as f64)
            .send(framed.get_mut(), 3, self.stream_id, self.chunk_size)
            .await
            .ok();
        info!("#{} [{}] shutdown", self.index, self.config.addr);
    }
}
