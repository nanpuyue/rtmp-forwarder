//! RTMP Forwarding Engine
//!
//! This module implements the "Forwarder" model for RTMP stream fan-out.
//! Each `Forwarder` represents a single destination server and is responsible
//! for its own connection lifecycle, handshaking, and media forwarding.
//!
//! Architecture:
//! 1. **Isolation**: Failures in one destination (timeouts, disconnects) do not affect
//!    the source client or other destinations.
//! 2. **State Syncing**: Using `ProtocolSnapshot`, we capture client metadata
//!    and sequence headers during live streaming to ensure reconnected destinations
//!    can resume immediately.
//! 3. **Protocol Unification**: Handles both original relay (dynamic path) and
//!    explicit upstream destinations through a unified path detection mechanism.

use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use tokio::net::{TcpStream, tcp};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};

use crate::amf::{Amf0, amf_command_name};
use crate::handshake::handshake_with_server;
use crate::rtmp::{send_rtmp_command, write_rtmp_message, write_rtmp_message2};
use crate::rtmp_codec::{RtmpMessage, RtmpMessageStream};
use crate::server::ForwarderConfig;

/// ProtocolSnapshot caches the critical state headers and dynamic names
/// received from the client to sync with new or reconnected destinations.
#[derive(Clone, Debug, Default)]
pub struct ProtocolSnapshot {
    pub metadata: Option<RtmpMessage>,
    pub video_seq_hdr: Option<RtmpMessage>,
    pub audio_seq_hdr: Option<RtmpMessage>,
    pub client_app: Option<String>,
    pub client_stream: Option<String>,
}

pub enum ForwardEvent {
    Message(RtmpMessage),
    Shutdown,
}

/// Retry state for connection attempts
struct ConnectionState {
    conn: Option<tcp::OwnedWriteHalf>,
    failure: u32,
    last_attempt: Option<Instant>,
}

const MAX_FAILURE: u32 = 5;

impl ConnectionState {
    fn new() -> Self {
        Self {
            conn: None,
            failure: 0,
            last_attempt: None,
        }
    }

    fn can_attempt(&self) -> bool {
        if self.failure >= MAX_FAILURE {
            return false;
        }

        if let Some(last) = self.last_attempt {
            let backoff_secs = match self.failure {
                0 => 0,
                1 => 5,
                2 => 10,
                3 => 20,
                4 => 30,
                _ => 30,
            };
            last.elapsed() >= Duration::from_secs(backoff_secs)
        } else {
            true
        }
    }
}

/// Forwarder handles forwarding to a single forwarder server.
/// It runs in its own task to isolate client from destination network issues.
pub struct Forwarder {
    pub index: usize,
    pub chunk_size: usize,
    pub config: ForwarderConfig,
    pub rx: mpsc::Receiver<ForwardEvent>,
    pub snapshot: ProtocolSnapshot,
}

impl Forwarder {
    /// Main loop for the forwarder task.
    pub async fn run(mut self) {
        let mut state = ConnectionState::new();

        info!("#{} [{}] started", self.index, self.config.addr);

        while let Some(event) = self.rx.recv().await {
            match event {
                ForwardEvent::Message(msg) => {
                    // 元数据与序列头：如果转发器启动时客户端尚未发送则后续原样转发
                    // 若客户端已经发送则可从快照中获取
                    // self.snapshot.update_from_message(&msg);

                    // Skip forwarding control commands as we replay our own handshake
                    if msg.header.msg_type == 20 {
                        continue;
                    }

                    // Try to establish connection with retry logic
                    if state.conn.is_none() && state.can_attempt() {
                        state.last_attempt = Some(Instant::now());
                        debug!(
                            "[{}] Connecting (attempt #{})",
                            self.config.addr,
                            state.failure + 1
                        );

                        if let Some(conn) = self.try_connect().await {
                            info!(
                                "#{} [{}] entered forwarding state",
                                self.index, self.config.addr
                            );
                            state.conn = Some(conn);
                            state.failure = 0;
                            state.last_attempt = None;
                        } else {
                            state.failure += 1;
                            if state.failure >= MAX_FAILURE {
                                warn!(
                                    "[{}] Failed {} times, giving up",
                                    self.config.addr, state.failure
                                );
                                break;
                            }
                        }
                    }

                    // Forward media data if connected
                    if let Some(ref mut w) = state.conn {
                        if let Err(e) = write_rtmp_message(w, &msg, self.chunk_size).await {
                            error!("[{}] Write error: {}", self.config.addr, e);
                            state.conn = None;
                        }

                        // Sync output chunk size if destination requests change (via SetChunkSize)
                        if msg.header.msg_type == 1 && msg.header.msg_len >= 4 {
                            self.chunk_size =
                                u32::from_be_bytes(msg.payload()[..4].try_into().unwrap()) as usize;
                        }
                    }
                }
                ForwardEvent::Shutdown => {
                    if let Some(ref mut w) = state.conn {
                        self.shutdown(w).await;
                    }
                    break;
                }
            }
        }
        info!("Forwarder [{}] stopped", self.config.addr);
    }

    /// Attempt to connect and perform RTMP handshake + setup.
    async fn try_connect(&self) -> Option<tcp::OwnedWriteHalf> {
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
                if self.setup_protocol(&mut w, response_rx).await.is_ok() {
                    return Some(w);
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
                        if message.header.msg_type == 1 && message.header.msg_len >= 4 {
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
        &self,
        w: &mut tcp::OwnedWriteHalf,
        mut response_rx: mpsc::Receiver<RtmpMessage>,
    ) -> Result<()> {
        let app = self
            .config
            .app
            .as_deref()
            .or(self.snapshot.client_app.as_deref())
            .unwrap_or_default();
        let stream = self
            .config
            .stream
            .as_deref()
            .or(self.snapshot.client_stream.as_deref())
            .unwrap_or_default();

        // 1. connect
        info!(
            "#{} [{}] connect to app=\"{}\"",
            self.index, self.config.addr, app,
        );
        send_rtmp_command(
            w,
            3,
            0,
            128,
            "connect",
            1.0,
            &[
                ("app", Amf0::String(app.into())),
                (
                    "tcUrl",
                    Amf0::String(format!("rtmp://{}/{}", self.config.addr, app)),
                ),
                ("fmsVer", Amf0::String("FMS/3,0,1,123".into())),
                ("capabilities", Amf0::Number(31.0)),
            ],
            &[],
        )
        .await?;

        // 等待并处理 connect 响应
        if let Ok(Some(response)) = timeout(Duration::from_secs(5), response_rx.recv()).await {
            self.handle_server_response(&response).await?;
        }

        // 设置 chunk size
        write_rtmp_message2(
            w,
            3,
            0,
            1,
            0,
            &Bytes::from((self.chunk_size as u32).to_be_bytes().to_vec()),
            128,
        )
        .await?;
        info!(
            "#{} [{}] set chunk size to {}",
            self.index, self.config.addr, self.chunk_size
        );

        // 2-3. releaseStream and FCPublish are required by many standard servers (like Nginx-RTMP)
        for (cmd, tx) in [("releaseStream", 2.0), ("FCPublish", 3.0)] {
            send_rtmp_command(
                w,
                3,
                0,
                self.chunk_size,
                cmd,
                tx,
                &[],
                &[Amf0::String(stream.into())],
            )
            .await?;

            // 等待并处理响应
            if let Ok(Some(response)) = timeout(Duration::from_secs(5), response_rx.recv()).await {
                self.handle_server_response(&response).await?;
            }
        }

        // 4. Create the logical stream
        send_rtmp_command(w, 3, 0, self.chunk_size, "createStream", 4.0, &[], &[]).await?;

        // 等待并处理 createStream 响应
        if let Ok(Some(response)) = timeout(Duration::from_secs(5), response_rx.recv()).await {
            self.handle_server_response(&response).await?;
        }

        // 5. Start publishing as "live"
        info!(
            "#{} [{}] publish to stream=\"{}\"",
            self.index, self.config.addr, stream
        );
        send_rtmp_command(
            w,
            3,
            1,
            self.chunk_size,
            "publish",
            5.0,
            &[],
            &[Amf0::String(stream.into()), Amf0::String("live".into())],
        )
        .await?;

        // 等待并处理 publish 响应
        if let Ok(Some(response)) = timeout(Duration::from_secs(5), response_rx.recv()).await {
            self.handle_server_response(&response).await?;
        }

        // 6. Sync initial state (MetaData + Sequence Headers) so the stream can be decoded instantly
        if let Some(ref m) = self.snapshot.metadata {
            let mut m = m.clone();
            m.header.timestamp = 0;
            write_rtmp_message(w, &m, self.chunk_size).await.ok();
        }
        if let Some(ref v) = self.snapshot.video_seq_hdr {
            let mut v = v.clone();
            v.header.timestamp = 0;
            write_rtmp_message(w, &v, self.chunk_size).await.ok();
        }
        if let Some(ref a) = self.snapshot.audio_seq_hdr {
            let mut a = a.clone();
            a.header.timestamp = 0;
            write_rtmp_message(w, &a, self.chunk_size).await.ok();
        }
        Ok(())
    }

    /// 处理服务器响应
    async fn handle_server_response(&self, response: &RtmpMessage) -> Result<()> {
        if response.header.msg_type != 20 {
            return Ok(());
        }

        let payload = response.payload();
        if let Ok(cmd) = amf_command_name(&payload) {
            // let mut reader: AmfReader<'_> = AmfReader::new(&payload);
            // let _ = reader.read_string(); // name
            // let _ = reader.read_number(); // tx_id
            // let _ = reader.read_null();

            // TODO
            match cmd.as_str() {
                "_result" => {}
                "_error" => {
                    warn!(
                        "#{} [{}] received _error response",
                        self.index, self.config.addr
                    );
                    return Err(anyhow::anyhow!("Error"));
                }
                "onStatus" => {}
                _ => {
                    debug!(
                        "#{} [{}] unknown response: {}",
                        self.index, self.config.addr, cmd
                    );
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self, w: &mut tcp::OwnedWriteHalf) {
        if let (Some(_app), Some(stream)) = (
            self.config
                .app
                .as_deref()
                .or(self.snapshot.client_app.as_deref()),
            self.config
                .stream
                .as_deref()
                .or(self.snapshot.client_stream.as_deref()),
        ) {
            info!("#{} [{}] shutdown", self.index, self.config.addr);

            send_rtmp_command(
                w,
                3,
                1,
                self.chunk_size,
                "FCUnpublish",
                6.0,
                &[],
                &[Amf0::String(stream.into())],
            )
            .await
            .ok();

            send_rtmp_command(
                w,
                3,
                1,
                self.chunk_size,
                "deleteStream",
                7.0,
                &[],
                &[Amf0::Number(1.0)],
            )
            .await
            .ok();
        }
    }
}
