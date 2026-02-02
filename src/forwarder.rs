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

use crate::handshake::handshake_with_server;
use crate::rtmp::write_rtmp_message;
use crate::rtmp_codec::RtmpMessage;
use crate::server::ForwarderConfig;
use anyhow::Result;
use bytes::Bytes;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpStream, tcp};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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
            let backoff_secs = 2u64.pow(self.failure);
            last.elapsed() >= Duration::from_secs(backoff_secs)
        } else {
            true
        }
    }
}

/// Forwarder handles forwarding to a single forwarder server.
/// It runs in its own task to isolate client from destination network issues.
pub struct Forwarder {
    pub chunk_size: usize,
    pub config: ForwarderConfig,
    pub rx: mpsc::Receiver<ForwardEvent>,
    pub snapshot: ProtocolSnapshot,
}

impl Forwarder {
    /// Main loop for the forwarder task.
    pub async fn run(mut self) {
        let mut state = ConnectionState::new();

        info!("Forwarder [{}] started", self.config.addr);

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
                            info!("[{}] Connected successfully", self.config.addr);
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
                        self.graceful_shutdown(w).await;
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
        if let Ok(Ok(mut socket)) =
            tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&addr))
                .await
        {
            socket.set_nodelay(true).ok();

            // Step 0: C0+C1 -> S0+S1+S2 -> C2
            if handshake_with_server(&mut socket).await.is_ok() {
                let (mut r, mut w) = socket.into_split();

                // Spawn a background task to drain anything sent from destination to us
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    while let Ok(n) = r.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                });

                // Perform RTMP command sequence (Connect -> Publish)
                if self.setup_protocol(&mut w).await.is_ok() {
                    return Some(w);
                }
            }
        }
        None
    }

    /// Perform the standard RTMP publishing command sequence.
    async fn setup_protocol(
        &self,
        w: &mut tcp::OwnedWriteHalf,
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
            "c->u [{}]: setup: app=\"{}\" client=\"{}\"",
            self.config.addr,
            app,
            self.snapshot.client_app.as_deref().unwrap_or("")
        );
        crate::rtmp::send_rtmp_command(
            w,
            3,
            0,
            128,
            "connect",
            1.0,
            &[
                ("app", crate::amf::Amf0::String(app.into())),
                (
                    "tcUrl",
                    crate::amf::Amf0::String(format!("rtmp://{}/{}", self.config.addr, app)),
                ),
                ("fmsVer", crate::amf::Amf0::String("FMS/3,0,1,123".into())),
                ("capabilities", crate::amf::Amf0::Number(31.0)),
            ],
            &[],
        )
        .await?;

        // 设置 chunk size
        crate::rtmp::write_rtmp_message2(w, 3, 0, 1, 0 ,
            &Bytes::from((self.chunk_size as u32).to_be_bytes().to_vec()), 128
        ).await?;
        info!("c->u [{}]: set chunk size to {}", self.config.addr, self.chunk_size);

        // 2-3. releaseStream and FCPublish are required by many standard servers (like Nginx-RTMP)
        for (cmd, tx) in [("releaseStream", 2.0), ("FCPublish", 3.0)] {
            crate::rtmp::send_rtmp_command(
                w,
                3,
                0,
                self.chunk_size,
                cmd,
                tx,
                &[],
                &[crate::amf::Amf0::String(stream.into())],
            )
            .await?;
        }

        // 4. Create the logical stream
        crate::rtmp::send_rtmp_command(w, 3, 0, self.chunk_size, "createStream", 4.0, &[], &[]).await?;

        // 5. Start publishing as "live"
        info!(
            "c->u [{}]: publish: stream=\"{}\"",
            self.config.addr, stream
        );
        crate::rtmp::send_rtmp_command(
            w,
            3,
            1,
            self.chunk_size,
            "publish",
            5.0,
            &[],
            &[
                crate::amf::Amf0::String(stream.into()),
                crate::amf::Amf0::String("live".into()),
            ],
        )
        .await?;

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

    async fn graceful_shutdown(&self, w: &mut tcp::OwnedWriteHalf) {
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
            info!(
                "Destination [{}] graceful shutdown: stream={}",
                self.config.addr, stream
            );

            crate::rtmp::send_rtmp_command(
                w,
                3,
                1,
                self.chunk_size,
                "FCUnpublish",
                6.0,
                &[],
                &[crate::amf::Amf0::String(stream.into())],
            )
            .await
            .ok();

            crate::rtmp::send_rtmp_command(
                w,
                3,
                1,
                self.chunk_size,
                "deleteStream",
                7.0,
                &[],
                &[crate::amf::Amf0::Number(1.0)],
            )
            .await
            .ok();
        }
    }
}
