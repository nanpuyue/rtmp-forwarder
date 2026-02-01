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

use crate::amf::amf_command_name;
use crate::handshake::handshake_with_server;
use crate::rtmp::write_rtmp_message;
use crate::rtmp_codec::RtmpMessage;
use crate::server::ForwarderConfig;
use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// ProtocolSnapshot caches the critical state headers and dynamic names
/// received from the client to sync with new or reconnected destinations.
#[derive(Clone, Default)]
pub struct ProtocolSnapshot {
    pub metadata: Option<RtmpMessage>,
    pub video_seq_hdr: Option<RtmpMessage>,
    pub audio_seq_hdr: Option<RtmpMessage>,
    pub client_app: Option<String>,
    pub client_stream: Option<String>,
}

impl ProtocolSnapshot {
    /// Inspect incoming messages and update the cached protocol state.
    /// This allows the forwarder to know the latest app/stream path at any time.
    pub fn update_from_message(&mut self, msg: &RtmpMessage) {
        let payload_prefix = msg.first_chunk_payload();

        match msg.header.msg_type {
            // MetaData (onMetaData)
            18 | 15 => self.metadata = Some(msg.clone()),
            // Video sequence header (AVC/H264)
            9 if payload_prefix.len() >= 2 && payload_prefix[0] == 0x17 && payload_prefix[1] == 0 => {
                self.video_seq_hdr = Some(msg.clone());
            }
            // Audio sequence header (AAC)
            8 if payload_prefix.len() >= 2 && (payload_prefix[0] >> 4) == 10 && payload_prefix[1] == 0 => {
                self.audio_seq_hdr = Some(msg.clone());
            }
            // Control commands (peek for app/stream names)
            20 => {
                if let Ok(cmd) = amf_command_name(&payload_prefix) {
                    let mut r = crate::amf::AmfReader::new(&payload_prefix);
                    let _ = r.read_string(); // Skip command name
                    match cmd.as_str() {
                        "connect" => {
                            if let Ok(obj) = r.read_number().and_then(|_| r.read_object())
                                && let Some((_, crate::amf::Amf0::String(s))) = obj.iter().find(|(k, _)| k == "app") {
                                    self.client_app = Some(s.clone());
                                }
                        }
                        "publish" | "releaseStream" | "FCPublish" => {
                            let _ = r.read_number().and_then(|_| r.read_null());
                            if let Ok(s) = r.read_string() { self.client_stream = Some(s); }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

pub enum ForwardEvent {
    Message(RtmpMessage),
    Shutdown,
}

/// Retry state for connection attempts
struct ConnectionState {
    conn: Option<tokio::net::tcp::OwnedWriteHalf>,
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
    pub config: ForwarderConfig,
    pub rx: mpsc::Receiver<ForwardEvent>,
    pub snapshot: ProtocolSnapshot,
}

impl Forwarder {
    /// Main loop for the forwarder task.
    pub async fn run(mut self) {
        let mut state = ConnectionState::new();
        let mut chunk_size = 128usize;

        info!("Forwarder [{}] started", self.config.addr);

        while let Some(event) = self.rx.recv().await {
            match event {
                ForwardEvent::Message(msg) => {
                    // Update state even if we haven't connected yet
                    self.snapshot.update_from_message(&msg);

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
                        if let Err(e) = write_rtmp_message(w, &msg, chunk_size).await {
                            error!("[{}] Write error: {}", self.config.addr, e);
                            state.conn = None;
                        } else if msg.header.msg_type == 1 && msg.header.msg_len >= 4 {
                            // Sync output chunk size if destination requests change (via SetChunkSize)
                            chunk_size =
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
    async fn try_connect(&self) -> Option<tokio::net::tcp::OwnedWriteHalf> {
        // Decide which app/stream names to use (priority: config override > client original)
        let app = self
            .config
            .app
            .as_deref()
            .or(self.snapshot.client_app.as_deref());
        let stream = self
            .config
            .stream
            .as_deref()
            .or(self.snapshot.client_stream.as_deref());

        if let (Some(a), Some(s)) = (app, stream) {
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
                    if self.setup_protocol(&mut w, a, s).await.is_ok() {
                        return Some(w);
                    }
                }
            }
        }
        None
    }

    /// Perform the standard RTMP publishing command sequence.
    async fn setup_protocol(
        &self,
        w: &mut tokio::net::tcp::OwnedWriteHalf,
        app: &str,
        stream: &str,
    ) -> Result<()> {
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

        // 2-3. releaseStream and FCPublish are required by many standard servers (like Nginx-RTMP)
        for (cmd, tx) in [("releaseStream", 2.0), ("FCPublish", 3.0)] {
            crate::rtmp::send_rtmp_command(
                w,
                3,
                0,
                128,
                cmd,
                tx,
                &[],
                &[crate::amf::Amf0::String(stream.into())],
            )
            .await?;
        }

        // 4. Create the logical stream
        crate::rtmp::send_rtmp_command(w, 3, 0, 128, "createStream", 4.0, &[], &[]).await?;

        // 5. Start publishing as "live"
        info!(
            "c->u [{}]: publish: stream=\"{}\"",
            self.config.addr, stream
        );
        crate::rtmp::send_rtmp_command(
            w,
            3,
            1,
            128,
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
            write_rtmp_message(w, &m, 128).await.ok();
        }
        if let Some(ref v) = self.snapshot.video_seq_hdr {
            let mut v = v.clone();
            v.header.timestamp = 0;
            write_rtmp_message(w, &v, 128).await.ok();
        }
        if let Some(ref a) = self.snapshot.audio_seq_hdr {
            let mut a = a.clone();
            a.header.timestamp = 0;
            write_rtmp_message(w, &a, 128).await.ok();
        }
        Ok(())
    }

    async fn graceful_shutdown(&self, w: &mut tokio::net::tcp::OwnedWriteHalf) {
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
                128,
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
                128,
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
