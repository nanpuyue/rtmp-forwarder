use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::amf::RtmpCommand;
use crate::handshake::handshake_with_client;
use crate::rtmp::write_rtmp_message2;
use crate::rtmp_codec::{RtmpMessage, RtmpMessageStream};
use crate::stream_manager::{StreamError, StreamInfo, StreamManager, StreamState};

static CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::new(1);
const DEFAULT_STREAM_ID: u32 = 1;

pub async fn handle_client(
    mut client: TcpStream,
    _shared_config: crate::config::SharedConfig,
    stream_manager: Arc<StreamManager>,
) -> Result<()> {
    client.set_nodelay(true)?;

    // 获取原始目的地址
    let orig_dest_addr = get_original_destination(&client).ok();
    if let Some(addr) = &orig_dest_addr {
        info!("Captured original destination address: {addr}",);
    }

    handshake_with_client(&mut client).await?;

    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut s2c_chunk = 128usize;

    let mut stream = Some(StreamInfo {
        stream_id: DEFAULT_STREAM_ID,
        client_id,
        app_name: None,
        stream_key: None,
        state: StreamState::None,
        last_active: Instant::now(),
        chunk_szie: 128,
        metadata: None,
        video_seq_hdr: None,
        audio_seq_hdr: None,
        orig_dest_addr,
    });

    let (client_rx, mut client_tx) = client.into_split();
    let mut msg_stream = RtmpMessageStream::new(client_rx, stream.as_ref().unwrap().chunk_szie);
    while let Some(msg) = msg_stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                info!("Client disconnected: {}", e);
                break;
            }
        };

        match msg.header().msg_type {
            1 => {
                if msg.header().msg_len >= 4 {
                    let payload = msg.payload();
                    let c2s_chunk = u32::from_be_bytes(payload[..4].try_into().unwrap()) as usize;
                    msg_stream.set_chunk_size(c2s_chunk);
                    if let Some(stream) = stream.as_mut() {
                        stream.chunk_szie = c2s_chunk;
                    }
                    stream_manager
                        .handle_set_chunk_size(client_id, c2s_chunk)
                        .await;
                    info!("Client {client_id} set chunk size to {c2s_chunk}");
                }
            }
            20 => {
                let stream_id = msg.header().stream_id;
                if let Ok(cmd) = RtmpMessage::command(&msg) {
                    let tx_num = cmd.transaction_id;

                    // 提取app和stream信息
                    let mut command_app = None;
                    let mut command_stream = None;
                    match cmd.name.as_str() {
                        "connect" => {
                            command_app = cmd
                                .command_object
                                .into_iter()
                                .find(|(k, _)| k == "app")
                                .and_then(|(_, v)| v.to_string());
                        }
                        "publish" | "releaseStream" | "FCPublish" => {
                            command_stream =
                                cmd.args.into_iter().next().and_then(|v| v.to_string());
                        }
                        _ => {}
                    }

                    match cmd.name.as_str() {
                        "connect" if let Some(stream) = stream.as_mut() => {
                            stream.app_name = command_app;
                            if let Err(StreamError::AlreadyPublishing) =
                                stream_manager.handle_connect(&stream.app_name).await
                            {
                                warn!("Connection rejected: already publishing");
                                RtmpCommand::new("_error", tx_num)
                                    .arg(vec![
                                        ("level", "error"),
                                        ("code", "NetConnection.Connect.Rejected"),
                                        ("description", "Already publishing"),
                                    ])
                                    .send(&mut client_tx, 3, 0, s2c_chunk)
                                    .await
                                    .ok();
                                break;
                            }
                            info!(
                                "Client {client_id} connect to app \"{}\"",
                                stream.app_name.as_deref().unwrap_or_default()
                            );

                            // Window Ack Size
                            write_rtmp_message2(
                                &mut client_tx,
                                2,
                                0,
                                5,
                                0,
                                &Bytes::from(2500000u32.to_be_bytes().to_vec()),
                                s2c_chunk,
                            )
                            .await
                            .ok();
                            // Peer Bandwidth
                            write_rtmp_message2(
                                &mut client_tx,
                                2,
                                0,
                                6,
                                0,
                                &Bytes::from(&[0x26, 0x25, 0xa0, 0x00, 0x02][..]),
                                s2c_chunk,
                            )
                            .await
                            .ok();
                            // Set Chunk Size
                            write_rtmp_message2(
                                &mut client_tx,
                                2,
                                0,
                                1,
                                0,
                                &Bytes::from(4096u32.to_be_bytes().to_vec()),
                                s2c_chunk,
                            )
                            .await
                            .ok();
                            s2c_chunk = 4096;

                            // _result(connect)
                            RtmpCommand::new("_result", tx_num)
                                .object("fmsVer", "FMS/3,0,1,123")
                                .object("capabilities", 31.0)
                                .arg(vec![
                                    ("level", "status"),
                                    ("code", "NetConnection.Connect.Success"),
                                    ("description", "Connection succeeded."),
                                ])
                                .send(&mut client_tx, 3, 0, s2c_chunk)
                                .await
                                .ok();
                        }
                        "createStream" if let Some(stream) = stream.as_mut() => {
                            match stream_manager.handle_create_stream().await {
                                Ok(_) => {
                                    stream.state = StreamState::Idle;
                                    RtmpCommand::new("_result", tx_num)
                                        .arg(stream.stream_id as f64)
                                        .send(&mut client_tx, 3, 0, s2c_chunk)
                                        .await
                                        .ok();
                                    info!("Created stream for client {}", client_id);
                                }
                                Err(StreamError::AlreadyPublishing) => {
                                    warn!("createStream rejected: already publishing");
                                    RtmpCommand::new("_error", tx_num)
                                        .arg(vec![
                                            ("level", "error"),
                                            ("code", "NetStream.Create.Failed"),
                                            ("description", "Already publishing"),
                                        ])
                                        .send(&mut client_tx, 3, 0, s2c_chunk)
                                        .await
                                        .ok();
                                }
                                _ => {}
                            }
                        }
                        // take stream
                        "publish" if let Some(mut stream) = stream.take() => {
                            let stream_key = command_stream.clone().unwrap_or_default();
                            stream.stream_key = command_stream;
                            match stream_manager.handle_publish(stream_id, stream).await {
                                Ok(_) => {
                                    RtmpCommand::new("onStatus", 0.0)
                                        .arg(vec![
                                            ("level", "status"),
                                            ("code", "NetStream.Publish.Start"),
                                            ("description", "Publishing started."),
                                        ])
                                        .send(&mut client_tx, 3, stream_id, s2c_chunk)
                                        .await
                                        .ok();
                                    info!("Client {client_id} publish to stream \"{stream_key}\"");
                                }
                                Err(e) => {
                                    let msg = match e {
                                        StreamError::AlreadyPublishing => "Already publishing",
                                        StreamError::StreamNotFound => "Stream not found",
                                        _ => "Publish failed",
                                    };
                                    warn!("publish rejected: {}", msg);
                                    RtmpCommand::new("onStatus", 0.0)
                                        .arg(vec![
                                            ("level", "error"),
                                            ("code", "NetStream.Publish.BadName"),
                                            ("description", msg),
                                        ])
                                        .send(&mut client_tx, 3, stream_id, s2c_chunk)
                                        .await
                                        .ok();
                                }
                            }
                        }
                        "FCUnpublish" => {
                            stream_manager
                                .handle_unpublish(stream_id, client_id)
                                .await
                                .ok();
                            RtmpCommand::new("onStatus", 0.0)
                                .arg(vec![
                                    ("level", "status"),
                                    ("code", "NetStream.Unpublish.Success"),
                                    ("description", "Unpublished."),
                                ])
                                .send(&mut client_tx, 3, stream_id, s2c_chunk)
                                .await
                                .ok();
                        }
                        "closeStream" => {
                            stream_manager
                                .handle_close_stream(stream_id, client_id)
                                .await
                                .ok();
                            RtmpCommand::new("onStatus", 0.0)
                                .arg(vec![
                                    ("level", "status"),
                                    ("code", "NetStream.Unpublish.Success"),
                                    ("description", "Stream closed."),
                                ])
                                .send(&mut client_tx, 3, stream_id, s2c_chunk)
                                .await
                                .ok();
                        }
                        "deleteStream" => {
                            stream_manager
                                .handle_delete_stream(stream_id, client_id)
                                .await
                                .ok();
                            RtmpCommand::new("onStatus", 0.0)
                                .arg(vec![
                                    ("level", "status"),
                                    ("code", "NetStream.DeleteStream.Success"),
                                    ("description", "Stream deleted."),
                                ])
                                .send(&mut client_tx, 3, stream_id, s2c_chunk)
                                .await
                                .ok();
                            break;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        // 将消息发送到流管理器
        stream_manager.handle_rtmp_message(msg).await;
    }

    stream_manager.handle_disconnect(client_id).await;
    Ok(())
}

// 平台相关的原始目的地址获取
#[cfg(target_os = "linux")]
fn get_original_destination(socket: &TcpStream) -> Result<String> {
    use nix::sys::socket::{SockaddrIn, SockaddrIn6, sockopt};
    use std::os::unix::io::{AsRawFd, BorrowedFd};

    let fd = unsafe { BorrowedFd::borrow_raw(socket.as_raw_fd()) };

    // 尝试 IPv4
    if let Ok(addr) = nix::sys::socket::getsockopt(&fd, sockopt::OriginalDst) {
        return Ok(SockaddrIn::from(addr).to_string());
    }

    // 尝试 IPv6
    if let Ok(addr) = nix::sys::socket::getsockopt(&fd, sockopt::Ip6tOriginalDst) {
        return Ok(SockaddrIn6::from(addr).to_string());
    }

    Err(anyhow::anyhow!("Failed to get original destination"))
}

#[cfg(not(target_os = "linux"))]
fn get_original_destination(_socket: &TcpStream) -> Result<String> {
    Err(anyhow::anyhow!("Platform not supported"))
}
