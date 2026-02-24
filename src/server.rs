use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tracing::{info, warn};

use crate::error::Result;
use crate::rtmp::handshake_with_client;
use crate::rtmp::{RtmpCommand, RtmpMessage, RtmpMessageStream};
use crate::stream::{StreamError, StreamInfo, StreamManager, StreamState};
use crate::util::{get_original_destination, try_handle_socks5};

static CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::new(1);
const DEFAULT_STREAM_ID: u32 = 1;

pub async fn handle_client(
    mut client: TcpStream,
    shared_config: crate::config::SharedConfig,
    stream_manager: Arc<StreamManager>,
) -> Result<()> {
    client.set_nodelay(true)?;

    let orig_dest_addr = try_handle_socks5(&mut client)
        .await?
        .or_else(|| get_original_destination(&client).ok())
        .map(|addr| {
            // 省略默认端口 1935
            if let Some(host) = addr.strip_suffix(":1935") {
                host.to_string()
            } else {
                addr
            }
        });
    if let Some(addr) = &orig_dest_addr {
        info!("Captured original destination address: {addr}",);
    }

    handshake_with_client(&mut client).await?;

    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut s2c_chunk = 128usize;

    let mut stream = Some(StreamInfo {
        is_default: false,
        stream_id: DEFAULT_STREAM_ID,
        client_id,
        tc_url: None,
        app_name: None,
        stream_key: None,
        state: StreamState::None,
        last_active: Instant::now(),
        chunk_szie: 128,
        metadata: None,
        video_seq_hdr: None,
        audio_seq_hdr: None,
        orig_dest_addr,
        message_tx: stream_manager.message_tx.clone(),
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
                            for (k, v) in cmd.command_object.into_iter() {
                                match k.as_str() {
                                    "tcUrl" => {
                                        let tc_url = v.into_string();
                                        if let Some(x) = tc_url.as_deref() {
                                            info!("Client {client_id} connect with tcUrl: {x}");
                                            if let Some(s) = stream.as_mut() {
                                                s.tc_url = tc_url;
                                            }
                                        }
                                    }
                                    "app" => command_app = v.into_string(),
                                    _ => {}
                                }
                            }
                        }
                        "publish" | "releaseStream" | "FCPublish" => {
                            command_stream =
                                cmd.args.into_iter().next().and_then(|v| v.into_string());
                        }
                        _ => {}
                    }

                    match cmd.name.as_str() {
                        "connect" if let Some(stream) = stream.as_mut() => {
                            // Validate app if configured
                            let server_cfg = shared_config.read().unwrap().server.clone();
                            if let Some(expected_app) = &server_cfg.app
                                && command_app.as_deref() != Some(expected_app.as_str())
                            {
                                warn!("Connection rejected: app mismatch");
                                RtmpCommand::error(
                                    tx_num,
                                    "NetConnection.Connect.Rejected",
                                    "Invalid app",
                                )
                                .send(&mut client_tx, 3, 0, s2c_chunk)
                                .await
                                .ok();
                                break;
                            }

                            stream.app_name = command_app;
                            if let Err(StreamError::AlreadyPublishing) =
                                stream_manager.handle_connect(&stream.app_name).await
                            {
                                warn!("Connection rejected: already publishing");
                                RtmpCommand::error(
                                    tx_num,
                                    "NetConnection.Connect.Rejected",
                                    "Already publishing",
                                )
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
                            RtmpMessage::window_ack_size(2500000)
                                .write_to(&mut client_tx)
                                .await?;
                            // Peer Bandwidth
                            RtmpMessage::set_peer_bandwidth(2500000, 2)
                                .write_to(&mut client_tx)
                                .await?;
                            // Set Chunk Size
                            RtmpMessage::set_chunk_size(4096)
                                .write_to(&mut client_tx)
                                .await?;
                            s2c_chunk = 4096;

                            // _result(connect)
                            RtmpCommand::result_connect(tx_num)
                                .send(&mut client_tx, 3, 0, s2c_chunk)
                                .await
                                .ok();
                        }
                        "createStream" if let Some(stream) = stream.as_mut() => {
                            match stream_manager.handle_create_stream().await {
                                Ok(_) => {
                                    stream.state = StreamState::Idle;
                                    RtmpCommand::result_create_stream(tx_num, stream.stream_id)
                                        .send(&mut client_tx, 3, 0, s2c_chunk)
                                        .await
                                        .ok();
                                    info!("Created stream for client {}", client_id);
                                }
                                Err(StreamError::AlreadyPublishing) => {
                                    warn!("createStream rejected: already publishing");
                                    RtmpCommand::error(
                                        tx_num,
                                        "NetStream.Create.Failed",
                                        "Already publishing",
                                    )
                                    .send(&mut client_tx, 3, 0, s2c_chunk)
                                    .await
                                    .ok();
                                }
                                _ => {}
                            }
                        }
                        // take stream
                        "publish" if let Some(mut stream) = stream.take() => {
                            // Validate stream_key if configured
                            let server_cfg = shared_config.read().unwrap().server.clone();
                            if let Some(expected_key) = &server_cfg.stream_key
                                && command_stream.as_deref() != Some(expected_key.as_str())
                            {
                                warn!("Publish rejected: stream_key mismatch");
                                RtmpCommand::on_status(
                                    "NetStream.Publish.BadName",
                                    "error",
                                    "Invalid stream key",
                                )
                                .send(&mut client_tx, 3, stream_id, s2c_chunk)
                                .await
                                .ok();
                                break;
                            }

                            let stream_key = command_stream.clone().unwrap_or_default();
                            stream.stream_key = command_stream;
                            match stream_manager.handle_publish(stream_id, stream).await {
                                Ok(_) => {
                                    RtmpCommand::on_status(
                                        "NetStream.Publish.Start",
                                        "status",
                                        "Publishing started.",
                                    )
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
                                    RtmpCommand::on_status(
                                        "NetStream.Publish.BadName",
                                        "error",
                                        msg,
                                    )
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
                            RtmpCommand::on_status(
                                "NetStream.Unpublish.Success",
                                "status",
                                "Unpublished.",
                            )
                            .send(&mut client_tx, 3, stream_id, s2c_chunk)
                            .await
                            .ok();
                        }
                        "closeStream" => {
                            stream_manager
                                .handle_close_stream(stream_id, client_id)
                                .await
                                .ok();
                            RtmpCommand::on_status(
                                "NetStream.Unpublish.Success",
                                "status",
                                "Stream closed.",
                            )
                            .send(&mut client_tx, 3, stream_id, s2c_chunk)
                            .await
                            .ok();
                        }
                        "deleteStream" => {
                            stream_manager
                                .handle_delete_stream(stream_id, client_id)
                                .await
                                .ok();
                            RtmpCommand::on_status(
                                "NetStream.DeleteStream.Success",
                                "status",
                                "Stream deleted.",
                            )
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

        // stream 为 None 或 is_default 为 true 时，将消息发送到流管理器
        if !stream.as_ref().is_some_and(|s| !s.is_default) {
            stream_manager.handle_rtmp_message(msg).await;
        }
    }

    stream_manager.handle_disconnect(client_id).await;
    Ok(())
}
