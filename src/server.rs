use crate::amf::{amf_command_name, Amf0, AmfReader};
use crate::handshake::handshake_with_client;
use crate::rtmp::write_rtmp_message2;
use crate::rtmp_codec::RtmpMessageStream;
use crate::stream_manager::{StreamManager, StreamError, StreamInfo, StreamState};
use anyhow::Result;
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tracing::{info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

static CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwarderConfig {
    pub addr: String,
    pub app: Option<String>,
    pub stream: Option<String>,
    pub enabled: bool,
}

pub async fn handle_client(
    mut client: TcpStream,
    _shared_config: crate::config::SharedConfig,
    stream_manager: Arc<StreamManager>,
) -> Result<()> {
    client.set_nodelay(true)?;
    
    // 获取原始目的地址
    let orig_dest_addr = get_original_destination(&client).ok();
    if orig_dest_addr.is_some() {
        info!("Captured original destination address: {}", orig_dest_addr.as_ref().unwrap());
    }
    
    handshake_with_client(&mut client).await?;

    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut s2c_chunk = 128usize;
    
    let mut stream = Some(StreamInfo {
        stream_id: 1,
        client_id,
        app_name: None,
        stream_key: None,
        state: StreamState::None,
        last_active: Instant::now(),
        chunk_szie: 128,
        metadata: None,
        video_seq_hdr: None,
        audio_seq_hdr: None,
        orig_dest_addr
    });

    let (client_rx,mut client_tx) = client.into_split();
    let mut msg_stream = RtmpMessageStream::new(client_rx, stream.as_ref().unwrap().chunk_szie);
    while let Some(msg) = msg_stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => { info!("Client disconnected: {}", e); break; }
        };
        // 提取app和stream信息
        let mut command_app = None;
        let mut command_stream = None;
        if msg.header.msg_type == 20 {
            let payload = msg.payload();
            if let Ok(cmd) = amf_command_name(&payload) {
                let mut r = AmfReader::new(&payload);
                let _ = r.read_string();
                match cmd.as_str() {
                    "connect" => {
                        if let Ok(obj) = r.read_number().and_then(|_| r.read_object())
                            && let Some((_, Amf0::String(s))) = obj.iter().find(|(k, _)| k == "app") {
                                command_app = Some(s.clone());
                            }
                    }
                    "publish" | "releaseStream" | "FCPublish" => {
                        let _ = r.read_number().and_then(|_| r.read_null());
                        if let Ok(s) = r.read_string() { command_stream = Some(s); }
                    }
                    _ => {}
                }
            }
        }

        match msg.header.msg_type {     
            1 => if msg.header.msg_len >= 4 {
                let payload = msg.payload();
                let c2s_chunk = u32::from_be_bytes(payload[..4].try_into().unwrap()) as usize;
                msg_stream.set_chunk_size(c2s_chunk);
                info!("Client {client_id} set chunk size to {c2s_chunk}");
                stream_manager.handle_set_chunk_size(client_id, c2s_chunk).await;
            }
            20 => {
                let payload = msg.payload();
                if let Ok(cmd) = amf_command_name(&payload) {
                    let mut reader = AmfReader::new(&payload);
                    let _ = reader.read_string(); // name
                    let tx_num = reader.read_number().unwrap_or(0.0);

                    match cmd.as_str() {
                        "connect" if let Some(stream) = stream.as_mut() => {
                            stream.app_name = command_app;
                            if let Err(StreamError::AlreadyPublishing) = stream_manager.handle_connect(&stream.app_name).await {
                                warn!("Connection rejected: already publishing");
                                crate::rtmp::send_rtmp_command(&mut client_tx, 3, 0, s2c_chunk, "_error", tx_num,
                                    &[],
                                    &[crate::amf::Amf0::Object(vec![
                                        ("level".into(), crate::amf::Amf0::String("error".into())),
                                        ("code".into(), crate::amf::Amf0::String("NetConnection.Connect.Rejected".into())),
                                        ("description".into(), crate::amf::Amf0::String("Already publishing".into()))
                                    ])]
                                ).await.ok();
                                break;
                            }
                            info!("Client {client_id} connect to app \"{}\"", stream.app_name.as_deref().unwrap_or_default());

                            // Window Ack Size
                            write_rtmp_message2(&mut client_tx, 2, 0, 5, 0,
                                &Bytes::from(2500000u32.to_be_bytes().to_vec()), s2c_chunk
                            ).await.ok();
                            // Peer Bandwidth
                            write_rtmp_message2(&mut client_tx, 2, 0, 6, 0,
                                &Bytes::from(&[0x26, 0x25, 0xa0, 0x00, 0x02][..]), s2c_chunk
                            ).await.ok();
                            // Set Chunk Size
                            write_rtmp_message2(&mut client_tx, 2, 0, 1, 0 ,
                                &Bytes::from(4096u32.to_be_bytes().to_vec()), s2c_chunk
                            ).await.ok();
                            s2c_chunk = 4096;
                            
                            // _result(connect)
                            crate::rtmp::send_rtmp_command(&mut client_tx, 3, 0, s2c_chunk, "_result", tx_num, 
                                &[("fmsVer", Amf0::String("FMS/3,0,1,123".into())), ("capabilities", Amf0::Number(31.0))],
                                &[Amf0::Object(vec![
                                    ("level".into(), Amf0::String("status".into())),
                                    ("code".into(), Amf0::String("NetConnection.Connect.Success".into())),
                                    ("description".into(), Amf0::String("Connection succeeded.".into()))
                                ])]
                            ).await.ok();
                        }
                        "createStream" if let Some(stream) = stream.as_mut() => {
                            match stream_manager.handle_create_stream().await {
                                Ok(_) => {
                                    stream.state = StreamState::Idle;
                                    crate::rtmp::send_rtmp_command(&mut client_tx, 3, 0, s2c_chunk, "_result", tx_num, &[], &[Amf0::Number(stream.stream_id as f64)]).await.ok();
                                    info!("Created stream for client {}", client_id);
                                }
                                Err(StreamError::AlreadyPublishing) => {
                                    warn!("createStream rejected: already publishing");
                                    crate::rtmp::send_rtmp_command(&mut client_tx, 3, 0, s2c_chunk, "_error", tx_num, &[], &[
                                        Amf0::Object(vec![
                                            ("level".into(), Amf0::String("error".into())),
                                            ("code".into(), Amf0::String("NetStream.Create.Failed".into())),
                                            ("description".into(), Amf0::String("Already publishing".into()))
                                        ])
                                    ]).await.ok();
                                }
                                _ => {}
                            }
                        }
                        // take stream
                        "publish" if let Some(mut stream) = stream.take() => {
                            let stream_key = command_stream.clone().unwrap_or_default();
                            stream.stream_key = command_stream;
                            match stream_manager.handle_publish(1, stream).await {
                                Ok(_) => {
                                    crate::rtmp::send_rtmp_command(&mut client_tx, 3, 1, s2c_chunk, "onStatus", 0.0, &[], &[
                                        crate::amf::Amf0::Object(vec![
                                            ("level".into(), crate::amf::Amf0::String("status".into())),
                                            ("code".into(), crate::amf::Amf0::String("NetStream.Publish.Start".into())),
                                            ("description".into(), crate::amf::Amf0::String("Publishing started.".into()))
                                        ])
                                    ]).await.ok();
                                   info!("Client {client_id} publish to stream \"{stream_key}\"");
                                }
                                Err(e) => {
                                    let msg = match e {
                                        StreamError::AlreadyPublishing => "Already publishing",
                                        StreamError::StreamNotFound => "Stream not found",
                                        _ => "Publish failed",
                                    };
                                    warn!("publish rejected: {}", msg);
                                    crate::rtmp::send_rtmp_command(&mut client_tx, 3, 1, s2c_chunk, "onStatus", 0.0, &[], &[
                                        crate::amf::Amf0::Object(vec![
                                            ("level".into(), crate::amf::Amf0::String("error".into())),
                                            ("code".into(), crate::amf::Amf0::String("NetStream.Publish.BadName".into())),
                                            ("description".into(), crate::amf::Amf0::String(msg.into()))
                                        ])
                                    ]).await.ok();
                                }
                            }
                        }
                        "FCUnpublish" => {
                            stream_manager.handle_unpublish(1, client_id).await.ok();
                            crate::rtmp::send_rtmp_command(&mut client_tx, 3, 1, s2c_chunk, "onStatus", 0.0, &[], &[
                                Amf0::Object(vec![
                                    ("level".into(), Amf0::String("status".into())),
                                    ("code".into(), Amf0::String("NetStream.Unpublish.Success".into())),
                                    ("description".into(), Amf0::String("Unpublished.".into()))
                                ])
                            ]).await.ok();
                        }
                        "closeStream" => {
                            stream_manager.handle_close_stream(1, client_id).await.ok();
                            crate::rtmp::send_rtmp_command(&mut client_tx, 3, 1, s2c_chunk, "onStatus", 0.0, &[], &[
                                Amf0::Object(vec![
                                    ("level".into(), Amf0::String("status".into())),
                                    ("code".into(), Amf0::String("NetStream.Unpublish.Success".into())),
                                    ("description".into(), Amf0::String("Stream closed.".into()))
                                ])
                            ]).await.ok();
                        }
                        "deleteStream" => {
                            stream_manager.handle_delete_stream(1, client_id).await.ok();
                            crate::rtmp::send_rtmp_command(&mut client_tx, 3, 1, s2c_chunk, "onStatus", 0.0, &[], &[
                                Amf0::Object(vec![
                                    ("level".into(), Amf0::String("status".into())),
                                    ("code".into(), Amf0::String("NetStream.DeleteStream.Success".into())),
                                    ("description".into(), Amf0::String("Stream deleted.".into()))
                                ])
                            ]).await.ok();
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
    use std::os::unix::io::{AsRawFd, BorrowedFd};
    use nix::sys::socket::{sockopt, SockaddrIn, SockaddrIn6};
    
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
