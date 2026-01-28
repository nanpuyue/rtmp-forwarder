use crate::amf::amf_command_name;
use crate::handshake::handshake_with_client;
use crate::rtmp::{read_rtmp_message, write_rtmp_message, RtmpMessage};
use crate::stream_manager::{StreamManager, StreamError};
use anyhow::Result;
use bytes::BytesMut;
use tokio::net::TcpStream;
use tracing::{info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

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
        info!("Captured original destination address");
    }
    
    handshake_with_client(&mut client).await?;

    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut u2c_chunk = 128usize;
    let mut c2u_chunk = 128usize;
    let mut client_headers: Vec<Option<crate::rtmp::RtmpHeader>> = vec![None; 64];
    let mut client_app: Option<String> = None;
    let mut client_stream: Option<String> = None;

    loop {
        let msg = match read_rtmp_message(&mut client, &mut c2u_chunk, &mut client_headers[..]).await {
            Ok(m) => m,
            Err(e) => { info!("Client disconnected: {}", e); break; }
        };
        
        // 提取app和stream信息
        if msg.msg_type == 20 {
            if let Ok(cmd) = amf_command_name(&msg.payload) {
                let mut r = crate::amf::AmfReader::new(&msg.payload);
                let _ = r.read_string();
                match cmd.as_str() {
                    "connect" => {
                        if let Ok(obj) = r.read_number().and_then(|_| r.read_object())
                            && let Some((_, crate::amf::Amf0::String(s))) = obj.iter().find(|(k, _)| k == "app") {
                                client_app = Some(s.clone());
                            }
                    }
                    "publish" | "releaseStream" | "FCPublish" => {
                        let _ = r.read_number().and_then(|_| r.read_null());
                        if let Ok(s) = r.read_string() { client_stream = Some(s); }
                    }
                    _ => {}
                }
            }
        }

        match msg.msg_type {
            1 => if msg.payload.len() >= 4 {
                c2u_chunk = u32::from_be_bytes(msg.payload[..4].try_into().unwrap()) as usize;
            }
            20 => {
                if let Ok(cmd) = amf_command_name(&msg.payload) {
                    let mut r = crate::amf::AmfReader::new(&msg.payload);
                    let _ = r.read_string(); // name
                    let tx_num = r.read_number().unwrap_or(0.0);

                    match cmd.as_str() {
                        "connect" => {
                            if let Err(StreamError::AlreadyPublishing) = stream_manager.handle_connect().await {
                                warn!("Connection rejected: already publishing");
                                crate::rtmp::send_rtmp_command(&mut client, 3, 0, u2c_chunk, "_error", tx_num,
                                    &[],
                                    &[crate::amf::Amf0::Object(vec![
                                        ("level".into(), crate::amf::Amf0::String("error".into())),
                                        ("code".into(), crate::amf::Amf0::String("NetConnection.Connect.Rejected".into())),
                                        ("description".into(), crate::amf::Amf0::String("Already publishing".into()))
                                    ])]
                                ).await.ok();
                                break;
                            }
                            
                            // Window Ack Size
                            write_rtmp_message(&mut client, &RtmpMessage { csid: 2, timestamp: 0, msg_type: 5, stream_id: 0, payload: BytesMut::from(&2500000u32.to_be_bytes()[..]) }, u2c_chunk).await.ok();
                            // Peer Bandwidth
                            write_rtmp_message(&mut client, &RtmpMessage { csid: 2, timestamp: 0, msg_type: 6, stream_id: 0, payload: BytesMut::from(&[0x26, 0x25, 0xa0, 0x00, 0x02][..]) }, u2c_chunk).await.ok();
                            // Set Chunk Size
                            write_rtmp_message(&mut client, &RtmpMessage { csid: 2, timestamp: 0, msg_type: 1, stream_id: 0, payload: BytesMut::from(&4096u32.to_be_bytes()[..]) }, u2c_chunk).await.ok();
                            u2c_chunk = 4096;
                            
                            // _result(connect)
                            crate::rtmp::send_rtmp_command(&mut client, 3, 0, u2c_chunk, "_result", tx_num, 
                                &[("fmsVer", crate::amf::Amf0::String("FMS/3,0,1,123".into())), ("capabilities", crate::amf::Amf0::Number(31.0))],
                                &[crate::amf::Amf0::Object(vec![
                                    ("level".into(), crate::amf::Amf0::String("status".into())),
                                    ("code".into(), crate::amf::Amf0::String("NetConnection.Connect.Success".into())),
                                    ("description".into(), crate::amf::Amf0::String("Connection succeeded.".into()))
                                ])]
                            ).await.ok();
                        }
                        "createStream" => {
                            match stream_manager.handle_create_stream(
                                client_app.as_deref().unwrap_or("live"), 
                                client_id,
                                orig_dest_addr.clone()
                            ).await {
                                Ok(stream_id) => {
                                    crate::rtmp::send_rtmp_command(&mut client, 3, 0, u2c_chunk, "_result", tx_num, &[], &[crate::amf::Amf0::Number(stream_id as f64)]).await.ok();
                                }
                                Err(StreamError::AlreadyPublishing) => {
                                    warn!("createStream rejected: already publishing");
                                    crate::rtmp::send_rtmp_command(&mut client, 3, 0, u2c_chunk, "_error", tx_num, &[], &[
                                        crate::amf::Amf0::Object(vec![
                                            ("level".into(), crate::amf::Amf0::String("error".into())),
                                            ("code".into(), crate::amf::Amf0::String("NetStream.Create.Failed".into())),
                                            ("description".into(), crate::amf::Amf0::String("Already publishing".into()))
                                        ])
                                    ]).await.ok();
                                }
                                _ => {}
                            }
                        }
                        "publish" => {
                            let stream_key = client_stream.as_deref().unwrap_or("stream");
                            let app_name = client_app.as_deref().unwrap_or("live");
                            
                            match stream_manager.handle_publish(1, stream_key, client_id, app_name).await {
                                Ok(_) => {
                                    crate::rtmp::send_rtmp_command(&mut client, 3, 1, u2c_chunk, "onStatus", 0.0, &[], &[
                                        crate::amf::Amf0::Object(vec![
                                            ("level".into(), crate::amf::Amf0::String("status".into())),
                                            ("code".into(), crate::amf::Amf0::String("NetStream.Publish.Start".into())),
                                            ("description".into(), crate::amf::Amf0::String("Publishing started.".into()))
                                        ])
                                    ]).await.ok();
                                }
                                Err(e) => {
                                    let msg = match e {
                                        StreamError::AlreadyPublishing => "Already publishing",
                                        StreamError::StreamNotFound => "Stream not found",
                                        _ => "Publish failed",
                                    };
                                    warn!("publish rejected: {}", msg);
                                    crate::rtmp::send_rtmp_command(&mut client, 3, 1, u2c_chunk, "onStatus", 0.0, &[], &[
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
                            crate::rtmp::send_rtmp_command(&mut client, 3, 1, u2c_chunk, "onStatus", 0.0, &[], &[
                                crate::amf::Amf0::Object(vec![
                                    ("level".into(), crate::amf::Amf0::String("status".into())),
                                    ("code".into(), crate::amf::Amf0::String("NetStream.Unpublish.Success".into())),
                                    ("description".into(), crate::amf::Amf0::String("Unpublished.".into()))
                                ])
                            ]).await.ok();
                        }
                        "closeStream" => {
                            stream_manager.handle_close_stream(1, client_id).await.ok();
                            crate::rtmp::send_rtmp_command(&mut client, 3, 1, u2c_chunk, "onStatus", 0.0, &[], &[
                                crate::amf::Amf0::Object(vec![
                                    ("level".into(), crate::amf::Amf0::String("status".into())),
                                    ("code".into(), crate::amf::Amf0::String("NetStream.Unpublish.Success".into())),
                                    ("description".into(), crate::amf::Amf0::String("Stream closed.".into()))
                                ])
                            ]).await.ok();
                        }
                        "deleteStream" => {
                            stream_manager.handle_delete_stream(1, client_id).await.ok();
                            crate::rtmp::send_rtmp_command(&mut client, 3, 1, u2c_chunk, "onStatus", 0.0, &[], &[
                                crate::amf::Amf0::Object(vec![
                                    ("level".into(), crate::amf::Amf0::String("status".into())),
                                    ("code".into(), crate::amf::Amf0::String("NetStream.DeleteStream.Success".into())),
                                    ("description".into(), crate::amf::Amf0::String("Stream deleted.".into()))
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
