use crate::amf::amf_command_name;
use crate::handshake::handshake_with_client;
use crate::rtmp::{read_rtmp_message, write_rtmp_message, RtmpMessage};
use crate::forwarder::{ForwardEvent, ProtocolSnapshot, TargetActor};
use crate::web::FlvStreamManager;
use crate::stream_manager::{StreamManager, StreamError};
use anyhow::Result;
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

static CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpstreamConfig {
    pub addr: String,
    pub app: Option<String>,
    pub stream: Option<String>,
    pub enabled: bool,
}

pub async fn handle_client(
    mut client: TcpStream,
    shared_config: crate::config::SharedConfig,
    flv_manager: Arc<FlvStreamManager>,
    stream_manager: Arc<StreamManager>,
) -> Result<()> {
    client.set_nodelay(true)?;
    handshake_with_client(&mut client).await?;

    let client_id = CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut u2c_chunk = 128usize;
    let mut c2u_chunk = 128usize;
    let mut client_headers: Vec<Option<crate::rtmp::RtmpHeader>> = vec![None; 64];

    let mut active_workers: std::collections::HashMap<String, mpsc::Sender<ForwardEvent>> = std::collections::HashMap::new();
    let mut snapshot = ProtocolSnapshot::default();

    loop {
        // 1. Sync targets
        {
            let config = shared_config.read().unwrap();
            let mut expected = Vec::new();
            if config.relay_enabled
                && let Some(ref addr) = config.relay_addr {
                    expected.push(UpstreamConfig { addr: addr.clone(), app: None, stream: None, enabled: true });
                }
            for u in &config.upstreams { if u.enabled { expected.push(u.clone()); } }

            active_workers.retain(|key, tx| {
                let needed = expected.iter().any(|u| &target_key(u) == key);
                if !needed { let _ = tx.try_send(ForwardEvent::Shutdown); }
                needed
            });

            for cfg in expected {
                let key = target_key(&cfg);
                active_workers.entry(key).or_insert_with(|| {
                    let (tx, rx) = mpsc::channel(128);
                    tokio::spawn(TargetActor { config: cfg, rx, snapshot: snapshot.clone() }.run());
                    tx
                });
            }
        }

        // 2. Read and Handle Message
        let msg = match read_rtmp_message(&mut client, &mut c2u_chunk, &mut client_headers[..]).await {
            Ok(m) => m,
            Err(e) => { info!("Client disconnected: {}", e); break; }
        };

        let old_app = snapshot.client_app.clone();
        let old_stream = snapshot.client_stream.clone();
        snapshot.update_from_message(&msg);
        
        if snapshot.client_app != old_app
            && let Some(ref a) = snapshot.client_app { info!("client connect: app=\"{}\"", a); }
        if snapshot.client_stream != old_stream
            && let Some(ref s) = snapshot.client_stream { info!("client publish: stream=\"{}\"", s); }

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
                            match stream_manager.handle_create_stream(snapshot.client_app.as_deref().unwrap_or("live"), client_id).await {
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
                            let stream_key = snapshot.client_stream.as_deref().unwrap_or("stream");
                            let app_name = snapshot.client_app.as_deref().unwrap_or("live");
                            
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
                        "FCUnpublish" | "deleteStream" | "closeStream" => {
                            let result = match cmd.as_str() {
                                "FCUnpublish" => stream_manager.handle_unpublish(1, client_id).await,
                                "deleteStream" => stream_manager.handle_delete_stream(1, client_id).await,
                                "closeStream" => stream_manager.handle_close_stream(1, client_id).await,
                                _ => Ok(()),
                            };
                            
                            if let Err(StreamError::NotPublishingClient) = result {
                                warn!("{} rejected: not publishing client", cmd);
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        // 3. Dispatch to all workers
        for tx in active_workers.values() {
            let _ = tx.try_send(ForwardEvent::Message(msg.clone()));
        }
        
        // 4. Handle HTTP-FLV streaming - 固定使用 "stream" 作为流名称
        tracing::debug!("Server: Forwarding RTMP message to FLV manager for fixed stream: stream");
        flv_manager.handle_rtmp_message("stream", &msg).await;
    }

    stream_manager.handle_disconnect(client_id).await;
    
    for tx in active_workers.values() {
        let _ = tx.try_send(ForwardEvent::Shutdown);
    }

    Ok(())
}

fn target_key(u: &UpstreamConfig) -> String {
    format!("{}/{}/{}", u.addr, u.app.as_deref().unwrap_or(""), u.stream.as_deref().unwrap_or(""))
}
