use crate::amf::amf_command_name;
use crate::handshake::handshake_with_client;
use crate::rtmp::{read_rtmp_message, write_rtmp_message, RtmpMessage};
use crate::forwarder::{ForwardEvent, ProtocolSnapshot, TargetActor};
use crate::web::FlvStreamManager;
use anyhow::Result;
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::info;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpstreamConfig {
    pub addr: String,
    pub app: Option<String>,
    pub stream: Option<String>,
    pub enabled: bool,
}

pub async fn handle_client(mut client: TcpStream, shared_config: crate::config::SharedConfig, flv_manager: std::sync::Arc<FlvStreamManager>) -> Result<()> {
    client.set_nodelay(true)?;
    handshake_with_client(&mut client).await?;

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
                            crate::rtmp::send_rtmp_command(&mut client, 3, 0, u2c_chunk, "_result", tx_num, &[], &[crate::amf::Amf0::Number(1.0)]).await.ok();
                        }
                        "publish" => {
                            crate::rtmp::send_rtmp_command(&mut client, 3, 1, u2c_chunk, "onStatus", 0.0, &[], &[
                                crate::amf::Amf0::Object(vec![
                                    ("level".into(), crate::amf::Amf0::String("status".into())),
                                    ("code".into(), crate::amf::Amf0::String("NetStream.Publish.Start".into())),
                                    ("description".into(), crate::amf::Amf0::String("Publishing started.".into()))
                                ])
                            ]).await.ok();
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
        
        // 4. Handle HTTP-FLV streaming
        if let Some(ref stream_name) = snapshot.client_stream {
            tracing::debug!("Server: Forwarding RTMP message to FLV manager for stream: {}", stream_name);
            flv_manager.handle_rtmp_message(stream_name, &msg).await;
        } else {
            tracing::debug!("Server: No stream name available for RTMP message type: {}", msg.msg_type);
        }
    }

    for tx in active_workers.values() {
        let _ = tx.try_send(ForwardEvent::Shutdown);
    }

    Ok(())
}

fn target_key(u: &UpstreamConfig) -> String {
    format!("{}/{}/{}", u.addr, u.app.as_deref().unwrap_or(""), u.stream.as_deref().unwrap_or(""))
}
