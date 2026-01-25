use crate::amf::{amf_command_name, rewrite_amf};
use crate::handshake::{handshake_with_client, handshake_with_upstream};
use crate::rtmp::{read_rtmp_message, write_rtmp_message};
use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use tracing::{debug, error, info, trace};

#[derive(Clone, Debug)]
pub struct UpstreamConfig {
    pub addr: String,
    pub app: Option<String>,
    pub stream: Option<String>,
}

struct Target {
    conn: Option<tokio::net::tcp::OwnedWriteHalf>,
    config: UpstreamConfig,
    c2u_chunk: usize,
}

pub async fn handle_client(mut client: TcpStream, configs: Vec<UpstreamConfig>) -> Result<()> {
    // Configure TCP options
    client.set_nodelay(true)?;

    // 1. Handshake with client first to prevent timeout
    handshake_with_client(&mut client)
        .await
        .context("handshake with client failed")?;

    // State for client
    let mut u2c_chunk = 128usize;
    let mut client_chunk = 128usize;
    let mut client_headers: Vec<Option<crate::rtmp::RtmpHeader>> = vec![None; 64];

    // Targets state
    let mut targets: Vec<Target> = configs
        .into_iter()
        .map(|c| Target {
            conn: None,
            config: c,
            c2u_chunk: 128,
        })
        .collect();



    loop {
        // Concurrently read from client
        let msg = read_rtmp_message(&mut client, &mut client_chunk, &mut client_headers[..])
            .await
            .context("client read error")?;

        trace!(
            direction = "c->u",
            csid = msg.csid,
            msg_type = msg.msg_type,
            len = msg.payload.len(),
            "received message from client"
        );

        let cmd_opt = if msg.msg_type == 20 {
            amf_command_name(&msg.payload).ok()
        } else {
            None
        };

        // If client sets chunk size, update local knowledge
        if msg.msg_type == 1 && msg.payload.len() >= 4 {
            let new_size = u32::from_be_bytes(msg.payload[..4].try_into().unwrap()) as usize;
            trace!(
                old_chunk = client_chunk,
                new_chunk = new_size,
                "client set chunk size"
            );
            client_chunk = new_size;
        }

        // Handle commands locally to ensure client progress
        if let Some(ref cmd) = cmd_opt {
            trace!("handling command locally: {}", cmd);
            match cmd.as_str() {
                "connect" => {
                    // Send local responses for connect
                    // 1. Window Ack Size
                    let mut m = crate::rtmp::RtmpMessage {
                        csid: 2,
                        msg_type: 5,
                        stream_id: 0,
                        payload: BytesMut::from(&2500000u32.to_be_bytes()[..]),
                    };
                    write_rtmp_message(&mut client, &m, u2c_chunk).await.ok();
                    // 2. Set Peer Bandwidth
                    m.msg_type = 6;
                    m.payload = BytesMut::from(&[0x26, 0x25, 0xa0, 0x00, 0x02][..]); // 2.5MB, Large
                    write_rtmp_message(&mut client, &m, u2c_chunk).await.ok();
                    // 3. Set Chunk Size (optional, but good)
                    m.msg_type = 1;
                    m.payload = BytesMut::from(&4096u32.to_be_bytes()[..]);
                    write_rtmp_message(&mut client, &m, u2c_chunk).await.ok();
                    u2c_chunk = 4096;
                    // 4. _result(connect)
                    // We need to parse transaction ID (usually 1.0)
                    let mut p = &msg.payload[..];
                    crate::amf::amf_read_string(&mut p).ok(); // skip connect
                    let tx = crate::amf::amf_read_number(&mut p).unwrap_or(1.0);

                    let mut out = BytesMut::new();
                    crate::amf::amf_write_string(&mut out, "_result");
                    crate::amf::amf_write_number(&mut out, tx);
                    // Properties
                    crate::amf::amf_write_object(&mut out, &[
                        ("fmsVer".into(), crate::amf::Amf0::String("FMS/3,0,1,123".into())),
                        ("capabilities".into(), crate::amf::Amf0::Number(31.0)),
                    ]);
                    // Information
                    crate::amf::amf_write_object(&mut out, &[
                        ("level".into(), crate::amf::Amf0::String("status".into())),
                        ("code".into(), crate::amf::Amf0::String("NetConnection.Connect.Success".into())),
                        ("description".into(), crate::amf::Amf0::String("Connection succeeded.".into())),
                    ]);
                    let resp = crate::rtmp::RtmpMessage {
                        csid: 3,
                        msg_type: 20,
                        stream_id: 0,
                        payload: out,
                    };
                    write_rtmp_message(&mut client, &resp, u2c_chunk).await.ok();
                }
                "createStream" => {
                    let mut p = &msg.payload[..];
                    crate::amf::amf_read_string(&mut p).ok(); 
                    let tx = crate::amf::amf_read_number(&mut p).unwrap_or(2.0);
                    
                    let mut out = BytesMut::new();
                    crate::amf::amf_write_string(&mut out, "_result");
                    crate::amf::amf_write_number(&mut out, tx);
                    crate::amf::amf_write_null(&mut out);
                    crate::amf::amf_write_number(&mut out, 1.0); // stream id 1
                    
                    let resp = crate::rtmp::RtmpMessage {
                        csid: 3,
                        msg_type: 20,
                        stream_id: 0,
                        payload: out,
                    };
                    write_rtmp_message(&mut client, &resp, u2c_chunk).await.ok();
                }
                "publish" => {
                    let mut out = BytesMut::new();
                    crate::amf::amf_write_string(&mut out, "onStatus");
                    crate::amf::amf_write_number(&mut out, 0.0);
                    crate::amf::amf_write_null(&mut out);
                    crate::amf::amf_write_object(&mut out, &[
                        ("level".into(), crate::amf::Amf0::String("status".into())),
                        ("code".into(), crate::amf::Amf0::String("NetStream.Publish.Start".into())),
                        ("description".into(), crate::amf::Amf0::String("Publishing started.".into())),
                    ]);
                    let resp = crate::rtmp::RtmpMessage {
                        csid: 3,
                        msg_type: 20,
                        stream_id: 1,
                        payload: out,
                    };
                    write_rtmp_message(&mut client, &resp, u2c_chunk).await.ok();
                }
                _ => {}
            }
        }

        // Forward to all targets
        for target in &mut targets {
            // Establish connection and handshake if not yet done
            if target.conn.is_none() {
                debug!("connecting to target: {}", target.config.addr);
                match tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&target.config.addr)).await {
                    Ok(Ok(mut s)) => {
                        s.set_nodelay(true).ok();
                        debug!("handshaking with target: {}", target.config.addr);
                        if let Err(e) = handshake_with_upstream(&mut s).await {
                            error!("handshake failed for target {}: {}", target.config.addr, e);
                            continue;
                        }

                        let addr = target.config.addr.clone();
                        let (mut reader, writer) = s.into_split();
                        tokio::spawn(async move {
                            let mut drain = [0u8; 4096];
                            while let Ok(n) = reader.read(&mut drain).await {
                                if n == 0 { break; }
                                trace!("discarded {} bytes from upstream {}", n, addr);
                            }
                            debug!("upstream {} reader task ended", addr);
                        });
                        target.conn = Some(writer);
                    }
                    Ok(Err(e)) => {
                        error!("failed to connect to target {}: {}", target.config.addr, e);
                        continue;
                    }
                    Err(_) => {
                        error!("connection timeout for target {}", target.config.addr);
                        continue;
                    }
                }
            }

            if let Some(ref mut s) = target.conn {
                // Consume any data from upstream to avoid blocking
                // We should really do this in a background task or select!
                // For now, let's just try to write.
                
                let mut out_msg = msg.clone();

                if msg.msg_type == 20 {
                    // Rewrite if required
                    let app = target.config.app.as_deref();
                    let stream = target.config.stream.as_deref();

                    if let (Some(app), Some(stream)) = (app, stream) {
                        if let Ok(Some((new_payload, summary))) =
                            rewrite_amf(&msg.payload, app, stream)
                        {
                            out_msg.payload = new_payload;
                            info!("c->u [{}]: {}", target.config.addr, summary);
                        } else if let Some(ref cmd) = cmd_opt {
                            debug!("c->u [{}]: {}", target.config.addr, cmd);
                        }
                    } else if let Some(ref cmd) = cmd_opt {
                        // Relay mode (original app/stream)
                        debug!("c->u [{}] (relay): {}", target.config.addr, cmd);
                    }
                }

                if let Err(e) = write_rtmp_message(s, &out_msg, target.c2u_chunk).await {
                    error!("failed to write to target {}: {}", target.config.addr, e);
                    target.conn = None; // Drop target on error
                } else if msg.msg_type == 1 {
                    target.c2u_chunk = client_chunk;
                }
            }
        }

        // Check if we should enter transparent mode (broadcast)
        if let Some("publish") = cmd_opt.as_deref() {
            info!("publish command detected — entering broadcast passthrough");
            break;
        }
    }

    // Entering broadcast passthrough loop
    let mut buf = vec![0u8; 16384];
    loop {
        use tokio::io::AsyncReadExt;
        let n = client.read(&mut buf).await.context("client read error")?;
        if n == 0 {
            info!("client closed connection");
            break;
        }

        for target in &mut targets {
            if let Some(ref mut s) = target.conn {
                if let Err(e) = s.write_all(&buf[..n]).await {
                    error!("target {} disconnected during passthrough: {}", target.config.addr, e);
                    target.conn = None;
                }
            }
        }
    }

    Ok(())
}

