use crate::amf::{amf_command_name, is_publish, rewrite_amf};
use crate::handshake::{handshake_with_client, handshake_with_upstream};
use crate::rtmp::{read_rtmp_message, write_rtmp_message};
use anyhow::{Context, Result};
use tokio::net::TcpStream;
use tracing::{debug, info, trace};

pub async fn handle_client(
    mut client: TcpStream,
    upstream_addr: &str,
    app: &str,
    stream_name: &str,
) -> Result<()> {
    // Configure TCP options for both client and upstream
    client.set_nodelay(true)?;
    let mut upstream = TcpStream::connect(upstream_addr).await?;
    info!("Connected to upstream: {}", upstream_addr);
    upstream.set_nodelay(true)?;

    // Perform active RTMP handshake with client
    handshake_with_client(&mut client)
        .await
        .context("handshake with client failed")?;

    // Perform active RTMP handshake with upstream
    handshake_with_upstream(&mut upstream)
        .await
        .context("handshake with upstream failed")?;

    // Initial chunk sizes for both directions, per-csid header state, and transparent flag
    let mut c2u_chunk = 128usize; // client -> upstream chunk read size
    let mut u2c_chunk = 128usize; // upstream -> client chunk read size
    let mut c2u_headers = vec![None; 64];
    let mut u2c_headers = vec![None; 64];

    loop {
        // Concurrently read from client or upstream and handle whichever arrives first
        tokio::select! {
            res = read_rtmp_message(&mut client, &mut c2u_chunk, &mut c2u_headers[..]) => {
                let msg = res.context("client read error")?;
                trace!(direction = "c->u", csid = msg.csid, msg_type = msg.msg_type, len = msg.payload.len(), "received message from client");
                // For AMF0 commands, we'll log either the plain command or a single-line rewrite summary
                let cmd_opt = if msg.msg_type == 20 { amf_command_name(&msg.payload).ok() } else { None };

                // Set Chunk Size from client: update local c2u chunk size and forward
                if msg.msg_type == 1 && msg.payload.len() >= 4 {
                    let new_size = u32::from_be_bytes(msg.payload[..4].try_into().unwrap()) as usize;
                    trace!(old_chunk = c2u_chunk, new_chunk = new_size, "client set chunk size");
                    c2u_chunk = new_size;
                    write_rtmp_message(&mut upstream, &msg, c2u_chunk).await.context("failed to forward chunk size to upstream")?;
                    continue;
                }

                // AMF0 commands from client may be rewritten before forwarding upstream
                if msg.msg_type == 20 {
                    if let Some((new_payload, summary)) = rewrite_amf(&msg.payload, app, stream_name)? {
                        let mut m = msg.clone();
                        m.payload = new_payload;
                        write_rtmp_message(&mut upstream, &m, c2u_chunk).await.context("failed to write rewritten AMF to upstream")?;
                        info!("c->u: {}", summary);

                        if is_publish(&msg.payload)? {
                            info!("publish command detected — entering transparent passthrough");
                            tokio::io::copy_bidirectional(&mut client, &mut upstream).await.context("transparent copy error")?;
                            info!("transparent mode ended");
                            return Ok(());
                        }
                        continue;
                    } else {
                        if let Some(cmd) = cmd_opt {
                            debug!("c->u: {}", cmd);
                        }
                    }
                }

                // Default: forward client message to upstream
                write_rtmp_message(&mut upstream, &msg, c2u_chunk).await.context("failed to forward message to upstream")?;
            }
            res = read_rtmp_message(&mut upstream, &mut u2c_chunk, &mut u2c_headers[..]) => {
                let msg = res.context("upstream read error")?;
                trace!(direction = "u->c", csid = msg.csid, msg_type = msg.msg_type, len = msg.payload.len(), "received message from upstream");
                if msg.msg_type == 20
                    && let Ok(cmd) = amf_command_name(&msg.payload) {
                        debug!("u->c: {}", cmd);
                    }

                // If upstream sets chunk size, update u2c_chunk and forward
                if msg.msg_type == 1 && msg.payload.len() >= 4 {
                    let new_size = u32::from_be_bytes(msg.payload[..4].try_into().unwrap()) as usize;
                    trace!(old_chunk = u2c_chunk, new_chunk = new_size, "upstream set chunk size");
                    u2c_chunk = new_size;
                    write_rtmp_message(&mut client, &msg, u2c_chunk).await.context("failed to forward chunk size to client")?;
                    continue;
                }

                // Forward upstream messages back to client
                write_rtmp_message(&mut client, &msg, u2c_chunk).await.context("failed to forward message to client")?;
            }
        }
    }
}
