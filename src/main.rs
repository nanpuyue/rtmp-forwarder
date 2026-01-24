#![feature(random)]

use anyhow::{Context, Result, anyhow};
use bytes::{BufMut, BytesMut};
use clap::Parser;
use std::iter::Iterator;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, error, info, trace};
use tracing_subscriber;

const HANDSHAKE_SIZE: usize = 1536;

/* ================= args ================= */

/// CLI: support `-l`/`--listen` and `-u`/`--upstream`.
#[derive(Parser, Debug)]
#[command(about = "RTMP forwarder proxy")]
struct Cli {
    /// Local listen address (default 127.0.0.1:1935)
    #[arg(short = 'l', long = "listen", default_value = "127.0.0.1:1935")]
    listen: String,

    /// Upstream RTMP URL (rtmp://host[:port]/app/stream)
    #[arg(short = 'u', long = "upstream")]
    upstream: String,
    /// Logging level (eg. info, debug). If omitted, defaults to info. Can be overridden by RUST_LOG env.
    #[arg(long = "log")]
    log: Option<String>,
}

fn parse_rtmp_url(url: &str) -> Result<(String, String, String)> {
    if !url.starts_with("rtmp://") {
        return Err(anyhow!("invalid rtmp url"));
    }

    let s = &url[7..];
    let mut parts = s.splitn(2, '/');

    let host = parts.next().ok_or_else(|| anyhow!("missing host"))?;
    let host = if host.contains(':') {
        host.to_string()
    } else {
        format!("{}:1935", host)
    };

    let path = parts.next().ok_or_else(|| anyhow!("missing app"))?;
    let mut p = path.split('/');

    let app = p.next().unwrap().to_string();
    let stream = p.collect::<Vec<_>>().join("/");

    Ok((host, app, stream))
}

/* ================= main ================= */

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI using clap
    let cli = Cli::parse();

    // Initialize tracing subscriber: prefer CLI `--log` if provided, otherwise use RUST_LOG env, otherwise default to `info`
    let env_filter = if let Some(ref lvl) = cli.log {
        tracing_subscriber::EnvFilter::new(lvl.as_str())
    } else {
        tracing_subscriber::EnvFilter::try_from_env("RUST_LOG")
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
    let (upstream_addr, app, stream) = parse_rtmp_url(&cli.upstream)?;

    // Bind the listening socket and log the configured endpoints
    let listener = TcpListener::bind(&cli.listen).await?;
    info!("listening on: {}", cli.listen);
    info!("forward to: {}", cli.upstream);

    loop {
        let (client, peer) = listener.accept().await?;
        let upstream = upstream_addr.clone();
        let app = app.clone();
        let stream = stream.clone();

        tokio::spawn(async move {
            info!("Received connection from client: {}", peer);
            if let Err(e) = handle_client(client, &upstream, &app, &stream).await {
                error!(error = %e, "connection error");
            }
        });
    }
}

/* ================= proxy ================= */

async fn handle_client(
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
                debug!(direction = "c->u", csid = msg.csid, msg_type = msg.msg_type, len = msg.payload.len(), "received message from client");
                // For AMF0 commands, we'll log either the plain command or a single-line rewrite summary
                let cmd_opt = if msg.msg_type == 20 { amf_command_name(&msg.payload).ok() } else { None };

                // Set Chunk Size from client: update local c2u chunk size and forward
                if msg.msg_type == 1 && msg.payload.len() >= 4 {
                    let new_size = u32::from_be_bytes(msg.payload[..4].try_into().unwrap()) as usize;
                    debug!(old_chunk = c2u_chunk, new_chunk = new_size, "client set chunk size");
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
                        info!("Client -> Upstream： {}", summary);

                        if is_publish(&msg.payload)? {
                            info!("publish command detected — entering transparent passthrough");
                            tokio::io::copy_bidirectional(&mut client, &mut upstream).await.context("transparent copy error")?;
                            info!("transparent mode ended");
                            return Ok(());
                        }
                        continue;
                    } else {
                        if let Some(cmd) = cmd_opt {
                            info!("Client -> Upstream: {}", cmd);
                        }
                    }
                }

                // Default: forward client message to upstream
                write_rtmp_message(&mut upstream, &msg, c2u_chunk).await.context("failed to forward message to upstream")?;
            }
            res = read_rtmp_message(&mut upstream, &mut u2c_chunk, &mut u2c_headers[..]) => {
                let msg = res.context("upstream read error")?;
                debug!(direction = "u->c", csid = msg.csid, msg_type = msg.msg_type, len = msg.payload.len(), "received message from upstream");
                if msg.msg_type == 20 {
                    if let Ok(cmd) = amf_command_name(&msg.payload) {
                        info!("Upstream -> Client: {}", cmd);
                    }
                }

                // If upstream sets chunk size, update u2c_chunk and forward
                if msg.msg_type == 1 && msg.payload.len() >= 4 {
                    let new_size = u32::from_be_bytes(msg.payload[..4].try_into().unwrap()) as usize;
                    debug!(old_chunk = u2c_chunk, new_chunk = new_size, "upstream set chunk size");
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

/* ================= handshake ================= */

// Generate random bytes using std::random (Nightly)
fn gen_random_buffer(buf: &mut [u8]) {
    // Requires #![feature(random)]
    for b in buf.iter_mut() {
        *b = std::random::random(..);
    }
}

async fn handshake_with_client(client: &mut TcpStream) -> Result<()> {
    // RTMP Server Handshake
    // 1. Read C0
    let mut c0 = [0u8; 1];
    client.read_exact(&mut c0).await.context("read C0")?;
    let version = c0[0];
    trace!(version = version, "read C0 from client");

    // 2. Read C1
    let mut c1 = vec![0u8; HANDSHAKE_SIZE];
    client.read_exact(&mut c1).await.context("read C1")?;
    trace!("read C1 from client");

    // 3. Write S0 + S1 + S2
    // S0 = version (usually 3)
    // S1 = time(4) + zero(4) + random(1528)
    // S2 = c1_time(4) + c1_time2(4) + c1_random(1528) -> basically Echo of C1
    let mut s0s1s2 = Vec::with_capacity(1 + HANDSHAKE_SIZE * 2);
    s0s1s2.push(version); // S0

    // S1 construction
    let mut s1 = vec![0u8; HANDSHAKE_SIZE];
    // time (4 bytes) - mostly irrelevant for handshake but good to have
    let uptime = 0u32;
    s1[0..4].copy_from_slice(&uptime.to_be_bytes());
    // next 4 bytes are zero
    s1[4..8].copy_from_slice(&[0, 0, 0, 0]);
    // random fill
    gen_random_buffer(&mut s1[8..]);
    s0s1s2.extend_from_slice(&s1);

    // S2 construction = C1 (Echo)
    s0s1s2.extend_from_slice(&c1);

    client.write_all(&s0s1s2).await.context("write S0S1S2")?;
    debug!("sent S0+S1+S2 to client");

    // 4. Read C2
    let mut c2 = vec![0u8; HANDSHAKE_SIZE];
    client.read_exact(&mut c2).await.context("read C2")?;
    trace!("read C2 from client");

    // In a strict server, we'd verify C2 matches S1, but for a proxy we generally accept.
    debug!("handshake with client complete");
    Ok(())
}

async fn handshake_with_upstream(upstream: &mut TcpStream) -> Result<()> {
    // RTMP Client Handshake
    // 1. Write C0 + C1
    let mut c0c1 = Vec::with_capacity(1 + HANDSHAKE_SIZE);
    c0c1.push(3); // C0: version 3

    let mut c1 = vec![0u8; HANDSHAKE_SIZE];
    // time (4 bytes)
    let uptime = 0u32;
    c1[0..4].copy_from_slice(&uptime.to_be_bytes());
    // zero (4 bytes)
    c1[4..8].copy_from_slice(&[0, 0, 0, 0]);
    // random
    gen_random_buffer(&mut c1[8..]);
    c0c1.extend_from_slice(&c1);

    upstream
        .write_all(&c0c1)
        .await
        .context("write C0C1 to upstream")?;
    debug!("sent C0+C1 to upstream");

    // 2. Read S0 + S1 + S2
    let mut s0 = [0u8; 1];
    upstream.read_exact(&mut s0).await.context("read S0")?;

    let mut s1 = vec![0u8; HANDSHAKE_SIZE];
    upstream.read_exact(&mut s1).await.context("read S1")?;

    let mut s2 = vec![0u8; HANDSHAKE_SIZE];
    upstream.read_exact(&mut s2).await.context("read S2")?;
    trace!("read S0+S1+S2 from upstream");

    // 3. Write C2 (Echo of S1)
    upstream
        .write_all(&s1)
        .await
        .context("write C2 to upstream")?;
    debug!("sent C2 to upstream");

    debug!("handshake with upstream complete");
    Ok(())
}

/* ================= RTMP ================= */

#[derive(Clone)]
struct RtmpMessage {
    csid: u8,
    msg_type: u8,
    stream_id: u32,
    payload: BytesMut,
}

#[derive(Clone)]
struct RtmpHeader {
    msg_len: usize,
    msg_type: u8,
    stream_id: u32,
}

async fn read_rtmp_message(
    s: &mut TcpStream,
    chunk_size: &mut usize,
    last_headers: &mut [Option<RtmpHeader>],
) -> Result<RtmpMessage> {
    let mut bh = [0u8; 1];
    s.read_exact(&mut bh).await?;
    // Basic header: fmt (2 bits) and csid (6 bits)
    let fmt = bh[0] >> 6;
    let csid = (bh[0] & 0x3f) as usize;
    // Note: extended csid values (0 or 1) are not handled here; assume single-byte csid

    // Decode the message header depending on fmt (0..3)
    let (msg_len, msg_type, stream_id) = match fmt {
        0 => {
            // fmt0: 11-byte header (timestamp[3], msg_len[3], msg_type[1], stream_id[4 little-endian])
            let mut mh = [0u8; 11];
            s.read_exact(&mut mh).await?;
            let msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
            let msg_type = mh[6];
            let stream_id = u32::from_le_bytes([mh[7], mh[8], mh[9], mh[10]]);
            // store as last header for this csid
            last_headers[csid] = Some(RtmpHeader {
                msg_len,
                msg_type,
                stream_id,
            });
            (msg_len, msg_type, stream_id)
        }
        1 => {
            // fmt1: 7-byte header (timestamp delta[3], msg_len[3], msg_type[1])
            let mut mh = [0u8; 7];
            s.read_exact(&mut mh).await?;
            let msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
            let msg_type = mh[6];
            // stream_id unchanged from last header
            let last = last_headers[csid]
                .as_ref()
                .ok_or_else(|| anyhow!("missing previous header for fmt1"))?;
            let stream_id = last.stream_id;
            last_headers[csid] = Some(RtmpHeader {
                msg_len,
                msg_type,
                stream_id,
            });
            (msg_len, msg_type, stream_id)
        }
        2 => {
            // fmt2: 3-byte header (timestamp delta[3]) — other fields unchanged
            let mut mh = [0u8; 3];
            s.read_exact(&mut mh).await?;
            let last = last_headers[csid]
                .as_ref()
                .ok_or_else(|| anyhow!("missing previous header for fmt2"))?;
            (last.msg_len, last.msg_type, last.stream_id)
        }
        3 => {
            // fmt3: no header — reuse last header entirely
            let last = last_headers[csid]
                .as_ref()
                .ok_or_else(|| anyhow!("missing previous header for fmt3"))?;
            (last.msg_len, last.msg_type, last.stream_id)
        }
        _ => return Err(anyhow!("invalid fmt")),
    };

    // Read payload in chunk-sized pieces
    let mut payload = BytesMut::with_capacity(msg_len);
    while payload.len() < msg_len {
        let n = (*chunk_size).min(msg_len - payload.len());
        let mut buf = vec![0u8; n];
        s.read_exact(&mut buf).await?;
        payload.extend_from_slice(&buf);

        // If message spans multiple chunks, read the subsequent basic header byte (fmt3)
        if payload.len() < msg_len {
            s.read_exact(&mut bh).await?; // fmt3
        }
    }

    trace!(
        csid = csid,
        msg_type = msg_type,
        len = msg_len,
        "decoded RTMP message"
    );

    Ok(RtmpMessage {
        csid: csid as u8,
        msg_type,
        stream_id,
        payload,
    })
}

async fn write_rtmp_message(s: &mut TcpStream, msg: &RtmpMessage, chunk_size: usize) -> Result<()> {
    let mut h = BytesMut::new();
    h.put_u8(msg.csid);
    h.put_u24(0);
    h.put_u24(msg.payload.len() as u32);
    h.put_u8(msg.msg_type);
    h.put_u32_le(msg.stream_id);
    s.write_all(&h).await?;
    debug!(
        csid = msg.csid,
        msg_type = msg.msg_type,
        len = msg.payload.len(),
        "wrote RTMP header"
    );
    // Write payload in chunk-sized fragments, writing fmt3 basic headers between chunks
    let mut off = 0;
    while off < msg.payload.len() {
        let end = (off + chunk_size).min(msg.payload.len());
        s.write_all(&msg.payload[off..end]).await?;
        off = end;
        if off < msg.payload.len() {
            // Subsequent chunk header: fmt=3, csid in low 6 bits
            s.write_all(&[msg.csid | 0b1100_0000]).await?;
        }
    }
    debug!(
        csid = msg.csid,
        msg_type = msg.msg_type,
        len = msg.payload.len(),
        "wrote RTMP message"
    );
    Ok(())
}

/* ================= AMF0 ================= */

fn rewrite_amf(payload: &[u8], app: &str, stream: &str) -> Result<Option<(BytesMut, String)>> {
    // Attempt to read AMF0 command name and possibly rewrite it.
    let mut p = payload;
    let cmd = amf_read_string(&mut p)?;
    debug!(original_command = %cmd, "AMF0 command received");

    match cmd.as_str() {
        "connect" => {
            // connect(tx, object)
            let tx = amf_read_number(&mut p)?;
            let mut obj = amf_read_object(&mut p)?;
            // find existing app value if present
            let mut old_app: Option<String> = None;
            let mut found = false;
            for (k, v) in obj.iter_mut() {
                if k == "app" {
                    if let Amf0::String(s) = v.clone() {
                        old_app = Some(s);
                    }
                    *v = Amf0::String(app.into());
                    found = true;
                    break;
                }
            }
            if !found {
                obj.push(("app".into(), Amf0::String(app.into())));
            }

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "connect");
            amf_write_number(&mut out, tx);
            amf_write_object(&mut out, &obj);
            let old = old_app.unwrap_or_else(|| "".into());
            let summary = format!("rewrote app \"{}\" to \"{}\"", old, app);
            Ok(Some((out, summary)))
        }
        "publish" => {
            // publish(tx, null, name)
            let tx = amf_read_number(&mut p)?;
            amf_read_null(&mut p)?;
            let orig_stream = amf_read_string(&mut p)?; // original stream name

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "publish");
            amf_write_number(&mut out, tx);
            amf_write_null(&mut out);
            amf_write_string(&mut out, stream);
            let summary = format!("rewrote stream \"{}\" to \"{}\"", orig_stream, stream);
            Ok(Some((out, summary)))
        }
        "releaseStream" => {
            // releaseStream may come as (tx, name) or (tx, null, name) or (name)
            let mut p2 = p;
            let mut tx_opt: Option<f64> = None;
            if !p2.is_empty() && p2[0] == 0x00 {
                tx_opt = Some(amf_read_number(&mut p2)?);
            }
            let mut had_null = false;
            if !p2.is_empty() && p2[0] == 0x05 {
                amf_read_null(&mut p2)?;
                had_null = true;
            }
            let orig_stream = if !p2.is_empty() && p2[0] == 0x02 {
                amf_read_string(&mut p2)?
            } else {
                String::new()
            };

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "releaseStream");
            if let Some(tx) = tx_opt {
                amf_write_number(&mut out, tx);
            }
            if had_null {
                amf_write_null(&mut out);
            }
            amf_write_string(&mut out, stream);
            let summary = format!("rewrote stream \"{}\" to \"{}\"", orig_stream, stream);
            Ok(Some((out, summary)))
        }
        "FCPublish" => {
            // FCPublish may be sent as (tx, name) or (name) or (tx, null, name)
            let mut p2 = p;
            let mut tx_opt: Option<f64> = None;
            if !p2.is_empty() && p2[0] == 0x00 {
                tx_opt = Some(amf_read_number(&mut p2)?);
            }
            let mut had_null = false;
            if !p2.is_empty() && p2[0] == 0x05 {
                amf_read_null(&mut p2)?;
                had_null = true;
            }
            let orig_stream = if !p2.is_empty() && p2[0] == 0x02 {
                amf_read_string(&mut p2)?
            } else {
                String::new()
            };

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "FCPublish");
            if let Some(tx) = tx_opt {
                amf_write_number(&mut out, tx);
            }
            if had_null {
                amf_write_null(&mut out);
            }
            amf_write_string(&mut out, stream);
            let summary = format!("rewrote stream \"{}\" to \"{}\"", orig_stream, stream);
            Ok(Some((out, summary)))
        }
        _ => {
            debug!(command = %cmd, "no rewrite performed for AMF0 command");
            Ok(None)
        }
    }
}

fn is_publish(payload: &[u8]) -> Result<bool> {
    let mut p = payload;
    let cmd = amf_read_string(&mut p)?;
    trace!(command = %cmd, "is_publish check");
    Ok(cmd == "publish")
}

// Peek AMF0 command name without mutating the original slice
fn amf_command_name(payload: &[u8]) -> Result<String> {
    let mut tmp = payload;
    let cmd = amf_read_string(&mut tmp)?;
    Ok(cmd)
}

/* ================= AMF0 helpers ================= */

#[derive(Clone)]
enum Amf0 {
    String(String),
    Number(f64),
    Boolean(bool),
}

fn amf_read_string(p: &mut &[u8]) -> Result<String> {
    // AMF0 string: type 0x02, 2-byte big-endian length, then utf8 bytes
    if p.is_empty() || p[0] != 0x02 {
        return Err(anyhow!("not string"));
    }
    let len = u16::from_be_bytes([p[1], p[2]]) as usize;
    let s = std::str::from_utf8(&p[3..3 + len])?.to_string();
    trace!(value = %s, len = len, "amf_read_string");
    *p = &p[3 + len..];
    Ok(s)
}

fn amf_read_number(p: &mut &[u8]) -> Result<f64> {
    // AMF0 number: type 0x00 followed by 8-byte big-endian IEEE-754
    if p.is_empty() || p[0] != 0x00 {
        return Err(anyhow!("not number"));
    }
    let v = f64::from_be_bytes(p[1..9].try_into().unwrap());
    trace!(value = v, "amf_read_number");
    *p = &p[9..];
    Ok(v)
}

fn amf_read_boolean(p: &mut &[u8]) -> Result<bool> {
    // AMF0 boolean: type 0x01 followed by 1 byte (0=false, !=0=true)
    if p.is_empty() || p[0] != 0x01 {
        return Err(anyhow!("not boolean"));
    }
    let v = p[1] != 0;
    trace!(value = v, "amf_read_boolean");
    *p = &p[2..];
    Ok(v)
}

fn amf_read_null(p: &mut &[u8]) -> Result<()> {
    if p.is_empty() || p[0] != 0x05 {
        return Err(anyhow!("not null"));
    }
    trace!("amf_read_null");
    *p = &p[1..];
    Ok(())
}

fn amf_read_object(p: &mut &[u8]) -> Result<Vec<(String, Amf0)>> {
    // AMF0 object: 0x03 marker, then repeated key-value pairs, terminated by 0x00 0x00 0x09
    if p.is_empty() || p[0] != 0x03 {
        return Err(anyhow!("not object"));
    }
    *p = &p[1..];
    let mut v = Vec::new();
    loop {
        if p.len() >= 3 && p[..3] == [0, 0, 9] {
            *p = &p[3..];
            break;
        }
        let klen = u16::from_be_bytes([p[0], p[1]]) as usize;
        let key = std::str::from_utf8(&p[2..2 + klen])?.to_string();
        *p = &p[2 + klen..];
        match p[0] {
            0x00 => v.push((key, Amf0::Number(amf_read_number(p)?))),
            0x01 => v.push((key, Amf0::Boolean(amf_read_boolean(p)?))),
            0x02 => v.push((key, Amf0::String(amf_read_string(p)?))),
            _ => return Err(anyhow!("unsupported amf type: {}", p[0])),
        }
    }
    trace!(pairs = v.len(), "amf_read_object parsed key-value pairs");
    Ok(v)
}

fn amf_write_string(b: &mut BytesMut, s: &str) {
    b.put_u8(0x02);
    b.put_u16(s.len() as u16);
    b.extend_from_slice(s.as_bytes());
}
fn amf_write_number(b: &mut BytesMut, n: f64) {
    b.put_u8(0x00);
    b.extend_from_slice(&n.to_be_bytes());
}
fn amf_write_boolean(b: &mut BytesMut, v: bool) {
    b.put_u8(0x01);
    b.put_u8(if v { 1 } else { 0 });
}
fn amf_write_null(b: &mut BytesMut) {
    b.put_u8(0x05);
}
fn amf_write_object(b: &mut BytesMut, o: &[(String, Amf0)]) {
    b.put_u8(0x03);
    for (k, v) in o {
        b.put_u16(k.len() as u16);
        b.extend_from_slice(k.as_bytes());
        match v {
            Amf0::String(s) => amf_write_string(b, s),
            Amf0::Number(n) => amf_write_number(b, *n),
            Amf0::Boolean(flag) => amf_write_boolean(b, *flag),
        }
    }
    b.extend_from_slice(&[0, 0, 9]);
}

/* ================= misc ================= */

trait PutU24 {
    fn put_u24(&mut self, v: u32);
}
impl PutU24 for BytesMut {
    fn put_u24(&mut self, v: u32) {
        self.put_u8(((v >> 16) & 0xff) as u8);
        self.put_u8(((v >> 8) & 0xff) as u8);
        self.put_u8((v & 0xff) as u8);
    }
}
