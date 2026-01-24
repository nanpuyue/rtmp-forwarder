use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use clap::Parser;
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
        tracing_subscriber::EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
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

    // Perform RTMP handshake between client and upstream
    handshake(&mut client, &mut upstream).await?;

    // Initial chunk sizes for both directions, per-csid header state, and transparent flag
    let mut c2u_chunk = 128usize; // client -> upstream chunk read size
    let mut u2c_chunk = 128usize; // upstream -> client chunk read size
    let mut c2u_headers = vec![None; 64];
    let mut u2c_headers = vec![None; 64];

    loop {
        // Concurrently read from client or upstream and handle whichever arrives first
        tokio::select! {
            res = read_rtmp_message(&mut client, &mut c2u_chunk, &mut c2u_headers[..]) => {
                let msg = res?;
                debug!(direction = "c->u", csid = msg.csid, msg_type = msg.msg_type, len = msg.payload.len(), "received message from client");
                // For AMF0 commands, we'll log either the plain command or a single-line rewrite summary
                let cmd_opt = if msg.msg_type == 20 { amf_command_name(&msg.payload).ok() } else { None };

                // Set Chunk Size from client: update local c2u chunk size and forward
                if msg.msg_type == 1 && msg.payload.len() >= 4 {
                    let new_size = u32::from_be_bytes(msg.payload[..4].try_into().unwrap()) as usize;
                    debug!(old_chunk = c2u_chunk, new_chunk = new_size, "client set chunk size");
                    c2u_chunk = new_size;
                    write_rtmp_message(&mut upstream, &msg, c2u_chunk).await?;
                    continue;
                }

                // AMF0 commands from client may be rewritten before forwarding upstream
                if msg.msg_type == 20 {
                    if let Some((new_payload, summary)) = rewrite_amf(&msg.payload, app, stream_name)? {
                        let mut m = msg.clone();
                        m.payload = new_payload;
                        write_rtmp_message(&mut upstream, &m, c2u_chunk).await?;
                        info!("Client -> Upstream： {}", summary);

                        if is_publish(&msg.payload)? {
                            info!("publish command detected — entering transparent passthrough");
                            tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
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
                write_rtmp_message(&mut upstream, &msg, c2u_chunk).await?;
            }
            res = read_rtmp_message(&mut upstream, &mut u2c_chunk, &mut u2c_headers[..]) => {
                let msg = res?;
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
                    write_rtmp_message(&mut client, &msg, u2c_chunk).await?;
                    continue;
                }

                // Forward upstream messages back to client
                write_rtmp_message(&mut client, &msg, u2c_chunk).await?;
            }
        }
    }
}

/* ================= handshake ================= */

async fn handshake(client: &mut TcpStream, upstream: &mut TcpStream) -> Result<()> {
    // RTMP handshake consists of C0+C1, S0+S1+S2, C2 exchanges.
    debug!("starting handshake: relaying C0+C1");
    let mut c0c1 = vec![0u8; 1 + HANDSHAKE_SIZE];
    client.read_exact(&mut c0c1).await?;
    trace!(bytes = c0c1.len(), "read C0+C1 from client");
    upstream.write_all(&c0c1).await?;

    debug!("relaying S0+S1+S2 from upstream to client");
    let mut s0s1s2 = vec![0u8; 1 + HANDSHAKE_SIZE * 2];
    upstream.read_exact(&mut s0s1s2).await?;
    trace!(bytes = s0s1s2.len(), "read S0+S1+S2 from upstream");
    client.write_all(&s0s1s2).await?;

    debug!("relaying C2 from client to upstream");
    let mut c2 = vec![0u8; HANDSHAKE_SIZE];
    client.read_exact(&mut c2).await?;
    trace!(bytes = c2.len(), "read C2 from client");
    upstream.write_all(&c2).await?;
    debug!("handshake complete");
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
            last_headers[csid] = Some(RtmpHeader { msg_len, msg_type, stream_id });
            (msg_len, msg_type, stream_id)
        }
        1 => {
            // fmt1: 7-byte header (timestamp delta[3], msg_len[3], msg_type[1])
            let mut mh = [0u8; 7];
            s.read_exact(&mut mh).await?;
            let msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
            let msg_type = mh[6];
            // stream_id unchanged from last header
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing previous header for fmt1"))?;
            let stream_id = last.stream_id;
            last_headers[csid] = Some(RtmpHeader { msg_len, msg_type, stream_id });
            (msg_len, msg_type, stream_id)
        }
        2 => {
            // fmt2: 3-byte header (timestamp delta[3]) — other fields unchanged
            let mut mh = [0u8; 3];
            s.read_exact(&mut mh).await?;
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing previous header for fmt2"))?;
            (last.msg_len, last.msg_type, last.stream_id)
        }
        3 => {
            // fmt3: no header — reuse last header entirely
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing previous header for fmt3"))?;
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

    trace!(csid = csid, msg_type = msg_type, len = msg_len, "decoded RTMP message");

    Ok(RtmpMessage {
        csid: csid as u8,
        msg_type,
        stream_id,
        payload,
    })
}

async fn write_rtmp_message(
    s: &mut TcpStream,
    msg: &RtmpMessage,
    chunk_size: usize,
) -> Result<()> {
    let mut h = BytesMut::new();
    h.put_u8(msg.csid);
    h.put_u24(0);
    h.put_u24(msg.payload.len() as u32);
    h.put_u8(msg.msg_type);
    h.put_u32_le(msg.stream_id);
    s.write_all(&h).await?;
    debug!(csid = msg.csid, msg_type = msg.msg_type, len = msg.payload.len(), "wrote RTMP header");
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
    debug!(csid = msg.csid, msg_type = msg.msg_type, len = msg.payload.len(), "wrote RTMP message");
    Ok(())
}

/* ================= AMF0 ================= */

fn rewrite_amf(
    payload: &[u8],
    app: &str,
    stream: &str,
) -> Result<Option<(BytesMut, String)>> {
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
            0x02 => v.push((key, Amf0::String(amf_read_string(p)?))),
            0x00 => v.push((key, Amf0::Number(amf_read_number(p)?))),
            _ => return Err(anyhow!("unsupported amf type")),
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
