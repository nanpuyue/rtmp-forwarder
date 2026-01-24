use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::trace;

#[derive(Clone)]
pub struct RtmpMessage {
    pub csid: u8,
    pub msg_type: u8,
    pub stream_id: u32,
    pub payload: BytesMut,
}

#[derive(Clone)]
pub struct RtmpHeader {
    pub msg_len: usize,
    pub msg_type: u8,
    pub stream_id: u32,
}

pub async fn read_rtmp_message(
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

pub async fn write_rtmp_message(s: &mut TcpStream, msg: &RtmpMessage, chunk_size: usize) -> Result<()> {
    let mut h = BytesMut::new();
    h.put_u8(msg.csid);
    h.put_u24(0);
    h.put_u24(msg.payload.len() as u32);
    h.put_u8(msg.msg_type);
    h.put_u32_le(msg.stream_id);
    s.write_all(&h).await?;
    trace!(
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
    trace!(
        csid = msg.csid,
        msg_type = msg.msg_type,
        len = msg.payload.len(),
        "wrote RTMP message"
    );
    Ok(())
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
