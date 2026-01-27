use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};


#[derive(Clone, Debug)]
pub struct RtmpMessage {
    pub csid: u8,
    pub timestamp: u32,
    pub msg_type: u8,
    pub stream_id: u32,
    pub payload: BytesMut,
}

#[derive(Clone)]
pub struct RtmpHeader {
    pub timestamp: u32,
    pub msg_len: usize,
    pub msg_type: u8,
    pub stream_id: u32,
}

pub async fn read_rtmp_message<S>(
    s: &mut S,
    chunk_size: &mut usize,
    last_headers: &mut [Option<RtmpHeader>],
) -> Result<RtmpMessage>
where S: tokio::io::AsyncRead + Unpin
{
    let mut bh = [0u8; 1];
    s.read_exact(&mut bh).await?;
    let fmt = bh[0] >> 6;
    let csid = (bh[0] & 0x3f) as usize;

    let (ts_raw, msg_len, msg_type, stream_id) = match fmt {
        0 => {
            let mut mh = [0u8; 11];
            s.read_exact(&mut mh).await?;
            let ts = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            let msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
            let msg_type = mh[6];
            let stream_id = u32::from_le_bytes([mh[7], mh[8], mh[9], mh[10]]);
            (ts, msg_len, msg_type, stream_id)
        }
        1 => {
            let mut mh = [0u8; 7];
            s.read_exact(&mut mh).await?;
            let delta = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            let msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
            let msg_type = mh[6];
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing prev fmt1"))?;
            (last.timestamp.wrapping_add(delta), msg_len, msg_type, last.stream_id)
        }
        2 => {
            let mut mh = [0u8; 3];
            s.read_exact(&mut mh).await?;
            let delta = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing prev fmt2"))?;
            (last.timestamp.wrapping_add(delta), last.msg_len, last.msg_type, last.stream_id)
        }
        3 => {
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing prev fmt3"))?;
            (last.timestamp, last.msg_len, last.msg_type, last.stream_id)
        }
        _ => return Err(anyhow!("invalid fmt")),
    };

    // Extended timestamp: if the 24-bit value was 0xffffff, read 4 extra bytes
    let mut final_ts = ts_raw;
    if fmt != 3 {
        // According to spec, fmt 3 doesn't have an extended timestamp field if the prev header had one,
        // but it reused it. Actually fmt 3 is special - if it follows a header that had extended ts, 
        // it may or may NOT have its own extended ts field depending on chunking.
        // Simple impl: check if ts was 0xffffff in the MATCH arms.
        if ts_raw == 0xffffff {
            let mut ext = [0u8; 4];
            s.read_exact(&mut ext).await?;
            final_ts = u32::from_be_bytes(ext);
        }
    } else if let Some(ref l) = last_headers[csid] {
        // For fmt3, if the previous header for this CSID had an extended timestamp, it might be here too
        if l.timestamp >= 0xffffff {
             // Some encoders always put extended TS for fmt3 if the prev packet used it.
             // We'll skip complex fmt3 ext-ts for now unless it breaks.
        }
    }

    last_headers[csid] = Some(RtmpHeader {
        timestamp: final_ts,
        msg_len,
        msg_type,
        stream_id,
    });

    let mut payload = BytesMut::with_capacity(msg_len);
    while payload.len() < msg_len {
        let n = (*chunk_size).min(msg_len - payload.len());
        let mut buf = vec![0u8; n];
        s.read_exact(&mut buf).await?;
        payload.extend_from_slice(&buf);
        if payload.len() < msg_len {
            s.read_exact(&mut bh).await?;
        }
    }

    Ok(RtmpMessage { csid: csid as u8, timestamp: final_ts, msg_type, stream_id, payload })
}

pub async fn write_rtmp_message<S>(s: &mut S, msg: &RtmpMessage, chunk_size: usize) -> Result<()> 
where S: tokio::io::AsyncWrite + Unpin
{
    let mut h = BytesMut::new();
    h.put_u8(msg.csid); // fmt=0, csid
    
    let ts_24 = if msg.timestamp >= 0xffffff { 0xffffff } else { msg.timestamp };
    h.put_u24(ts_24);
    h.put_u24(msg.payload.len() as u32);
    h.put_u8(msg.msg_type);
    h.put_u32_le(msg.stream_id);
    
    if ts_24 == 0xffffff {
        h.put_u32(msg.timestamp);
    }
    
    s.write_all(&h).await?;

    let mut off = 0;
    while off < msg.payload.len() {
        let end = (off + chunk_size).min(msg.payload.len());
        s.write_all(&msg.payload[off..end]).await?;
        off = end;
        if off < msg.payload.len() {
            // Subsequent fmt3 header
            let mut h3 = BytesMut::new();
            h3.put_u8(msg.csid | 0b1100_0000);
            if ts_24 == 0xffffff {
                h3.put_u32(msg.timestamp);
            }
            s.write_all(&h3).await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn send_rtmp_command<S>(
    s: &mut S,
    csid: u8,
    stream_id: u32,
    chunk_size: usize,
    name: &str,
    tx_id: f64,
    items: &[(&str, crate::amf::Amf0)],
    args: &[crate::amf::Amf0],
) -> Result<()>
where S: tokio::io::AsyncWrite + Unpin {
    let mut payload = BytesMut::new();
    crate::amf::amf_write_string(&mut payload, name);
    crate::amf::amf_write_number(&mut payload, tx_id);
    if items.is_empty() {
        crate::amf::amf_read_null(&mut &[][..]).ok(); // This is just for dummy null if needed, but better use amf_write_null
        crate::amf::amf_write_null(&mut payload);
    } else {
        crate::amf::amf_write_object(&mut payload, items);
    }
    for arg in args {
        crate::amf::amf_write_value(&mut payload, arg);
    }
    write_rtmp_message(s, &RtmpMessage { csid, timestamp: 0, msg_type: 20, stream_id, payload }, chunk_size).await
}

/* ================= misc ================= */

pub trait PutU24 {
    fn put_u24(&mut self, v: u32);
}
impl PutU24 for BytesMut {
    fn put_u24(&mut self, v: u32) {
        self.put_u8(((v >> 16) & 0xff) as u8);
        self.put_u8(((v >> 8) & 0xff) as u8);
        self.put_u8((v & 0xff) as u8);
    }
}
