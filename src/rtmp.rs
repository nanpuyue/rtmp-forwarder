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
    pub ts_raw: u32,
    pub msg_len: usize,
    pub msg_type: u8,
    pub stream_id: u32,
}

/// 读取单个 RTMP chunk
/// payload 为上一条消息未完成的 buffer，如果读取完一条消息就 take 出来返回 Some(Bytes)
pub async fn read_rtmp_chunk<S>(
    s: &mut S,
    chunk_size: &mut usize,
    payload: &mut Option<BytesMut>,
    last_timestamp: &mut u32,
    last_headers: &mut [Option<RtmpHeader>],
) -> Result<Option<RtmpMessage>>
where
    S: AsyncReadExt + Unpin,
{
    // --- Step 1: 读取基本 chunk header ---
    let mut bh = [0u8; 1];
    s.read_exact(&mut bh).await?;
    let fmt = bh[0] >> 6;
    let csid = (bh[0] & 0x3f) as usize;
    
    // --- Step 2: 解析消息头 ---
    let (mut ts_raw, msg_len, msg_type, stream_id) = match fmt {
        0 => {
            let mut mh = [0u8; 11];
            s.read_exact(&mut mh).await?;
            let ts = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            *last_timestamp = ts;
            let msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
            let msg_type = mh[6];
            let stream_id = u32::from_le_bytes([mh[7], mh[8], mh[9], mh[10]]);
            (ts, msg_len, msg_type, stream_id)
        }
        1 => {
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing prev fmt1"))?;
            let mut mh = [0u8; 7];
            s.read_exact(&mut mh).await?;
            let ts = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            *last_timestamp = last_timestamp.wrapping_add(ts);
            let msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
            let msg_type = mh[6];
            (ts, msg_len, msg_type, last.stream_id)
        }
        2 => {
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing prev fmt2"))?;
            let mut mh = [0u8; 3];
            s.read_exact(&mut mh).await?;
            let ts = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            *last_timestamp = last_timestamp.wrapping_add(ts);
            (ts, last.msg_len, last.msg_type, last.stream_id)
        }
        3 => {
            let last = last_headers[csid].as_ref().ok_or_else(|| anyhow!("missing prev fmt3"))?;
            if payload.is_none() {
                *last_timestamp = last_timestamp.wrapping_add(last.ts_raw);
            }
            (last.ts_raw, last.msg_len, last.msg_type, last.stream_id)
        }
        _ => return Err(anyhow!("invalid fmt")),
    };

    // --- Step 3: 扩展时间戳 ---
    if ts_raw >= 0xFFFFFF {
        let mut ext = [0u8; 4];
        s.read_exact(&mut ext).await?;
        ts_raw = u32::from_be_bytes(ext);
    }

    // --- Step 4: 初始化 payload ---
    if payload.is_none() {
        *payload = Some(BytesMut::with_capacity(msg_len));
    }
    let buf = payload.as_mut().unwrap();

    // --- Step 5: 读取 chunk 数据 ---
    let remaining = msg_len - buf.len();
    let read_len = remaining.min(*chunk_size);
    let mut tmp = vec![0u8; read_len];
    s.read_exact(&mut tmp).await?;
    buf.extend_from_slice(&tmp);

    // --- Step 6: 更新 last_headers ---
    last_headers[csid] = Some(RtmpHeader {
        ts_raw,
        msg_len,
        msg_type,
        stream_id,
    });

    // --- Step 7: 消息是否完整 ---
    if buf.len() == msg_len {
        Ok(Some(RtmpMessage {
                csid: csid as _,
                timestamp: *last_timestamp,
                msg_type: msg_type,
                stream_id: stream_id,
                payload: payload.take().unwrap(),
            }))
    } else {
        Ok(None)
    }
}

/// 读取完整的 RTMP 消息
pub async fn read_rtmp_message<S>(
    s: &mut S,
    chunk_size: &mut usize,
    last_timestamp: &mut u32,
    last_headers: &mut [Option<RtmpHeader>],
) -> Result<RtmpMessage>
where
    S: AsyncReadExt + Unpin,
{
    let mut payload = None;
    loop {
        let chunk = read_rtmp_chunk(s, chunk_size, &mut payload, last_timestamp, last_headers).await?;
        if let Some(x) = chunk {
            // 消息读取完成
            return Ok(x);
        }
    }
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
