use anyhow::Result;
use bytes::{BufMut, BytesMut};
use tokio::io::AsyncWriteExt;



#[derive(Clone, Debug)]
pub struct RtmpMessageHeader {
    pub timestamp: u32,
    pub msg_type: u8,
    pub stream_id: u32,
}

#[derive(Clone, Debug)]
pub struct RtmpMessage {
    pub csid: u8,
    pub header: RtmpMessageHeader,
    pub payload: BytesMut,
}

pub async fn write_rtmp_message<S>(s: &mut S, msg: &RtmpMessage, chunk_size: usize) -> Result<()> 
where S: tokio::io::AsyncWrite + Unpin
{
    let mut h = BytesMut::new();
    h.put_u8(msg.csid); // fmt=0, csid
    
    let ts_24 = if msg.header.timestamp >= 0xffffff { 0xffffff } else { msg.header.timestamp };
    h.put_u24(ts_24);
    h.put_u24(msg.payload.len() as u32);
    h.put_u8(msg.header.msg_type);
    h.put_u32_le(msg.header.stream_id);
    
    if ts_24 == 0xffffff {
        h.put_u32(msg.header.timestamp);
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
                h3.put_u32(msg.header.timestamp);
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
    write_rtmp_message(s, &RtmpMessage {csid, header: RtmpMessageHeader{timestamp: 0, msg_type: 20, stream_id}, payload }, chunk_size).await
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
