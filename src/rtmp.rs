use anyhow::Result;
use bytes::{Bytes, BytesMut};
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::amf::Amf0;
use crate::amf::amf_write_null;
use crate::amf::amf_write_number;
use crate::amf::amf_write_object;
use crate::amf::amf_write_string;
use crate::amf::amf_write_value;
use crate::rtmp_codec::{RtmpMessage, RtmpMessageIter};

pub async fn write_rtmp_message<S>(s: &mut S, msg: &RtmpMessage, chunk_size: usize) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    // chunk size 匹配时原样转发
    if chunk_size == msg.chunk_size {
        for chunk in &msg.chunks {
            s.write_all(&chunk.raw_bytes).await?;
        }
    } else {
        for chunk in RtmpMessageIter::new_with_msg(msg, chunk_size) {
            s.write_all(&chunk.raw_bytes).await?;
        }
    }
    Ok(())
}

pub async fn write_rtmp_message2<S>(
    s: &mut S,
    csid: u8,
    timestamp: u32,
    msg_type: u8,
    stream_id: u32,
    payload: &Bytes,
    chunk_size: usize,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    for chunk in
        RtmpMessageIter::new_with_payload(csid, timestamp, msg_type, stream_id, chunk_size, payload)
    {
        s.write_all(&chunk.raw_bytes).await?;
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
    items: &[(&str, Amf0)],
    args: &[Amf0],
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut payload = BytesMut::new();
    amf_write_string(&mut payload, name);
    amf_write_number(&mut payload, tx_id);

    if items.is_empty() {
        amf_write_null(&mut payload);
    } else {
        amf_write_object(&mut payload, items);
    }

    for arg in args {
        amf_write_value(&mut payload, arg);
    }

    write_rtmp_message2(s, csid, 0, 20, stream_id, &payload.freeze(), chunk_size).await
}

/* ================= misc ================= */

pub trait PutU24 {
    fn put_u24(&mut self, v: u32);
}
impl PutU24 for BytesMut {
    fn put_u24(&mut self, v: u32) {
        self.extend_from_slice(&v.to_be_bytes()[1..]);
    }
}
