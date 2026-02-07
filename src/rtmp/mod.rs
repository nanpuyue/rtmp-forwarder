use bytes::Bytes;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use self::codec::RtmpMessageIter;
use crate::error::Result;

pub use self::amf::RtmpCommand;
pub use self::codec::{RtmpMessage, RtmpMessageStream};
pub use self::handshake::{handshake_with_client, handshake_with_server};

mod amf;
mod codec;
mod handshake;

pub async fn write_rtmp_message<S>(s: &mut S, msg: &RtmpMessage, chunk_size: usize) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    // chunk size 匹配时原样转发
    if chunk_size == msg.chunk_size() {
        for chunk in msg.chunks() {
            s.write_all(chunk.raw_bytes()).await?;
        }
    } else {
        for chunk in RtmpMessageIter::new_with_msg(msg, chunk_size) {
            s.write_all(chunk.raw_bytes()).await?;
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
        s.write_all(chunk.raw_bytes()).await?;
    }
    Ok(())
}
