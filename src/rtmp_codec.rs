use bytes::{Buf, BufMut, BytesMut};
use tokio::io::AsyncReadExt;
use tokio_stream::{Stream, StreamExt};
use tokio_util::codec::{Decoder, Encoder, Framed, FramedRead};
use crate::rtmp::{RtmpMessage, RtmpHeader};
use std::{io, pin::Pin, task::{Context, Poll}};

/// RTMP 编解码器，用于在异步流中编码和解码 RTMP 消息
#[derive(Debug, Clone)]
pub struct RtmpCodec {
    chunk_size: usize,
    last_headers: [Option<RtmpHeader>; 64],
    remaining_payload: [usize; 64]
}

pub struct  RtmpChunk {
    pub header: RtmpHeader,
    pub payload_offset: usize,
    pub data: BytesMut,
}

pub struct RtmpMessageStream<S: AsyncReadExt + Unpin> {
    pub framed_chunk: FramedRead<S, RtmpCodec>
}

impl RtmpChunk {
    pub fn payload(&self) -> &[u8] {
        &self.data[self.payload_offset..]
    }
}

impl Default for RtmpCodec {
    fn default() -> Self {
        Self {
            chunk_size: 128,
            last_headers: [None; 64],
            remaining_payload: [0; 64]
        }
    }
}

impl RtmpCodec {
    /// 创建一个新的 RTMP 编解码器
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置 chunk 大小
    pub fn set_chunk_size(&mut self, size: usize) {
        self.chunk_size = size;
    }

    /// 获取当前 chunk 大小
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

impl Decoder for RtmpCodec {
    type Item = RtmpChunk;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // 读取基本 chunk header
        if src.len() < 1 {
            return Ok(None);
        }
        let fmt = src[0] >> 6;
        let csid = (src[0] & 0x3f) as usize;

        // 确保有足够的数据读取消息头
        let header_len = match fmt {
            0 => 11, // 1 + 11 字节
            1 => 7,  // 1 + 7 字节
            2 => 3,  // 1 + 3 字节
            3 => 0,  // 只有基本 header
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RTMP format")),
        };
        if src.len() < 1 + header_len {
            return Ok(None);
        }

        // 解析消息头
        let header = self.last_headers[csid].get_or_insert_default();
        match fmt {
            0 => {
                let mh = &src[1..11];
                header.timestamp_is_delta = false;
                header.timestamp_raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
                header.msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
                header.msg_type = mh[6];
                header.stream_id = u32::from_le_bytes([mh[7], mh[8], mh[9], mh[10]]);
            }
            1 => {
                let mh = &src[1..7];
                header.timestamp_is_delta = true;
                header.timestamp_raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
                header.msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
                header.msg_type = mh[6];
            }
            2 => {
                let mh = &src[1..3];
                header.timestamp_is_delta = true;
                header.timestamp_raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            }
            3 => {}
            _ => unreachable!(),
        };
        let mut offset = 1 + header_len;

        // 处理扩展时间戳
        if header.timestamp_raw == 0xffffff {
            if src.len() < offset + 4 {
                return Ok(None);
            }
            let ext = &src[offset..offset + 4];
            let timestamp_ext = u32::from_be_bytes([ext[0], ext[1], ext[2], ext[3]]);
            header.timestamp = timestamp_ext;
            offset += 4;
        } else if self.remaining_payload[csid] == 0 {
            if header.timestamp_is_delta {
                header.timestamp = header.timestamp.wrapping_add(header.timestamp_raw);
            } else {
                header.timestamp = header.timestamp_raw;
            }
        }

        // 计算当前 chunk 长度
        if self.remaining_payload[csid] == 0 {
            self.remaining_payload[csid] = header.msg_len;
        }
        let payload_len = self.remaining_payload[csid].min(self.chunk_size);
        // 读取 chunk
        if src.len() < offset + payload_len {
            return Ok(None);
        }
        let data = src.split_to(offset + payload_len);
        self.remaining_payload[csid] -= payload_len;

            Ok(Some(RtmpChunk {
                header: *header,
                payload_offset: offset,
                data
            }))
    }
}

impl Encoder<RtmpChunk> for RtmpCodec {
    type Error = io::Error;

    fn encode(&mut self, chunk: RtmpChunk, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&chunk.data);
        Ok(())
    }
}

impl<S: AsyncReadExt + Unpin>  RtmpMessageStream<S> {
    pub fn new(s: S, chunk_size: usize) -> Self {
        let mut codec = RtmpCodec::new();
        codec.set_chunk_size(chunk_size);
        let framed_chunk = FramedRead::new(s, codec);
        Self { framed_chunk }
    }
}

impl<S: AsyncReadExt + Unpin> Stream for RtmpMessageStream<S> {
    type Item = RtmpMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

// 为 BytesMut 实现 put_u24 方法
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

