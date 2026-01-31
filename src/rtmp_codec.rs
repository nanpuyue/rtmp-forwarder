use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio_stream::{Stream};
use tokio_util::codec::{Decoder, Encoder, FramedRead};
use crate::rtmp::{RtmpMessage, RtmpHeader};
use std::{io, pin::Pin, task::{Context, Poll}};

/// RTMP 编解码器，用于在异步流中编码和解码 RTMP 消息
#[derive(Debug, Clone)]
pub struct RtmpCodec {
    chunk_size: usize,
    last_headers: Vec<Option<RtmpHeader>>,
    remaining_payload: Vec<usize>
}

#[derive(Debug)]
pub struct  RtmpChunk {
    pub header: RtmpHeader,
    pub payload_offset: usize,
    pub data: BytesMut,
}

pub struct RtmpMessageStream<S: AsyncReadExt + Unpin> {
    pub framed_chunk: FramedRead<S, RtmpCodec>,
    pub payload: Vec<Option<BytesMut>>,
}

impl RtmpChunk {
    pub fn payload(&self) -> &[u8] {
        &self.data[self.payload_offset..]
    }
}


impl RtmpCodec {
    /// 创建一个新的 RTMP 编解码器
    pub fn new(chunk_size: usize) -> Self {
        Self{
            chunk_size,
            last_headers: vec![None; 64],
            remaining_payload: vec![0; 64],
        }
    }

    pub fn set_chunk_size(&mut self, size: usize) {
        self.chunk_size = size;
    }
}

impl Decoder for RtmpCodec {
    type Item = RtmpChunk;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
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
        header.csid = csid;
        match fmt {
            0 => {
                let mh = &src[1..12];
                header.timestamp_is_delta = false;
                header.timestamp_raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
                header.msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
                header.msg_type = mh[6];
                header.stream_id = u32::from_le_bytes([mh[7], mh[8], mh[9], mh[10]]);
            }
            1 => {
                let mh = &src[1..8];
                header.timestamp_is_delta = true;
                header.timestamp_raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
                header.msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
                header.msg_type = mh[6];
            }
            2 => {
                let mh = &src[1..4];
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

        let chunk = RtmpChunk {
            header: *header,
            payload_offset: offset,
            data
        };
        Ok(Some(chunk))
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
        let codec = RtmpCodec::new(chunk_size);
        let framed_chunk = FramedRead::new(s, codec);
        Self {
            framed_chunk,
            payload: vec![None; 64],
         }
    }
    
    pub fn set_chunk_size(&mut self, size: usize) {
        self.framed_chunk.decoder_mut().set_chunk_size(size);
    }
}

impl<S: AsyncReadExt + Unpin> Stream for RtmpMessageStream<S> {
    type Item = Result<RtmpMessage, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // 使用内部的 framed_chunk 来获取下一个 chunk
        loop {
            match Pin::new(&mut this.framed_chunk).poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    let csid = chunk.header.csid;
                    if this.payload[csid].is_none() {
                        this.payload[csid] = Some(BytesMut::with_capacity(chunk.header.msg_len));
                    }
                    let payload = this.payload[csid].as_mut().unwrap();
                    payload.extend_from_slice(chunk.payload());
                    if chunk.header.msg_len == payload.len() {
                        // 完整消息，直接返回
                        break Poll::Ready(Some(Ok(RtmpMessage {
                            csid: csid as u8,
                            timestamp: chunk.header.timestamp,
                            msg_type: chunk.header.msg_type,
                            stream_id: chunk.header.stream_id,
                            payload: this.payload[csid].take().unwrap(),
                        })))
                    }
                }
                // 传播错误
                Poll::Ready(Some(Err(e))) => break Poll::Ready(Some(Err(e))),
                // 流结束
                Poll::Ready(None) => break Poll::Ready(None),
                // 还没有数据，等待更多数据
                Poll::Pending => break Poll::Pending,
            }
        }
    }
}

