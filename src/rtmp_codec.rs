use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::AsyncReadExt;
use tokio_stream::{Stream};
use tokio_util::codec::{Decoder, Encoder, FramedRead};
use std::{io, pin::Pin, task::{Context, Poll}};

use crate::rtmp::PutU24;

/// RTMP 编解码器，用于在异步流中编码和解码 RTMP 消息
#[derive(Debug, Clone)]
pub struct RtmpCodec {
    chunk_size: usize,
    last_headers: Vec<Option<RtmpChunkHeader>>,
    remaining_payload: Vec<usize>
}

#[derive(Default, Debug, Clone, Copy)]
pub struct RtmpChunkTimestamp {
    pub absolute: u32,
    pub raw: u32,
    pub delta: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct RtmpChunkHeader {
    pub fmt: u8,
    pub csid: usize,
    pub timestamp: RtmpChunkTimestamp,
    pub msg_len: usize,
    pub msg_type: u8,
    pub stream_id: u32,
}

#[derive(Clone, Debug)]
pub struct  RtmpChunk {
    pub fmt: u8,
    pub csid: usize,
    pub header: RtmpChunkHeader,
    pub payload_offset: usize,
    pub msg_complete: bool,
    pub raw_bytes: Bytes,
}

#[derive(Clone, Debug)]
pub struct RtmpMessageHeader {
    pub timestamp: u32,
    pub msg_len: usize,
    pub msg_type: u8,
    pub stream_id: u32,
}

#[derive(Clone, Debug)]
pub struct RtmpMessage {
    pub csid: u8,
    pub header: RtmpMessageHeader,
    pub chunks: Vec<RtmpChunk>,
}

pub struct RtmpMessageIter {
    csid: usize,
    first: bool,
    chunk_size: usize,
    header: RtmpMessageHeader,
    payload: BytesMut,
}

pub struct RtmpMessageStream<S: AsyncReadExt + Unpin> {
    pub framed_chunk: FramedRead<S, RtmpCodec>,
    pub chunks: Vec<Option<Vec<RtmpChunk>>>,
}

impl RtmpChunkTimestamp {
    fn update(&mut self, raw: u32) {
        if self.delta {
           self.absolute = self.absolute.wrapping_add(raw)
        } else {
            self.absolute = raw;
        }
    }
}

impl RtmpChunk {
    pub fn payload(&self) -> Bytes {
        self.raw_bytes.slice(self.payload_offset..)
    }
}

impl From<RtmpChunkHeader> for RtmpMessageHeader {
    fn from(ch: RtmpChunkHeader) -> Self {
        Self {
            timestamp: ch.timestamp.absolute,
            msg_len: ch.msg_len,
            msg_type: ch.msg_type,
            stream_id: ch.stream_id,
        }
    }
}

impl RtmpMessage {
    pub fn new_with_payload(
        csid: u8,
        timestamp: u32,
        msg_type: u8,
        stream_id: u32,
        chunk_size: usize,
        payload: &Bytes
    ) -> Self {
        let header = RtmpMessageHeader { timestamp, msg_len: payload.len(), msg_type, stream_id };
        let chunks = RtmpMessageIter::new_with_payload(csid, timestamp, msg_type, stream_id, chunk_size, payload).collect();
        Self { csid, header, chunks }
    }

    pub fn payload(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.header.msg_len);
        for chunk in &self.chunks {
            buf.extend_from_slice(&chunk.payload());
        }
        buf.freeze()
    }

    pub fn first_chunk_payload(&self) -> Bytes {
        self.chunks.first().unwrap().payload()
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
        if csid == 0 || csid == 1 {
            return Err(io::Error::new( io::ErrorKind::InvalidData, "Extended CSID not supported"));
        }

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
        header.fmt = fmt;
        header.csid = csid;
        match fmt {
            0 => {
                let mh = &src[1..12];
                header.timestamp.delta = false;
                header.timestamp.raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
                header.msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
                header.msg_type = mh[6];
                header.stream_id = u32::from_le_bytes([mh[7], mh[8], mh[9], mh[10]]);
            }
            1 => {
                let mh = &src[1..8];
                header.timestamp.delta = true;
                header.timestamp.raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
                header.msg_len = u32::from_be_bytes([0, mh[3], mh[4], mh[5]]) as usize;
                header.msg_type = mh[6];
            }
            2 => {
                let mh = &src[1..4];
                header.timestamp.delta = true;
                header.timestamp.raw = u32::from_be_bytes([0, mh[0], mh[1], mh[2]]);
            }
            3 => {}
            _ => unreachable!(),
        };
        let mut offset = 1 + header_len;

        // 处理扩展时间戳
        let new_timestamp = if header.timestamp.raw == 0xffffff {
            if src.len() < offset + 4 {
                return Ok(None);
            }
            let ext = &src[offset..offset + 4];
            offset += 4;
            u32::from_be_bytes([ext[0], ext[1], ext[2], ext[3]])
        } else {
            header.timestamp.raw
        };
        if self.remaining_payload[csid] == 0 {
            header.timestamp.update(new_timestamp);
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
            fmt,
            csid,
            header: *header,
            payload_offset: offset,
            msg_complete: self.remaining_payload[csid] == 0,
            raw_bytes: data.freeze()
        };
        Ok(Some(chunk))
    }
}

impl Encoder<RtmpChunk> for RtmpCodec {
    type Error = io::Error;

    fn encode(&mut self, chunk: RtmpChunk, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&chunk.raw_bytes);
        Ok(())
    }
}

impl<S: AsyncReadExt + Unpin>  RtmpMessageStream<S> {
    pub fn new(s: S, chunk_size: usize) -> Self {
        let codec = RtmpCodec::new(chunk_size);
        let framed_chunk = FramedRead::new(s, codec);
        Self {
            framed_chunk,
            chunks: vec![None; 64],
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

        while let Poll::Ready(opt) = Pin::new(&mut this.framed_chunk).poll_next(cx) {
            match opt {
                Some(Ok(chunk)) => {
                    let csid = chunk.header.csid;
                    if this.chunks[csid].is_none() {
                        this.chunks[csid] = Some(Vec::new());
                    }
                    let chunks = this.chunks[csid].as_mut().unwrap();
                    chunks.push(chunk);
                    let chunk = chunks.last().unwrap();

                    if chunk.msg_complete {
                        let msg = RtmpMessage {
                            csid: csid as u8,
                            header: chunk.header.into(),
                            chunks: this.chunks[csid].take().unwrap(),
                        };
                        return Poll::Ready(Some(Ok(msg)));
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }

        Poll::Pending
    }
}

impl RtmpMessageIter {
    pub fn new_with_payload(
        csid: u8,
        timestamp: u32,
        msg_type: u8,
        stream_id: u32,
        chunk_size: usize,
        payload: &Bytes,
    ) -> Self {
        let header = RtmpMessageHeader {
            timestamp,
            msg_len: payload.len(),
            msg_type,
            stream_id,
        };
        Self {
            csid: csid as usize,
            header,
            chunk_size,
            payload: BytesMut::from(&payload[..]),
            first: true,
        }
    }

    pub fn new_with_msg(msg: &RtmpMessage, chunk_size: usize) -> Self {
        let payload = msg.payload();
        Self {
            csid: msg.csid as usize,
            header: msg.header.clone(),
            chunk_size,
            payload: payload.into(),
            first: true,
    }
    }
}


impl Iterator for RtmpMessageIter {
    type Item = RtmpChunk;

    fn next(&mut self) -> Option<Self::Item> {
        if self.payload.is_empty() {
            return None;
        }

        /* ---------- payload split ---------- */
        let payload_len = self.chunk_size.min(self.payload.len());
        let payload = self.payload.split_to(payload_len);

        let fmt = if self.first { 0 } else { 3 };
        self.first = false;

        /* ---------- timestamp ---------- */
        let ts = self.header.timestamp;
        let need_ext_ts = ts >= 0xFFFFFF;
        let ts_field = if need_ext_ts { 0xFFFFFF } else { ts };

        /* ---------- build raw bytes ---------- */
        let mut buf = BytesMut::with_capacity(64 + payload_len);

        /* ===== Basic Header ===== */
        buf.put_u8((fmt << 6) | (self.csid as u8 & 0x3f));

        /* ===== Message Header ===== */
        if fmt < 3 {
            // timestamp / timestamp delta
            buf.put_u24(ts_field);

            if fmt < 2 {
                buf.put_u24(self.header.msg_len as u32);
                buf.put_u8(self.header.msg_type);

                if fmt == 0 {
                    buf.put_u32_le(self.header.stream_id);
                }
            }
        }

        /* ===== Extended Timestamp ===== */
        if need_ext_ts {
            buf.put_u32(ts);
        }

        let payload_offset = buf.len();

        /* ===== Chunk Data ===== */
        buf.extend_from_slice(&payload);

        let msg_complete = self.payload.is_empty();

        Some(RtmpChunk {
            fmt,
            csid: self.csid as usize,
            header: RtmpChunkHeader {
                fmt,
                csid: self.csid as usize,
                timestamp: RtmpChunkTimestamp {
                    absolute: ts,
                    raw: ts_field,
                    delta: false,
                },
                msg_len: self.header.msg_len,
                msg_type: self.header.msg_type,
                stream_id: self.header.stream_id,
            },
            payload_offset,
            msg_complete,
            raw_bytes: buf.freeze(),
        })
    }
}
