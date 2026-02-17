use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, mem};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_stream::Stream;
use tokio_util::codec::{Decoder, Encoder, FramedRead};

use crate::util::PutU24;

/// RTMP 编解码器，用于在异步流中编码和解码 RTMP 消息
#[derive(Debug, Clone)]
pub struct RtmpCodec {
    chunk_size: usize,
    last_headers: Vec<Option<RtmpChunkHeader>>,
    remaining_payload: Vec<usize>,
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
pub struct RtmpChunk {
    header: RtmpChunkHeader,
    payload_offset: usize,
    msg_complete: bool,
    raw_bytes: Bytes,
}

impl RtmpChunk {
    pub fn header(&self) -> &RtmpChunkHeader {
        &self.header
    }

    pub fn raw_bytes(&self) -> &Bytes {
        &self.raw_bytes
    }

    pub fn payload(&self) -> Bytes {
        self.raw_bytes.slice(self.payload_offset..)
    }

    pub fn set_stream_id(&mut self, id: u32) {
        if id != self.header.stream_id {
            self.header.stream_id = id;

            // 仅 fmt=0 包含 stream_id 字段
            if self.header.fmt == 0 {
                let raw_bytes = mem::replace(&mut self.raw_bytes, Bytes::new());
                let mut buf = BytesMut::from(raw_bytes);
                buf[8..12].copy_from_slice(&id.to_le_bytes());
                self.raw_bytes = buf.freeze();
            }
        }
    }
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
    csid: u8,
    header: RtmpMessageHeader,
    chunk_size: usize,
    chunks: Vec<RtmpChunk>,
}

impl RtmpMessage {
    pub fn header(&self) -> &RtmpMessageHeader {
        &self.header
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn chunks(&self) -> &[RtmpChunk] {
        &self.chunks
    }

    pub fn set_stream_id(&mut self, id: u32) {
        if id != self.header.stream_id {
            self.header.stream_id = id;
            for chunk in &mut self.chunks {
                chunk.set_stream_id(id);
            }
        }
    }
}

pub struct RtmpMessageIter<'a> {
    codec: &'a mut RtmpCodec,
    csid: usize,
    ts_value: u32,
    ts_field: u32,
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
    pub fn new(chunk_size: usize) -> Self {
        Self {
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
        if src.is_empty() {
            return Ok(None);
        }
        let fmt = src[0] >> 6;
        let csid = (src[0] & 0x3f) as usize;
        if csid == 0 || csid == 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Extended CSID not supported",
            ));
        }

        // 确保有足够的数据读取消息头
        let header_len = match fmt {
            0 => 11, // 1 + 11 字节
            1 => 7,  // 1 + 7 字节
            2 => 3,  // 1 + 3 字节
            3 => 0,  // 只有基本 header
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid RTMP format",
                ));
            }
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
            header: *header,
            payload_offset: offset,
            msg_complete: self.remaining_payload[csid] == 0,
            raw_bytes: data.freeze(),
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

impl Encoder<RtmpMessage> for RtmpCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: RtmpMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        for chunk in RtmpMessageIter::from_message(self, &msg) {
            dst.extend_from_slice(&chunk.raw_bytes);
        }
        Ok(())
    }
}

impl Encoder<RtmpMessage> for &mut RtmpCodec {
    type Error = io::Error;

    fn encode(&mut self, msg: RtmpMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        for chunk in RtmpMessageIter::from_message(self, &msg) {
            dst.extend_from_slice(&chunk.raw_bytes);
        }
        Ok(())
    }
}

impl<S: AsyncRead + Unpin> RtmpMessageStream<S> {
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

impl<S: AsyncRead + Unpin> Stream for RtmpMessageStream<S> {
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
                            chunk_size: this.framed_chunk.decoder().chunk_size,
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

impl<'a> RtmpMessageIter<'a> {
    pub fn from_payload(
        codec: &'a mut RtmpCodec,
        csid: u8,
        timestamp: u32,
        msg_type: u8,
        stream_id: u32,
        payload: &Bytes,
    ) -> Self {
        let header = RtmpMessageHeader {
            timestamp,
            msg_len: payload.len(),
            msg_type,
            stream_id,
        };
        Self {
            codec,
            csid: csid as usize,
            ts_field: 0,
            ts_value: 0,
            header,
            payload: BytesMut::from(&payload[..]),
        }
    }

    pub fn from_message(codec: &'a mut RtmpCodec, msg: &RtmpMessage) -> Self {
        let payload = msg.payload();
        assert_eq!(msg.header.msg_len, payload.len());

        Self {
            codec,
            csid: msg.csid as usize,
            ts_field: 0,
            ts_value: 0,
            header: msg.header.clone(),
            payload: payload.into(),
        }
    }
}

impl<'a> Iterator for RtmpMessageIter<'a> {
    type Item = RtmpChunk;

    fn next(&mut self) -> Option<Self::Item> {
        if self.payload.is_empty() {
            return None;
        }

        let first = self.header.msg_len == self.payload.len();
        let fmt = if first {
            // 按 FFmpeg 逻辑选择首 chunk fmt
            if let Some(last) = &self.codec.last_headers[self.csid] {
                let use_delta = last.stream_id == self.header.stream_id
                    && self.header.timestamp >= last.timestamp.absolute;

                // 时间戳表示值（绝对值或增量）
                self.ts_value = if use_delta {
                    self.header.timestamp.wrapping_sub(last.timestamp.absolute)
                } else {
                    self.header.timestamp
                };
                // 头部时间戳字段值
                self.ts_field = if self.ts_value >= 0xFFFFFF {
                    0xFFFFFF
                } else {
                    self.ts_value
                };

                if !use_delta {
                    0
                } else if last.msg_type != self.header.msg_type
                    || last.msg_len != self.header.msg_len
                {
                    1
                } else if !last.timestamp.delta || self.ts_field != last.timestamp.raw {
                    // 只有上次也是增量时间戳，且 ts_field 相同，才能用 fmt 3
                    // 这比 FFmpeg 更严格，但兼容性更好
                    2
                } else {
                    3
                }
            } else {
                // 首个消息，时间戳使用绝对值
                self.ts_value = self.header.timestamp;
                self.ts_field = if self.ts_value >= 0xFFFFFF {
                    0xFFFFFF
                } else {
                    self.ts_value
                };
                0
            }
        } else {
            // 续 chunk
            3
        };

        // 计算当前 chunk 长度
        let payload_len = self.codec.chunk_size.min(self.payload.len());
        let payload = self.payload.split_to(payload_len);

        // 构建 chunk
        let mut buf = BytesMut::with_capacity(64 + payload_len);

        // Basic Header
        buf.put_u8((fmt << 6) | (self.csid as u8 & 0x3f));

        // Message Header
        match fmt {
            0 => {
                buf.put_u24(self.ts_field);
                buf.put_u24(self.header.msg_len as u32);
                buf.put_u8(self.header.msg_type);
                buf.put_u32_le(self.header.stream_id);
            }
            1 => {
                buf.put_u24(self.ts_field);
                buf.put_u24(self.header.msg_len as u32);
                buf.put_u8(self.header.msg_type);
            }
            2 => {
                buf.put_u24(self.ts_field);
            }
            3 => {}
            _ => unreachable!(),
        }

        // 根据 ts_field 判断是否需要扩展时间戳
        if self.ts_field == 0xFFFFFF {
            buf.put_u32(self.ts_value);
        }

        let payload_offset = buf.len();

        // Chunk Data
        buf.extend_from_slice(&payload);

        let msg_complete = self.payload.is_empty();

        // 创建 chunk
        let chunk = RtmpChunk {
            header: RtmpChunkHeader {
                fmt,
                csid: self.csid,
                timestamp: RtmpChunkTimestamp {
                    delta: fmt != 0,                 // fmt 0 → 绝对, 其他 → 增量
                    raw: self.ts_field,              // 记录写入的 ts_field 值
                    absolute: self.header.timestamp, // 记录绝对时间戳用于计算下次增量
                },
                msg_len: self.header.msg_len,
                msg_type: self.header.msg_type,
                stream_id: self.header.stream_id,
            },
            payload_offset,
            msg_complete,
            raw_bytes: buf.freeze(),
        };

        // 更新 last_headers 供下次比较 - 只有首 chunk 才更新
        if first {
            // 我们的实现约定: fmt 0 = 绝对时间戳, fmt 1/2/3 = 增量时间戳
            // 这个约定确保了对任何符合协议的解码器的兼容性:
            //   - fmt 0 输出绝对时间戳，解码器将其作为基准
            //   - fmt 1/2 输出增量时间戳，解码器将其累加
            //   - fmt 3 不输出时间戳，解码器复用上次的增量
            // 我们的 fmt 选择逻辑保证了不会在 fmt 0 后的下条消息首 chunk 使用 fmt 3，避免混淆绝对值和增量
            self.codec.last_headers[self.csid] = Some(chunk.header);
        }

        Some(chunk)
    }
}
