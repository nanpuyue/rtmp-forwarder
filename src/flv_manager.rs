use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::info;

use crate::rtmp::PutU24;
use crate::rtmp_codec::RtmpMessage;
use crate::stream_manager::{StreamManager, StreamMessage, StreamSnapshot};

pub struct FlvManager {
    stream_manager: Arc<StreamManager>,
    // 直接使用广播通道，无需HashMap和锁
    broadcast_tx: broadcast::Sender<Bytes>,
}

impl FlvManager {
    pub fn new(stream_manager: Arc<StreamManager>) -> Self {
        let (broadcast_tx, _) = broadcast::channel(1024);
        Self {
            stream_manager,
            broadcast_tx,
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut msg_rx = self.stream_manager.subscribe();
        info!("FlvManager started");

        while let Ok(stream_msg) = msg_rx.recv().await {
            if self.broadcast_tx.receiver_count() > 0
                && let StreamMessage::RtmpMessage(msg) = stream_msg
                && let Some(flv_data) = self.rtmp_to_flv(&msg)
            {
                self.broadcast_flv(flv_data).await;
            }
        }

        info!("FlvManager stopped");
    }

    async fn broadcast_flv(&self, data: Bytes) {
        // 直接使用广播通道发送数据
        let _ = self.broadcast_tx.send(data);
    }

    pub async fn subscribe_flv(&self) -> (Bytes, broadcast::Receiver<Bytes>) {
        let mut rx = self.broadcast_tx.subscribe();
        // 过滤出第一个关键帧并追加到头
        let first_keyframe = async {
            loop {
                match rx.recv().await {
                    Ok(data) => {
                        // FLV tag header格式：
                        // [1字节tag类型][3字节数据长度][3字节时间戳][1字节时间戳扩展][3字节流ID][payload...]
                        // payload起始位置固定为第11字节

                        // 检查数据长度足够且为视频包 (tag_type = 9)
                        // payload[0] = frame_type(4位) + codec_id(4位)
                        // 0x17 = 0001 0111, 前4位是frame_type, 后4位是codec_id
                        if data.len() >= 21 // 数据长度
                            && data[0] == 9 // tag 类型
                            && data[11] >> 4 == 0x01 // 帧类型
                            && data[20] & 0x1f == 5 // 帧类型不可靠，检查 NALU
                        {
                            break data;
                        }
                    }
                    Err(_) => {
                        break Bytes::new();
                    }
                }
            }
        };
        let first_keyframe = timeout(Duration::from_secs(4), first_keyframe)
            .await
            .unwrap_or_default();
        // 使用快照创建FLV头部，包含序列头
        let snapshot = self.stream_manager.get_stream_snapshot().await;
        let mut header = self.create_flv_header(snapshot.as_ref()).await;
        header.extend_from_slice(&first_keyframe);

        // 返回广播通道订阅者和带首包的头部数据
        (header.freeze(), rx)
    }

    fn rtmp_to_flv(&self, msg: &RtmpMessage) -> Option<Bytes> {
        match msg.header.msg_type {
            8 => Some(self.create_flv_audio_tag(&msg.payload(), msg.header.timestamp)),
            9 => Some(self.create_flv_video_tag(&msg.payload(), msg.header.timestamp)),
            18 => Some(self.create_flv_script_tag(&msg.payload(), msg.header.timestamp)),
            _ => None,
        }
    }

    async fn create_flv_header(&self, snapshot: Option<&StreamSnapshot>) -> BytesMut {
        let mut buf = BytesMut::new();

        // 添加 FLV 文件头
        buf.extend_from_slice(b"FLV");

        buf.put_u8(1);
        // 根据Adobe FLV规范：
        // bit 0 = TypeFlagsVideo (hasVideo)
        // bit 2 = TypeFlagsAudio (hasAudio)
        // 默认假设音视频都有
        buf.put_u8(5); // bit 1 (1) + bit 2 (4) = 5
        buf.put_u32(9);
        buf.put_u32(0);

        // 根据快照添加序列头
        if let Some(snapshot) = snapshot {
            // 添加视频序列头（假设一定有视频序列头）
            if let Some(ref video_hdr) = snapshot.video_seq_hdr {
                let flv_tag = self.create_flv_video_tag(video_hdr, 0);
                buf.extend_from_slice(&flv_tag);

                // 只有在有视频头的情况下才处理音频头或设置音频标记位
                if let Some(ref audio_hdr) = snapshot.audio_seq_hdr {
                    // 有视频头和音频头
                    let flv_tag = self.create_flv_audio_tag(audio_hdr, 0);
                    buf.extend_from_slice(&flv_tag);
                } else {
                    // 有视频头但无音频头，将音频标记位设为0，只保留视频标记 (bit 0 = 1)
                    buf[4] = 1;
                }
            }
        }

        buf
    }

    fn create_flv_audio_tag(&self, data: &Bytes, timestamp: u32) -> Bytes {
        self.create_flv_tag(8, data, timestamp)
    }

    fn create_flv_video_tag(&self, data: &Bytes, timestamp: u32) -> Bytes {
        self.create_flv_tag(9, data, timestamp)
    }

    fn create_flv_script_tag(&self, data: &Bytes, timestamp: u32) -> Bytes {
        self.create_flv_tag(18, data, timestamp)
    }

    fn create_flv_tag(&self, tag_type: u8, data: &Bytes, timestamp: u32) -> Bytes {
        let data_size = data.len() as u32;
        let mut buf = BytesMut::with_capacity(11 + data.len() + 4);

        buf.put_u8(tag_type);
        buf.put_u24(data_size);
        buf.put_u24(timestamp); // 时间戳，低 24 位
        buf.put_u32(timestamp & 0xff000000); // stream id, 高 8 位为扩展时间戳
        buf.extend_from_slice(data);
        buf.put_u32(11 + data_size);

        buf.freeze()
    }
}
