use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use super::{StreamEvent, StreamManager, StreamMessage, StreamSnapshot, StreamState};
use crate::rtmp::RtmpMessage;
use crate::stream::KeyframeDetect;
use crate::util::PutU24;

/// FLV 广播帧
#[derive(Debug, Clone)]
pub enum FlvFrame {
    /// FLV tag 数据（音/视频/脚本）
    Tag(Bytes),
    /// 流结束标记
    End,
}

/// 等待首个关键帧的超时：超过后不再等待，直接从 GOP 中间开始输出
/// （从关键帧起播只是为了让 mpv 等播放器不报错，不能为此让客户端一直等）
const KEYFRAME_WAIT: Duration = Duration::from_secs(4);

pub struct FlvManager {
    stream_manager: Arc<StreamManager>,
    // 直接使用广播通道，无需HashMap和锁
    broadcast_tx: broadcast::Sender<FlvFrame>,
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
            match stream_msg {
                StreamMessage::RtmpMessage(msg) => {
                    if self.broadcast_tx.receiver_count() > 0
                        && let Some(flv_data) = self.rtmp_to_flv(&msg)
                    {
                        self.broadcast_flv(FlvFrame::Tag(flv_data)).await;
                    }
                }
                StreamMessage::StateChanged(event) => match event {
                    // 取消推流、流超时、连接断开都意味着流结束
                    StreamEvent::Idle | StreamEvent::Closed | StreamEvent::Deleted => {
                        self.broadcast_flv(FlvFrame::End).await;
                    }
                    _ => {}
                },
            }
        }

        info!("FlvManager stopped");
    }

    async fn broadcast_flv(&self, frame: FlvFrame) {
        // 直接使用广播通道发送数据
        let _ = self.broadcast_tx.send(frame);
    }

    /// 订阅 HTTP-FLV 流，返回 FLV 头部（含序列头和首个关键帧）和广播接收端
    ///
    /// 无推流、等待期间流结束、或等待窗口内没有视频数据时返回 None
    pub async fn subscribe_flv(&self) -> Option<(Bytes, broadcast::Receiver<FlvFrame>)> {
        // 先订阅再检查状态：结束事件总是在流状态变更之后广播，
        // 状态检查通过后发生的结束事件必然能被 rx 接收到，不会错过
        let mut rx = self.broadcast_tx.subscribe();
        let (_, state) = self.stream_manager.default_stream_state().await;
        if state != StreamState::Publishing {
            return None;
        }

        // 过滤出第一个关键帧并追加到头
        let first_keyframe = async {
            let start = Instant::now();
            let mut seen_video = false;
            loop {
                match timeout(KEYFRAME_WAIT.saturating_sub(start.elapsed()), rx.recv()).await {
                    // 等待关键帧超时：不再等待，直接从 GOP 中间开始输出
                    Err(_) => {
                        if seen_video {
                            break Some(Bytes::new());
                        }
                        // 正常推流端在推流后立即发送视频序列头，不会出现 4 秒内
                        // 没有任何视频数据；出现即视为无视频流（音频推流/假死）
                        warn!(
                            "HTTP-FLV: no video data within {}s, treat as no-video stream",
                            KEYFRAME_WAIT.as_secs()
                        );
                        break None;
                    }
                    // 推流在加入过程中结束
                    Ok(Ok(FlvFrame::End)) | Ok(Err(_)) => break None,
                    Ok(Ok(FlvFrame::Tag(data))) => {
                        if data.is_keyframe() {
                            debug!("first keyframe detected, size: {}", data.len());
                            break Some(data);
                        }
                        // FLV tag 首字节为 TagType，9 = video
                        seen_video |= data.first() == Some(&9);
                    }
                }
            }
        };
        let first_keyframe = first_keyframe.await?;

        // 使用快照创建FLV头部，包含序列头
        let snapshot = self.stream_manager.get_stream_snapshot().await?;
        let mut header = self.create_flv_header(snapshot).await;
        header.extend_from_slice(&first_keyframe);

        // 返回广播通道订阅者和带首包的头部数据
        Some((header.freeze(), rx))
    }

    fn rtmp_to_flv(&self, msg: &RtmpMessage) -> Option<Bytes> {
        let h = msg.header();
        match h.msg_type {
            8 | 9 | 18 => {
                let mut buf = BytesMut::with_capacity(11 + h.msg_len + 4);

                buf.put_u8(h.msg_type);
                buf.put_u24(h.msg_len as u32);
                buf.put_u24(h.timestamp); // 时间戳，低 24 位
                buf.put_u32(h.timestamp & 0xff000000); // stream id, 高 8 位为扩展时间戳
                for chunk in msg.chunks() {
                    buf.extend_from_slice(&chunk.payload());
                }
                buf.put_u32(11 + h.msg_len as u32);

                Some(buf.freeze())
            }
            _ => None,
        }
    }

    async fn create_flv_header(&self, snapshot: StreamSnapshot) -> BytesMut {
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

        // 添加视频序列头（假设一定有视频序列头）
        if let Some(ref video_hdr) = snapshot.video_seq_hdr.and_then(|x| self.rtmp_to_flv(&x)) {
            buf.extend_from_slice(video_hdr);

            // 只有在有视频头的情况下才处理音频头或设置音频标记位
            if let Some(ref audio_hdr) = snapshot.audio_seq_hdr.and_then(|x| self.rtmp_to_flv(&x)) {
                // 有视频头和音频头
                buf.extend_from_slice(audio_hdr);
            } else {
                // 有视频头但无音频头，将音频标记位设为0，只保留视频标记 (bit 0 = 1)
                buf[4] = 1;
            }
        }

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_stream_returns_none() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stream_manager = StreamManager::new();
            let flv_manager = FlvManager::new(stream_manager);
            assert!(flv_manager.subscribe_flv().await.is_none());
        });
    }
}
