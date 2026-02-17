pub trait KeyframeDetect {
    /// 返回 true 表示该数据包内包含关键帧（IDR）
    fn is_keyframe(&self) -> bool;
}

impl KeyframeDetect for [u8] {
    fn is_keyframe(&self) -> bool {
        // TagType: 9 = video
        // FLV Tag Header 固定 11 字节：
        // [TagType][DataSize(3)][Timestamp(3)][TimestampExt][StreamID(3)]
        // payload 至少需要包含 5 字节：
        // [FrameType+CodecID][PacketType][CTS(3)]
        if self[0] != 9 || self.len() < 11 + 5 {
            return false;
        }

        // payload 紧跟在 tag header 之后
        let data = &self[11..];

        // payload[0]:
        // 高 4 位：FrameType
        // 低 4 位：CodecID
        let frame_type = data[0] >> 4;
        let codec_id = data[0] & 0x0f;

        // FrameType 为 1 (keyframe) 才可能是关键帧
        if frame_type != 1 {
            return false;
        }

        // TODO: 支持 HEVC
        match codec_id {
            // 7  = AVC / H.264
            7 => is_avc_keyframe(data),
            _ => false,
        }
    }
}

impl KeyframeDetect for Vec<u8> {
    fn is_keyframe(&self) -> bool {
        self.as_slice().is_keyframe()
    }
}

/// 判断一个 FLV AVC payload 是否包含 IDR（NALU type 5）
fn is_avc_keyframe(data: &[u8]) -> bool {
    // FLV AVC payload 布局：
    //
    // [0] FrameType(4) + CodecID(4)
    // [1] AVCPacketType
    //     0 = AVC sequence header (SPS/PPS)
    //     1 = AVC NALU
    // [2..4] CompositionTime
    // [5..] 一个或多个 length-prefixed NALU

    // 只在真正承载 NALU 的包中判断关键帧
    if data[1] != 1 {
        return false;
    }

    let mut i = 5;
    while i + 4 <= data.len() {
        // AVC 在 FLV 中使用 4 字节大端长度前缀
        let nalu_len =
            u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;
        i += 4;

        // 防御：畸形 / 截断包
        if i + nalu_len > data.len() {
            break;
        }

        // AVC NALU header:
        // forbidden_zero_bit (1)
        // nal_ref_idc        (2)
        // nal_unit_type      (5)
        let nalu_type = data[i] & 0x1f;

        // 5 = IDR slice
        if nalu_type == 5 {
            return true;
        }

        i += nalu_len;
    }

    false
}
