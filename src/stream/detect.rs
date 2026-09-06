pub trait KeyframeDetect {
    /// 返回 true 表示该数据包是播放器可以直接开始解码的关键帧包
    fn is_keyframe(&self) -> bool;
}

impl KeyframeDetect for [u8] {
    fn is_keyframe(&self) -> bool {
        // TagType: 9 = video
        // FLV Tag Header 固定 11 字节：
        // [TagType][DataSize(3)][Timestamp(3)][TimestampExt][StreamID(3)]
        // payload 至少需要 5 字节：
        // [FrameType+CodecID][PacketType][CTS(3)]
        if self.first() != Some(&9) || self.len() < 11 + 5 {
            return false;
        }

        // payload 紧跟在 tag header 之后
        let data = &self[11..];

        // Enhanced RTMP: payload[0] bit7 为 IsExVideoHeader
        if data[0] & 0x80 != 0 {
            return is_ex_video_keyframe(data);
        }

        // Legacy
        // payload[0]:
        // 高 4 位：FrameType
        // 低 4 位：CodecID
        let frame_type = data[0] >> 4;
        let codec_id = data[0] & 0x0f;

        // FrameType 为 1 (keyframe) 才可能是关键帧
        if frame_type != 1 {
            return false;
        }

        // payload[1] 为 0 时是序列头，不承载 NALU
        if data[1] != 1 {
            return false;
        }

        match codec_id {
            // 7  = AVC / H.264
            7 => find_idr(&data[5..], false),
            // 12 = 非标 HEVC：封装沿用 AVC 布局，NALU 为 H.265 格式
            12 => find_idr(&data[5..], true),
            _ => false,
        }
    }
}

impl KeyframeDetect for Vec<u8> {
    fn is_keyframe(&self) -> bool {
        self.as_slice().is_keyframe()
    }
}

/// 在 4 字节大端长度前缀的 NALU 序列中查找 IDR
fn find_idr(data: &[u8], hevc: bool) -> bool {
    let mut i = 0;
    while i + 4 <= data.len() {
        let nalu_len =
            u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;
        i += 4;

        // 防御：畸形 / 截断包
        if i + nalu_len > data.len() {
            break;
        }
        // 0 长度 NALU 没有头可判断，跳过后继续读下一个长度前缀
        if nalu_len == 0 {
            continue;
        }

        let is_idr = if hevc {
            // HEVC NALU 头 2 字节，nal_unit_type 占首字节 bits6-1
            // 19 = IDR_W_RADL, 20 = IDR_N_LP
            let nalu_type = (data[i] >> 1) & 0x3f;
            nalu_type == 19 || nalu_type == 20
        } else {
            // AVC NALU 头 1 字节，nal_unit_type 占低 5 位，5 = IDR
            let nalu_type = data[i] & 0x1f;
            nalu_type == 5
        };
        if is_idr {
            return true;
        }

        i += nalu_len;
    }

    false
}

/// Enhanced RTMP（IsExVideoHeader 置位）的关键帧判断
///
/// ExVideoTagHeader:
/// [0]    bit7 = IsExVideoHeader（进入本函数前已确认）
///        bits6-4 = FrameType, bits3-0 = PacketType
/// [1..5] FourCC（"hvc1"/"hev1" = HEVC, "avc1" = AVC）
///
/// 以 FrameType 为准，PacketType 只需是承载编码数据的类型：
/// 1 = Coded Frames, 3 = Coded Frames X, 4 = Coded Frames Unconditional
/// 0 = Sequence Start / 2 = Sequence End / 5 = Metadata / 6 = Multitrack 不参与判断
fn is_ex_video_keyframe(data: &[u8]) -> bool {
    if !matches!(data[0] & 0x0f, 1 | 3 | 4) {
        return false;
    }

    (data[0] >> 4) & 0x07 == 1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造 FLV video tag：11 字节 tag header + payload
    fn tag(payload: &[u8]) -> Vec<u8> {
        let mut v = vec![9u8];
        v.extend_from_slice(&(payload.len() as u32).to_be_bytes()[1..]);
        v.extend_from_slice(&[0; 7]); // Timestamp(3) + TimestampExt + StreamID(3)
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn legacy_avc_keyframe() {
        // 0x17 = keyframe + AVC；IDR NALU（type 5）
        let key = tag(&[[0x17, 1, 0, 0, 0].as_slice(), &[0, 0, 0, 1], &[0x65]].concat());
        assert!(key.is_keyframe());

        // 0x27 = inter frame + AVC
        let inter = tag(&[[0x27, 1, 0, 0, 0].as_slice(), &[0, 0, 0, 1], &[0x41]].concat());
        assert!(!inter.is_keyframe());

        // 0x17 0x00 = AVC 序列头，不算关键帧
        let seq = tag(&[[0x17, 0].as_slice(), &[0x01, 0x64, 0x00, 0x1f]].concat());
        assert!(!seq.is_keyframe());
    }

    #[test]
    fn legacy_hevc_keyframe() {
        // 0x1C = keyframe + codec 12（非标 HEVC）
        // IDR_W_RADL (19)：NALU 头首字节 0x26
        let idr19 = tag(&[[0x1C, 1, 0, 0, 0].as_slice(), &[0, 0, 0, 2], &[0x26, 0x01]].concat());
        assert!(idr19.is_keyframe());

        // IDR_N_LP (20)：NALU 头首字节 0x28
        let idr20 = tag(&[[0x1C, 1, 0, 0, 0].as_slice(), &[0, 0, 0, 2], &[0x28, 0x01]].concat());
        assert!(idr20.is_keyframe());

        // TRAIL_R (1)：NALU 头首字节 0x02
        let inter = tag(&[[0x1C, 1, 0, 0, 0].as_slice(), &[0, 0, 0, 2], &[0x02, 0x01]].concat());
        assert!(!inter.is_keyframe());

        // 0x1C 0x00 = HEVC 序列头（HEVCDecoderConfigurationRecord），不算关键帧
        let seq = tag(&[[0x1C, 0].as_slice(), &[0x01, 0x01, 0x60, 0x00]].concat());
        assert!(!seq.is_keyframe());
    }

    #[test]
    fn enhanced_rtmp_keyframe() {
        // 0x91 = IsExVideoHeader + keyframe + Coded Frames
        let hevc = tag(
            &[[0x91].as_slice(), b"hvc1", &[0, 0, 0], &[0, 0, 0, 2], &[0x26, 0x01]].concat(),
        );
        assert!(hevc.is_keyframe());

        // hev1 是 HEVC 的别名
        let hev1 = tag(
            &[[0x91].as_slice(), b"hev1", &[0, 0, 0], &[0, 0, 0, 2], &[0x26, 0x01]].concat(),
        );
        assert!(hev1.is_keyframe());

        // avc1 同样以 FrameType 判定
        let avc = tag(
            &[[0x91].as_slice(), b"avc1", &[0, 0, 0], &[0, 0, 0, 1], &[0x65]].concat(),
        );
        assert!(avc.is_keyframe());

        // 0xA1 = IsExVideoHeader + inter frame + Coded Frames
        let inter = tag(
            &[[0xA1].as_slice(), b"hvc1", &[0, 0, 0], &[0, 0, 0, 2], &[0x02, 0x01]].concat(),
        );
        assert!(!inter.is_keyframe());
    }

    #[test]
    fn enhanced_rtmp_non_coded_frames() {
        // 0x90 = IsExVideoHeader + keyframe + Sequence Start
        let seq_start = tag(&[[0x90].as_slice(), b"hvc1", &[0x01, 0x00, 0x00, 0x00, 0x00]].concat());
        assert!(!seq_start.is_keyframe());

        // 0x92 = Sequence End
        let seq_end = tag(&[[0x92].as_slice(), b"hvc1"].concat());
        assert!(!seq_end.is_keyframe());
    }

    #[test]
    fn malformed_input() {
        // 空数据 / 不完整 tag
        assert!(!Vec::new().is_keyframe());
        assert!(!vec![9u8, 0, 0].is_keyframe());

        // 截断的 NALU 长度前缀
        let truncated = tag(&[[0x17, 1, 0, 0, 0].as_slice(), &[0, 0, 0, 100]].concat());
        assert!(!truncated.is_keyframe());

        // 末尾 0 长度 NALU 不应 panic
        let zero_tail = tag(&[[0x17, 1, 0, 0, 0].as_slice(), &[0, 0, 0, 0]].concat());
        assert!(!zero_tail.is_keyframe());
    }
}
