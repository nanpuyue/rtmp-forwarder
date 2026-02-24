use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::Arc;

use axum::http::StatusCode;
use bytes::Bytes;
use mpeg2ts::Result as TsResult;
use mpeg2ts::es::StreamId;
use mpeg2ts::es::StreamType;
use mpeg2ts::pes::PesHeader;
use mpeg2ts::time::Timestamp;
use mpeg2ts::ts::{
    ContinuityCounter, EsInfo, Pid, ProgramAssociation, TransportScramblingControl, TsHeader,
    TsPacket, TsPacketWriter, TsPayload, VersionNumber, WriteTsPacket,
};
use tokio::sync::RwLock;
use tracing::{info, warn};

use super::{StreamEvent, StreamManager, StreamMessage};
use crate::rtmp::RtmpMessage;

const HLS_KEYFRAMES_PER_SEGMENT: usize = 2;
const HLS_SEGMENT_WINDOW_SIZE: usize = 6;
const PMT_PID: u16 = 0x100;
const VIDEO_PID: u16 = 0x101;
const AUDIO_PID: u16 = 0x102;

#[derive(Clone)]
struct HlsSegment {
    id: u64,
    duration_secs: f32,
    data: Bytes,
}

#[derive(Clone)]
struct AvcConfig {
    sps: Vec<Vec<u8>>,
    pps: Vec<Vec<u8>>,
}

#[derive(Clone)]
struct AacConfig {
    object_type: u8,
    sampling_frequency_index: u8,
    channel_config: u8,
}

#[derive(Clone, Copy)]
struct ContinuityState {
    pat: ContinuityCounter,
    pmt: ContinuityCounter,
    video: ContinuityCounter,
    audio: ContinuityCounter,
}

struct SegmentBuilder {
    id: u64,
    writer: TsPacketWriter<Vec<u8>>,
    pat_cc: ContinuityCounter,
    pmt_cc: ContinuityCounter,
    video_cc: ContinuityCounter,
    audio_cc: ContinuityCounter,
    start_ts_ms: Option<u32>,
    end_ts_ms: u32,
    keyframes: usize,
    packet_count: usize,
}

impl SegmentBuilder {
    fn new(id: u64, has_audio: bool, continuity: ContinuityState) -> TsResult<Self> {
        let mut writer = TsPacketWriter::new(Vec::with_capacity(188 * 256));
        let mut pat_cc = continuity.pat;
        let mut pmt_cc = continuity.pmt;
        write_pat_pmt(&mut writer, has_audio, &mut pat_cc, &mut pmt_cc)?;
        Ok(Self {
            id,
            writer,
            pat_cc,
            pmt_cc,
            video_cc: continuity.video,
            audio_cc: continuity.audio,
            start_ts_ms: None,
            end_ts_ms: 0,
            keyframes: 0,
            packet_count: 0,
        })
    }

    fn write_video(
        &mut self,
        is_keyframe: bool,
        dts_ms: u32,
        pts_ms: u32,
        annexb: Vec<u8>,
    ) -> TsResult<()> {
        let dts = ms_to_ts(dts_ms);
        let pts = ms_to_ts(pts_ms);
        let header = PesHeader {
            stream_id: StreamId::new_video(StreamId::VIDEO_MIN)
                .expect("VIDEO_MIN is a valid video stream id"),
            priority: false,
            data_alignment_indicator: true,
            copyright: false,
            original_or_copy: false,
            pts: Some(pts),
            dts: Some(dts),
            escr: None,
        };
        write_pes_packets(
            &mut self.writer,
            Pid::new(VIDEO_PID)?,
            &mut self.video_cc,
            header,
            &annexb,
            0,
        )?;

        if self.start_ts_ms.is_none() {
            self.start_ts_ms = Some(dts_ms);
        }
        self.end_ts_ms = dts_ms;
        if is_keyframe {
            self.keyframes += 1;
        }
        self.packet_count += 1;
        Ok(())
    }

    fn write_audio(&mut self, ts_ms: u32, adts_frame: Vec<u8>) -> TsResult<()> {
        let pts = ms_to_ts(ts_ms);
        let header = PesHeader {
            stream_id: StreamId::new_audio(StreamId::AUDIO_MIN)
                .expect("AUDIO_MIN is a valid audio stream id"),
            priority: false,
            data_alignment_indicator: true,
            copyright: false,
            original_or_copy: false,
            pts: Some(pts),
            dts: None,
            escr: None,
        };
        let pes_packet_len = calc_pes_packet_len(&header, adts_frame.len());
        write_pes_packets(
            &mut self.writer,
            Pid::new(AUDIO_PID)?,
            &mut self.audio_cc,
            header,
            &adts_frame,
            pes_packet_len,
        )?;

        if self.start_ts_ms.is_none() {
            self.start_ts_ms = Some(ts_ms);
        }
        self.end_ts_ms = ts_ms;
        self.packet_count += 1;
        Ok(())
    }

    fn duration_secs(&self) -> f32 {
        let start = self.start_ts_ms.unwrap_or(self.end_ts_ms);
        if self.end_ts_ms >= start {
            (self.end_ts_ms - start) as f32 / 1000.0
        } else {
            0.0
        }
    }

    fn into_segment(self) -> Option<HlsSegment> {
        if self.packet_count == 0 {
            return None;
        }
        Some(HlsSegment {
            id: self.id,
            duration_secs: self.duration_secs(),
            data: Bytes::from(self.writer.into_stream()),
        })
    }
}

struct HlsState {
    segments: VecDeque<HlsSegment>,
    current: Option<SegmentBuilder>,
    next_segment_id: u64,
    avc_config: Option<AvcConfig>,
    aac_config: Option<AacConfig>,
    continuity: ContinuityState,
}

pub struct HlsManager {
    stream_manager: Arc<StreamManager>,
    state: RwLock<HlsState>,
}

impl HlsManager {
    pub fn new(stream_manager: Arc<StreamManager>) -> Self {
        Self {
            stream_manager,
            state: RwLock::new(HlsState {
                segments: VecDeque::with_capacity(HLS_SEGMENT_WINDOW_SIZE),
                current: None,
                next_segment_id: 0,
                avc_config: None,
                aac_config: None,
                continuity: ContinuityState {
                    pat: ContinuityCounter::new(),
                    pmt: ContinuityCounter::new(),
                    video: ContinuityCounter::new(),
                    audio: ContinuityCounter::new(),
                },
            }),
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut msg_rx = self.stream_manager.subscribe();
        info!("HlsManager started");

        while let Ok(stream_msg) = msg_rx.recv().await {
            match stream_msg {
                StreamMessage::RtmpMessage(msg) => self.handle_rtmp_message(msg).await,
                StreamMessage::StateChanged(
                    StreamEvent::Idle | StreamEvent::Closed | StreamEvent::Deleted,
                ) => self.reset().await,
                _ => {}
            }
        }

        info!("HlsManager stopped");
    }

    pub async fn playlist(&self) -> String {
        let state = self.state.read().await;
        let target_duration = state
            .segments
            .iter()
            .map(|s| s.duration_secs)
            .fold(1.0f32, f32::max)
            .ceil() as u32;
        let media_sequence = state.segments.front().map(|s| s.id).unwrap_or(0);

        let mut out = String::new();
        out.push_str("#EXTM3U\n");
        out.push_str("#EXT-X-VERSION:3\n");
        let _ = writeln!(&mut out, "#EXT-X-TARGETDURATION:{}", target_duration.max(1));
        let _ = writeln!(&mut out, "#EXT-X-MEDIA-SEQUENCE:{}", media_sequence);

        for seg in &state.segments {
            let _ = writeln!(&mut out, "#EXTINF:{:.3},", seg.duration_secs.max(0.1));
            let _ = writeln!(&mut out, "/live/hls/{}.ts", seg.id);
        }

        out
    }

    pub async fn segment(&self, id: u64) -> Result<Bytes, StatusCode> {
        let state = self.state.read().await;
        state
            .segments
            .iter()
            .find(|s| s.id == id)
            .map(|s| s.data.clone())
            .ok_or(StatusCode::NOT_FOUND)
    }

    async fn handle_rtmp_message(&self, msg: RtmpMessage) {
        let ts_ms = msg.header().timestamp;
        match msg.header().msg_type {
            9 => self.handle_video_message(msg, ts_ms).await,
            8 => self.handle_audio_message(msg, ts_ms).await,
            _ => {}
        }
    }

    async fn handle_video_message(&self, msg: RtmpMessage, dts_ms: u32) {
        let payload = collect_message_payload(&msg);
        if payload.len() < 5 {
            return;
        }
        let frame_type = payload[0] >> 4;
        let codec_id = payload[0] & 0x0F;
        if codec_id != 7 {
            return;
        }

        let avc_packet_type = payload[1];
        if avc_packet_type == 0 {
            let mut state = self.state.write().await;
            state.avc_config = parse_avc_decoder_config(&payload[5..]);
            return;
        }
        if avc_packet_type != 1 {
            return;
        }

        let composition_time = parse_signed_24(&payload[2..5]);
        let pts_ms = clamp_pts(dts_ms, composition_time);
        let is_keyframe = frame_type == 1 && contains_idr_nalu(&payload[5..]);

        let mut state = self.state.write().await;
        if state.current.is_none() {
            if !is_keyframe {
                return;
            }
            if let Ok(builder) = SegmentBuilder::new(
                state.next_segment_id,
                state.aac_config.is_some(),
                state.continuity,
            ) {
                state.next_segment_id += 1;
                state.current = Some(builder);
            } else {
                return;
            }
        } else if is_keyframe
            && state
                .current
                .as_ref()
                .is_some_and(|seg| seg.keyframes >= HLS_KEYFRAMES_PER_SEGMENT)
        {
            finalize_current_segment(&mut state);
            if let Ok(builder) = SegmentBuilder::new(
                state.next_segment_id,
                state.aac_config.is_some(),
                state.continuity,
            ) {
                state.next_segment_id += 1;
                state.current = Some(builder);
            } else {
                return;
            }
        }

        let avc_cfg = state.avc_config.clone();
        if let Some(current) = state.current.as_mut() {
            let annexb = avcc_to_annexb(&payload[5..], avc_cfg.as_ref(), is_keyframe);
            if annexb.is_empty() {
                return;
            }
            if let Err(e) = current.write_video(is_keyframe, dts_ms, pts_ms, annexb) {
                warn!("failed to write video TS packet: {}", e);
            }
        }
    }

    async fn handle_audio_message(&self, msg: RtmpMessage, ts_ms: u32) {
        let payload = collect_message_payload(&msg);
        if payload.len() < 2 {
            return;
        }
        let sound_format = payload[0] >> 4;
        if sound_format != 10 {
            return;
        }

        let aac_packet_type = payload[1];
        if aac_packet_type == 0 {
            let mut state = self.state.write().await;
            state.aac_config = parse_aac_audio_specific_config(&payload[2..]);
            return;
        }
        if aac_packet_type != 1 {
            return;
        }

        let mut state = self.state.write().await;
        let Some(aac_cfg) = state.aac_config.clone() else {
            return;
        };
        let Some(current) = state.current.as_mut() else {
            return;
        };

        let Some(adts_frame) = with_adts_header(&aac_cfg, &payload[2..]) else {
            return;
        };
        if let Err(e) = current.write_audio(ts_ms, adts_frame) {
            warn!("failed to write audio TS packet: {}", e);
        }
    }

    async fn reset(&self) {
        let mut state = self.state.write().await;
        state.current = None;
        state.segments.clear();
        state.next_segment_id = 0;
        state.avc_config = None;
        state.aac_config = None;
        state.continuity = ContinuityState {
            pat: ContinuityCounter::new(),
            pmt: ContinuityCounter::new(),
            video: ContinuityCounter::new(),
            audio: ContinuityCounter::new(),
        };
    }
}

fn finalize_current_segment(state: &mut HlsState) {
    let Some(builder) = state.current.take() else {
        return;
    };
    state.continuity = ContinuityState {
        pat: builder.pat_cc,
        pmt: builder.pmt_cc,
        video: builder.video_cc,
        audio: builder.audio_cc,
    };
    if let Some(seg) = builder.into_segment() {
        state.segments.push_back(seg);
        while state.segments.len() > HLS_SEGMENT_WINDOW_SIZE {
            state.segments.pop_front();
        }
    }
}

fn write_pes_packets(
    writer: &mut TsPacketWriter<Vec<u8>>,
    pid: Pid,
    cc: &mut ContinuityCounter,
    header: PesHeader,
    data: &[u8],
    pes_packet_len: u16,
) -> TsResult<()> {
    let header_len = 6usize + pes_optional_header_len(&header);
    let first_payload_cap = mpeg2ts::ts::payload::Bytes::MAX_SIZE.saturating_sub(header_len);
    let first_size = data.len().min(first_payload_cap);
    let first_data = mpeg2ts::ts::payload::Bytes::new(&data[..first_size])?;

    writer.write_ts_packet(&TsPacket {
        header: make_ts_header(pid, *cc),
        adaptation_field: None,
        payload: Some(TsPayload::PesStart(mpeg2ts::ts::payload::Pes {
            header,
            pes_packet_len,
            data: first_data,
        })),
    })?;
    cc.increment();

    let mut offset = first_size;
    while offset < data.len() {
        let end = (offset + mpeg2ts::ts::payload::Bytes::MAX_SIZE).min(data.len());
        let chunk = mpeg2ts::ts::payload::Bytes::new(&data[offset..end])?;
        writer.write_ts_packet(&TsPacket {
            header: make_ts_header(pid, *cc),
            adaptation_field: None,
            payload: Some(TsPayload::PesContinuation(chunk)),
        })?;
        cc.increment();
        offset = end;
    }

    Ok(())
}

fn write_pat_pmt(
    writer: &mut TsPacketWriter<Vec<u8>>,
    has_audio: bool,
    pat_cc: &mut ContinuityCounter,
    pmt_cc: &mut ContinuityCounter,
) -> TsResult<()> {
    let pat = TsPacket {
        header: make_ts_header(Pid::from(0), *pat_cc),
        adaptation_field: None,
        payload: Some(TsPayload::Pat(mpeg2ts::ts::payload::Pat {
            transport_stream_id: 1,
            version_number: VersionNumber::new(),
            table: vec![ProgramAssociation {
                program_num: 1,
                program_map_pid: Pid::new(PMT_PID)?,
            }],
        })),
    };
    writer.write_ts_packet(&pat)?;
    pat_cc.increment();

    let mut es_info = vec![EsInfo {
        stream_type: StreamType::H264,
        elementary_pid: Pid::new(VIDEO_PID)?,
        descriptors: vec![],
    }];
    if has_audio {
        es_info.push(EsInfo {
            stream_type: StreamType::AdtsAac,
            elementary_pid: Pid::new(AUDIO_PID)?,
            descriptors: vec![],
        });
    }

    let pmt = TsPacket {
        header: make_ts_header(Pid::new(PMT_PID)?, *pmt_cc),
        adaptation_field: None,
        payload: Some(TsPayload::Pmt(mpeg2ts::ts::payload::Pmt {
            program_num: 1,
            version_number: VersionNumber::new(),
            pcr_pid: Some(Pid::new(VIDEO_PID)?),
            program_info: vec![],
            es_info,
        })),
    };
    writer.write_ts_packet(&pmt)?;
    pmt_cc.increment();
    Ok(())
}

fn collect_message_payload(msg: &RtmpMessage) -> Vec<u8> {
    let mut out = Vec::with_capacity(msg.header().msg_len);
    for chunk in msg.chunks() {
        out.extend_from_slice(&chunk.payload());
    }
    out
}

fn parse_avc_decoder_config(data: &[u8]) -> Option<AvcConfig> {
    if data.len() < 7 {
        return None;
    }
    let mut i = 6usize;
    let sps_count = (data[5] & 0x1F) as usize;
    let mut sps = Vec::with_capacity(sps_count);
    for _ in 0..sps_count {
        if i + 2 > data.len() {
            return None;
        }
        let len = u16::from_be_bytes([data[i], data[i + 1]]) as usize;
        i += 2;
        if i + len > data.len() {
            return None;
        }
        sps.push(data[i..i + len].to_vec());
        i += len;
    }
    if i >= data.len() {
        return None;
    }
    let pps_count = data[i] as usize;
    i += 1;
    let mut pps = Vec::with_capacity(pps_count);
    for _ in 0..pps_count {
        if i + 2 > data.len() {
            return None;
        }
        let len = u16::from_be_bytes([data[i], data[i + 1]]) as usize;
        i += 2;
        if i + len > data.len() {
            return None;
        }
        pps.push(data[i..i + len].to_vec());
        i += len;
    }
    Some(AvcConfig { sps, pps })
}

fn avcc_to_annexb(data: &[u8], avc_cfg: Option<&AvcConfig>, prepend_cfg: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 256);
    if prepend_cfg && let Some(cfg) = avc_cfg {
        for nalu in &cfg.sps {
            out.extend_from_slice(&[0, 0, 0, 1]);
            out.extend_from_slice(nalu);
        }
        for nalu in &cfg.pps {
            out.extend_from_slice(&[0, 0, 0, 1]);
            out.extend_from_slice(nalu);
        }
    }

    let mut i = 0usize;
    while i + 4 <= data.len() {
        let len = u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;
        i += 4;
        if i + len > data.len() {
            break;
        }
        out.extend_from_slice(&[0, 0, 0, 1]);
        out.extend_from_slice(&data[i..i + len]);
        i += len;
    }
    out
}

fn contains_idr_nalu(data: &[u8]) -> bool {
    let mut i = 0usize;
    while i + 4 <= data.len() {
        let len = u32::from_be_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]) as usize;
        i += 4;
        if i + len > data.len() || len == 0 {
            break;
        }
        if data[i] & 0x1F == 5 {
            return true;
        }
        i += len;
    }
    false
}

fn parse_aac_audio_specific_config(data: &[u8]) -> Option<AacConfig> {
    if data.len() < 2 {
        return None;
    }
    let b0 = data[0];
    let b1 = data[1];
    let object_type = (b0 >> 3) & 0x1F;
    let sampling_frequency_index = ((b0 & 0x07) << 1) | ((b1 >> 7) & 0x01);
    let channel_config = (b1 >> 3) & 0x0F;
    Some(AacConfig {
        object_type,
        sampling_frequency_index,
        channel_config,
    })
}

fn with_adts_header(cfg: &AacConfig, raw_aac: &[u8]) -> Option<Vec<u8>> {
    let frame_len = raw_aac.len() + 7;
    if frame_len > 0x1FFF {
        return None;
    }
    let profile = cfg.object_type.saturating_sub(1) & 0x03;
    let sf_idx = cfg.sampling_frequency_index & 0x0F;
    let ch = cfg.channel_config & 0x07;
    let len = frame_len as u16;

    let mut adts = [0u8; 7];
    adts[0] = 0xFF;
    adts[1] = 0xF1;
    adts[2] = (profile << 6) | (sf_idx << 2) | ((ch >> 2) & 0x01);
    adts[3] = ((ch & 0x03) << 6) | (((len >> 11) & 0x03) as u8);
    adts[4] = ((len >> 3) & 0xFF) as u8;
    adts[5] = (((len & 0x07) << 5) as u8) | 0x1F;
    adts[6] = 0xFC;

    let mut out = Vec::with_capacity(frame_len);
    out.extend_from_slice(&adts);
    out.extend_from_slice(raw_aac);
    Some(out)
}

fn parse_signed_24(data: &[u8]) -> i32 {
    let raw = ((data[0] as i32) << 16) | ((data[1] as i32) << 8) | (data[2] as i32);
    if (raw & 0x80_0000) != 0 {
        raw | !0x00FF_FFFF
    } else {
        raw
    }
}

fn clamp_pts(dts_ms: u32, cts_ms: i32) -> u32 {
    if cts_ms >= 0 {
        dts_ms.saturating_add(cts_ms as u32)
    } else {
        dts_ms.saturating_sub((-cts_ms) as u32)
    }
}

fn ms_to_ts(ms: u32) -> Timestamp {
    Timestamp::new((ms as u64) * 90).expect("valid 90k timestamp")
}

fn pes_optional_header_len(header: &PesHeader) -> usize {
    3 + header.pts.map_or(0, |_| 5) + header.dts.map_or(0, |_| 5) + header.escr.map_or(0, |_| 6)
}

fn calc_pes_packet_len(header: &PesHeader, payload_len: usize) -> u16 {
    let total = pes_optional_header_len(header) + payload_len;
    if total > u16::MAX as usize {
        0
    } else {
        total as u16
    }
}

fn make_ts_header(pid: Pid, continuity_counter: ContinuityCounter) -> TsHeader {
    TsHeader {
        transport_error_indicator: false,
        transport_priority: false,
        pid,
        transport_scrambling_control: TransportScramblingControl::NotScrambled,
        continuity_counter,
    }
}
