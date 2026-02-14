use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::trace;

use super::codec::{RtmpMessage, RtmpMessageIter};
use crate::error::Result;

pub type ObjectItem = (String, Amf0);

#[derive(Clone, Debug)]
pub enum Amf0 {
    String(String),
    Number(f64),
    Boolean(bool),
    Object(Vec<(String, Amf0)>),
    EcmaArray(Vec<(String, Amf0)>), // 添加 ECMA Array 类型
    Null,                           // 添加 Null 类型以支持正确解析
}

impl Amf0 {
    pub fn string(&self) -> Option<&str> {
        match self {
            Amf0::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_string(self) -> Option<String> {
        match self {
            Amf0::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn num(&self) -> Option<f64> {
        match self {
            Amf0::Number(n) => Some(*n),
            _ => None,
        }
    }

    pub fn boolean(&self) -> Option<bool> {
        match self {
            Amf0::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    pub fn object(&self) -> Option<&Vec<(String, Amf0)>> {
        match self {
            Amf0::Object(obj) => Some(obj),
            _ => None,
        }
    }

    pub fn into_object(self) -> Option<Vec<(String, Amf0)>> {
        match self {
            Amf0::Object(obj) => Some(obj),
            _ => None,
        }
    }
}

#[allow(unused)]
pub trait Value {
    fn string(&self) -> Option<(&str, &str)>;
    fn to_string(self) -> Option<(String, String)>;
    fn num(&self) -> Option<(&str, f64)>;
    fn boolean(&self) -> Option<(&str, bool)>;
    fn object(&self) -> Option<(&str, &Vec<ObjectItem>)>;
    fn to_object(self) -> Option<(String, Vec<ObjectItem>)>;
}

impl Value for ObjectItem {
    fn string(&self) -> Option<(&str, &str)> {
        self.1.string().map(|v| (self.0.as_str(), v))
    }

    fn to_string(self) -> Option<(String, String)> {
        self.1.into_string().map(|v| (self.0, v))
    }

    fn num(&self) -> Option<(&str, f64)> {
        self.1.num().map(|v| (self.0.as_str(), v))
    }

    fn boolean(&self) -> Option<(&str, bool)> {
        self.1.boolean().map(|v| (self.0.as_str(), v))
    }

    fn object(&self) -> Option<(&str, &Vec<ObjectItem>)> {
        self.1.object().map(|v| (self.0.as_str(), v))
    }

    fn to_object(self) -> Option<(String, Vec<ObjectItem>)> {
        self.1.into_object().map(|v| (self.0, v))
    }
}

fn write_value(b: &mut BytesMut, v: &Amf0) {
    match v {
        Amf0::String(s) => write_string(b, s),
        Amf0::Number(n) => write_number(b, *n),
        Amf0::Boolean(flag) => write_boolean(b, *flag),
        Amf0::Object(obj) => {
            let items: Vec<(&str, Amf0)> =
                obj.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
            write_object(b, &items);
        }
        Amf0::EcmaArray(arr) => {
            write_ecma_array(b, arr); // 添加对 ECMA Array 的写入支持
        }
        Amf0::Null => write_null(b), // 添加对 Null 类型的写入支持
    }
}

fn write_string(b: &mut BytesMut, s: &str) {
    b.put_u8(0x02);
    b.put_u16(s.len() as u16);
    b.extend_from_slice(s.as_bytes());
}
fn write_number(b: &mut BytesMut, n: f64) {
    b.put_u8(0x00);
    b.extend_from_slice(&n.to_be_bytes());
}
fn write_boolean(b: &mut BytesMut, v: bool) {
    b.put_u8(0x01);
    b.put_u8(if v { 1 } else { 0 });
}
fn write_null(b: &mut BytesMut) {
    b.put_u8(0x05);
}
fn write_object(b: &mut BytesMut, o: &[(&str, Amf0)]) {
    b.put_u8(0x03);
    for (k, v) in o {
        b.put_u16(k.len() as u16);
        b.extend_from_slice(k.as_bytes());
        write_value(b, v);
    }
    b.extend_from_slice(&[0, 0, 9]);
}

fn write_ecma_array(b: &mut BytesMut, arr: &Vec<(String, Amf0)>) {
    b.put_u8(0x08); // ECMA Array 类型标记
    b.put_u32(arr.len() as u32); // 32位长度前缀
    for (k, v) in arr {
        b.put_u16(k.len() as u16);
        b.extend_from_slice(k.as_bytes());
        write_value(b, v);
    }
    b.extend_from_slice(&[0, 0, 9]); // 结尾标记
}

pub struct AmfReader<'a> {
    data: &'a [u8],
}

impl<'a> AmfReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    fn read_string(&mut self) -> Result<String> {
        if self.peek() != Some(0x02) {
            return Err(("not string").into());
        }
        let len = u16::from_be_bytes([self.data[1], self.data[2]]) as usize;
        let s = std::str::from_utf8(&self.data[3..3 + len])?.to_string();
        trace!(value = %s, len = len, "read_string");
        self.data = &self.data[3 + len..];
        Ok(s)
    }

    fn read_number(&mut self) -> Result<f64> {
        if self.peek() != Some(0x00) {
            return Err(("not number").into());
        }
        let v = f64::from_be_bytes(self.data[1..9].try_into().unwrap());
        trace!(value = v, "read_number");
        self.data = &self.data[9..];
        Ok(v)
    }

    fn read_boolean(&mut self) -> Result<bool> {
        if self.peek() != Some(0x01) {
            return Err(("not boolean").into());
        }
        let v = self.data[1] != 0;
        trace!(value = v, "read_boolean");
        self.data = &self.data[2..];
        Ok(v)
    }

    fn read_null(&mut self) -> Result<Amf0> {
        if self.peek() != Some(0x05) {
            return Err(("not null").into());
        }
        trace!("read_null");
        self.data = &self.data[1..];
        Ok(Amf0::Null)
    }

    // 辅助函数：读取单个键值对
    fn read_key_value_pair(&mut self) -> Result<(String, Amf0)> {
        // 检查是否有足够的数据来读取键长度
        if self.data.len() < 2 {
            return Err(("incomplete key length").into());
        }

        let klen = u16::from_be_bytes([self.data[0], self.data[1]]) as usize;
        self.data = &self.data[2..];

        // 检查是否有足够的数据来读取键
        if self.data.len() < klen {
            return Err(("incomplete key").into());
        }

        let key = std::str::from_utf8(&self.data[..klen])
            .map_err(|_| "invalid utf-8 in key")?
            .to_string();
        self.data = &self.data[klen..];

        // 读取值
        let val = match self.peek() {
            Some(0x00) => Amf0::Number(self.read_number()?),
            Some(0x01) => Amf0::Boolean(self.read_boolean()?),
            Some(0x02) => Amf0::String(self.read_string()?),
            Some(0x03) => Amf0::Object(self.read_object()?),
            Some(0x08) => Amf0::EcmaArray(self.read_ecma_array()?),
            Some(0x05) => Amf0::Null, // 返回 Null 而不是 ()
            Some(t) => return Err(format!("unsupported amf type: {}", t).into()),
            None => return Err(("unexpected EOF").into()),
        };
        Ok((key, val))
    }

    fn read_object(&mut self) -> Result<Vec<(String, Amf0)>> {
        if self.peek() != Some(0x03) {
            return Err(("not object").into());
        }
        self.data = &self.data[1..];

        let mut v = Vec::new();
        loop {
            if self.data.len() >= 3 && self.data[..3] == [0, 0, 9] {
                self.data = &self.data[3..];
                break;
            }

            let (key, val) = self.read_key_value_pair()?;
            v.push((key, val));
        }

        trace!(pairs = v.len(), "read_object parsed key-value pairs");
        Ok(v)
    }

    fn read_ecma_array(&mut self) -> Result<Vec<(String, Amf0)>> {
        if self.peek() != Some(0x08) {
            return Err(("not ecma array").into());
        }
        self.data = &self.data[1..]; // 跳过类型标记

        // 读取32位长度
        if self.data.len() < 4 {
            return Err(("incomplete ecma array length").into());
        }
        let len =
            u32::from_be_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize;
        self.data = &self.data[4..];

        // 添加长度限制以防止OOM攻击
        const MAX_ECMA_ARRAY_LEN: usize = 16; // 设置最大长度限制
        if len > MAX_ECMA_ARRAY_LEN {
            return Err(format!(
                "ecma array length {} exceeds maximum allowed {}",
                len, MAX_ECMA_ARRAY_LEN
            )
            .into());
        }

        // 预分配容量以防止 OOM 攻击
        let mut v = Vec::with_capacity(len);

        // 读取 len 个键值对
        for _ in 0..len {
            let (key, val) = self.read_key_value_pair()?;
            v.push((key, val));
        }

        // 检查结尾标记 [0, 0, 9]
        if self.data.len() < 3 || self.data[..3] != [0, 0, 9] {
            return Err(("missing ecma array end marker").into());
        }
        self.data = &self.data[3..];

        trace!(pairs = v.len(), "read_ecma_array parsed key-value pairs");
        Ok(v)
    }

    fn peek(&self) -> Option<u8> {
        self.data.first().copied()
    }

    fn has_more(&self) -> bool {
        !self.data.is_empty()
    }
}

/* ================= RtmpCommand ================= */

#[derive(Clone, Debug)]
pub struct RtmpCommand {
    pub name: String,
    pub transaction_id: f64,
    pub command_object: Vec<(String, Amf0)>,
    pub args: Vec<Amf0>,
}

impl RtmpMessage {
    pub fn command(msg: &RtmpMessage) -> Result<RtmpCommand> {
        if msg.header().msg_type != 20 {
            return Err(("not a command message").into());
        }
        let payload = msg.payload();
        let mut reader = AmfReader::new(&payload);
        let name = reader.read_string()?;
        let transaction_id = reader.read_number()?;
        let command_object = if reader.peek() == Some(0x05) {
            reader.read_null()?;
            Vec::new()
        } else if reader.peek() == Some(0x08) {
            // 如果命令对象是 ECMA Array 类型
            reader.read_ecma_array()?
        } else {
            reader.read_object()?
        };
        let mut args = Vec::new();
        while reader.has_more() {
            match reader.peek() {
                Some(0x00) => args.push(Amf0::Number(reader.read_number()?)),
                Some(0x01) => args.push(Amf0::Boolean(reader.read_boolean()?)),
                Some(0x02) => args.push(Amf0::String(reader.read_string()?)),
                Some(0x03) => args.push(Amf0::Object(reader.read_object()?)),
                Some(0x08) => args.push(Amf0::EcmaArray(reader.read_ecma_array()?)), // 添加对 ECMA Array 参数的支持
                Some(0x05) => {
                    reader.read_null()?;
                    break;
                }
                _ => break,
            }
        }
        Ok(RtmpCommand {
            name,
            transaction_id,
            command_object,
            args,
        })
    }
}

impl RtmpCommand {
    pub fn new(name: impl Into<String>, transaction_id: f64) -> Self {
        Self {
            name: name.into(),
            transaction_id,
            command_object: Vec::new(),
            args: Vec::new(),
        }
    }

    // 连接命令 - 固定参数：fmsVer, capabilities
    pub fn connect(tx_id: f64, app: impl Into<String>, tc_url: impl Into<String>) -> Self {
        Self::new("connect", tx_id)
            .object("app", app.into())
            .object("tcUrl", tc_url.into())
            .object("fmsVer", "FMS/3,0,1,123")
            .object("capabilities", 31.0)
    }

    // 创建流命令
    pub fn create_stream(tx_id: f64) -> Self {
        Self::new("createStream", tx_id)
    }

    // 发布流命令 - 固定参数："live"
    pub fn publish(tx_id: f64, stream_name: impl Into<String>) -> Self {
        Self::new("publish", tx_id)
            .arg(stream_name.into())
            .arg("live")
    }

    // 释放流命令
    pub fn release_stream(tx_id: f64, stream_name: impl Into<String>) -> Self {
        Self::new("releaseStream", tx_id).arg(stream_name.into())
    }

    // FCPublish命令
    pub fn fc_publish(tx_id: f64, stream_name: impl Into<String>) -> Self {
        Self::new("FCPublish", tx_id).arg(stream_name.into())
    }

    // FCUnpublish命令
    pub fn fc_unpublish(tx_id: f64, stream_name: impl Into<String>) -> Self {
        Self::new("FCUnpublish", tx_id).arg(stream_name.into())
    }

    // 删除流命令
    pub fn delete_stream(tx_id: f64, stream_id: f64) -> Self {
        Self::new("deleteStream", tx_id).arg(stream_id)
    }

    // 成功响应命令 - 固定参数：level, code, description
    pub fn result_connect(tx_id: f64) -> Self {
        Self::new("_result", tx_id)
            .object("fmsVer", "FMS/3,0,1,123")
            .object("capabilities", 31.0)
            .arg(vec![
                ("level", "status"),
                ("code", "NetConnection.Connect.Success"),
                ("description", "Connection succeeded."),
            ])
    }

    pub fn result_create_stream(tx_id: f64, stream_id: u32) -> Self {
        Self::new("_result", tx_id).arg(stream_id as f64)
    }

    // 错误响应命令 - 固定参数：level, code, description
    pub fn error(tx_id: f64, code: impl Into<String>, description: impl Into<String>) -> Self {
        Self::new("_error", tx_id).arg(vec![
            ("level", "error".into()),
            ("code", code.into()),
            ("description", description.into()),
        ])
    }

    // 状态通知命令 - 固定参数：level, code, description
    pub fn on_status(
        code: impl Into<String>,
        level: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self::new("onStatus", 0.0).arg(vec![
            ("level", level.into()),
            ("code", code.into()),
            ("description", description.into()),
        ])
    }

    pub fn object<T: Into<Amf0>>(mut self, key: impl Into<String>, value: T) -> Self {
        self.command_object.push((key.into(), value.into()));
        self
    }

    pub fn arg<T: Into<Amf0>>(mut self, value: T) -> Self {
        self.args.push(value.into());
        self
    }

    pub fn payload(&self) -> Bytes {
        let mut buf = BytesMut::new();
        write_string(&mut buf, &self.name);
        write_number(&mut buf, self.transaction_id);
        if self.command_object.is_empty() {
            write_null(&mut buf);
        } else {
            let items: Vec<(&str, Amf0)> = self
                .command_object
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            write_object(&mut buf, &items);
        }
        for arg in &self.args {
            write_value(&mut buf, arg);
        }
        buf.freeze()
    }

    pub async fn send<S>(
        &self,
        w: &mut S,
        csid: u8,
        stream_id: u32,
        chunk_size: usize,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        for chunk in
            RtmpMessageIter::from_payload(csid, 0, 20, stream_id, chunk_size, &self.payload())
        {
            w.write_all(chunk.raw_bytes()).await?;
        }
        Ok(())
    }
}

impl From<&str> for Amf0 {
    fn from(s: &str) -> Self {
        Amf0::String(s.to_string())
    }
}

impl From<String> for Amf0 {
    fn from(s: String) -> Self {
        Amf0::String(s)
    }
}

impl From<f64> for Amf0 {
    fn from(n: f64) -> Self {
        Amf0::Number(n)
    }
}

impl From<bool> for Amf0 {
    fn from(b: bool) -> Self {
        Amf0::Boolean(b)
    }
}

impl From<u32> for Amf0 {
    fn from(n: u32) -> Self {
        Amf0::Number(n as f64)
    }
}

impl<K: Into<String>, V: Into<Amf0>> From<Vec<(K, V)>> for Amf0 {
    fn from(obj: Vec<(K, V)>) -> Self {
        Amf0::Object(obj.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}
