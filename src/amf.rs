use anyhow::{Result, anyhow};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::trace;

use crate::rtmp_codec::{RtmpMessage, RtmpMessageIter};

/* ================= AMF0 ================= */

// Peek AMF0 command name without mutating the original slice
pub fn amf_command_name(payload: &[u8]) -> Result<String> {
    let mut tmp = payload;
    let cmd = amf_read_string(&mut tmp)?;
    Ok(cmd)
}

/* ================= AMF0 helpers ================= */

#[derive(Clone, Debug)]
pub enum Amf0 {
    String(String),
    Number(f64),
    Boolean(bool),
    Object(Vec<(String, Amf0)>),
}

pub fn amf_read_string(p: &mut &[u8]) -> Result<String> {
    // AMF0 string: type 0x02, 2-byte big-endian length, then utf8 bytes
    if p.is_empty() || p[0] != 0x02 {
        return Err(anyhow!("not string"));
    }
    let len = u16::from_be_bytes([p[1], p[2]]) as usize;
    let s = std::str::from_utf8(&p[3..3 + len])?.to_string();
    trace!(value = %s, len = len, "amf_read_string");
    *p = &p[3 + len..];
    Ok(s)
}

pub fn amf_read_number(p: &mut &[u8]) -> Result<f64> {
    // AMF0 number: type 0x00 followed by 8-byte big-endian IEEE-754
    if p.is_empty() || p[0] != 0x00 {
        return Err(anyhow!("not number"));
    }
    let v = f64::from_be_bytes(p[1..9].try_into().unwrap());
    trace!(value = v, "amf_read_number");
    *p = &p[9..];
    Ok(v)
}

pub fn amf_read_boolean(p: &mut &[u8]) -> Result<bool> {
    // AMF0 boolean: type 0x01 followed by 1 byte (0=false, !=0=true)
    if p.is_empty() || p[0] != 0x01 {
        return Err(anyhow!("not boolean"));
    }
    let v = p[1] != 0;
    trace!(value = v, "amf_read_boolean");
    *p = &p[2..];
    Ok(v)
}

pub fn amf_read_null(p: &mut &[u8]) -> Result<()> {
    if p.is_empty() || p[0] != 0x05 {
        return Err(anyhow!("not null"));
    }
    trace!("amf_read_null");
    *p = &p[1..];
    Ok(())
}

pub fn amf_read_object(p: &mut &[u8]) -> Result<Vec<(String, Amf0)>> {
    // AMF0 object: 0x03 marker, then repeated key-value pairs, terminated by 0x00 0x00 0x09
    if p.is_empty() || p[0] != 0x3 {
        return Err(anyhow!("not object"));
    }
    *p = &p[1..];
    let mut v = Vec::new();
    loop {
        if p.len() >= 3 && p[..3] == [0, 0, 9] {
            *p = &p[3..];
            break;
        }
        let klen = u16::from_be_bytes([p[0], p[1]]) as usize;
        let key = std::str::from_utf8(&p[2..2 + klen])?.to_string();
        *p = &p[2 + klen..];
        let val = match p[0] {
            0x00 => Amf0::Number(amf_read_number(p)?),
            0x01 => Amf0::Boolean(amf_read_boolean(p)?),
            0x02 => Amf0::String(amf_read_string(p)?),
            0x03 => Amf0::Object(amf_read_object(p)?),
            _ => return Err(anyhow!("unsupported amf type: {}", p[0])),
        };
        v.push((key, val));
    }
    trace!(pairs = v.len(), "amf_read_object parsed key-value pairs");
    Ok(v)
}

pub fn amf_write_value(b: &mut BytesMut, v: &Amf0) {
    match v {
        Amf0::String(s) => amf_write_string(b, s),
        Amf0::Number(n) => amf_write_number(b, *n),
        Amf0::Boolean(flag) => amf_write_boolean(b, *flag),
        Amf0::Object(obj) => {
            let items: Vec<(&str, Amf0)> =
                obj.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
            amf_write_object(b, &items);
        }
    }
}

pub fn amf_write_string(b: &mut BytesMut, s: &str) {
    b.put_u8(0x02);
    b.put_u16(s.len() as u16);
    b.extend_from_slice(s.as_bytes());
}
pub fn amf_write_number(b: &mut BytesMut, n: f64) {
    b.put_u8(0x00);
    b.extend_from_slice(&n.to_be_bytes());
}
pub fn amf_write_boolean(b: &mut BytesMut, v: bool) {
    b.put_u8(0x01);
    b.put_u8(if v { 1 } else { 0 });
}
pub fn amf_write_null(b: &mut BytesMut) {
    b.put_u8(0x05);
}
pub fn amf_write_object(b: &mut BytesMut, o: &[(&str, Amf0)]) {
    b.put_u8(0x03);
    for (k, v) in o {
        b.put_u16(k.len() as u16);
        b.extend_from_slice(k.as_bytes());
        amf_write_value(b, v);
    }
    b.extend_from_slice(&[0, 0, 9]);
}
pub struct AmfReader<'a> {
    data: &'a [u8],
}

impl<'a> AmfReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn read_string(&mut self) -> Result<String> {
        amf_read_string(&mut self.data)
    }

    pub fn read_number(&mut self) -> Result<f64> {
        amf_read_number(&mut self.data)
    }

    pub fn read_boolean(&mut self) -> Result<bool> {
        amf_read_boolean(&mut self.data)
    }

    pub fn read_null(&mut self) -> Result<()> {
        amf_read_null(&mut self.data)
    }

    pub fn read_object(&mut self) -> Result<Vec<(String, Amf0)>> {
        amf_read_object(&mut self.data)
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
            return Err(anyhow!("not a command message"));
        }
        let payload = msg.payload();
        let mut reader = AmfReader::new(&payload);
        let name = reader.read_string()?;
        let transaction_id = reader.read_number()?;
        let command_object = if reader.data.first() == Some(&0x05) {
            reader.read_null()?;
            Vec::new()
        } else {
            reader.read_object()?
        };
        let mut args = Vec::new();
        while !reader.data.is_empty() {
            match reader.data[0] {
                0x00 => args.push(Amf0::Number(reader.read_number()?)),
                0x01 => args.push(Amf0::Boolean(reader.read_boolean()?)),
                0x02 => args.push(Amf0::String(reader.read_string()?)),
                0x03 => args.push(Amf0::Object(reader.read_object()?)),
                0x05 => {
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
        amf_write_string(&mut buf, &self.name);
        amf_write_number(&mut buf, self.transaction_id);
        if self.command_object.is_empty() {
            amf_write_null(&mut buf);
        } else {
            let items: Vec<(&str, Amf0)> = self
                .command_object
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            amf_write_object(&mut buf, &items);
        }
        for arg in &self.args {
            amf_write_value(&mut buf, arg);
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
            RtmpMessageIter::new_with_payload(csid, 0, 20, stream_id, chunk_size, &self.payload())
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

impl<K: Into<String>, V: Into<Amf0>> From<Vec<(K, V)>> for Amf0 {
    fn from(obj: Vec<(K, V)>) -> Self {
        Amf0::Object(obj.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}
