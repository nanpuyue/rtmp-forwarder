use anyhow::{Result, anyhow};
use bytes::{BufMut, BytesMut};
use tracing::trace;

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

    pub fn read_null(&mut self) -> Result<()> {
        amf_read_null(&mut self.data)
    }

    pub fn read_object(&mut self) -> Result<Vec<(String, Amf0)>> {
        amf_read_object(&mut self.data)
    }
}
