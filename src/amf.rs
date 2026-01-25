use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use tracing::{debug, trace};

/* ================= AMF0 ================= */

pub fn rewrite_amf(payload: &[u8], app: &str, stream: &str) -> Result<Option<(BytesMut, String)>> {
    // Attempt to read AMF0 command name and possibly rewrite it.
    let mut p = payload;
    let cmd = amf_read_string(&mut p)?;
    debug!(original_command = %cmd, "AMF0 command received");

    match cmd.as_str() {
        "connect" => {
            // connect(tx, object)
            let tx = amf_read_number(&mut p)?;
            let mut obj = amf_read_object(&mut p)?;
            // find existing app value if present
            let mut old_app: Option<String> = None;
            let mut found = false;
            for (k, v) in obj.iter_mut() {
                if k == "app" {
                    if let Amf0::String(s) = v.clone() {
                        old_app = Some(s);
                    }
                    *v = Amf0::String(app.into());
                    found = true;
                    break;
                }
            }
            if !found {
                obj.push(("app".into(), Amf0::String(app.into())));
            }

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "connect");
            amf_write_number(&mut out, tx);
            amf_write_object(&mut out, &obj);
            let old = old_app.unwrap_or_else(|| "".into());
            let summary = format!("connect: rewrote app \"{}\" to \"{}\"", old, app);
            Ok(Some((out, summary)))
        }
        "publish" => {
            // publish(tx, null, name)
            let tx = amf_read_number(&mut p)?;
            amf_read_null(&mut p)?;
            let orig_stream = amf_read_string(&mut p)?; // original stream name

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "publish");
            amf_write_number(&mut out, tx);
            amf_write_null(&mut out);
            amf_write_string(&mut out, stream);
            let summary = format!("publish: rewrote stream \"{}\" to \"{}\"", orig_stream, stream);
            Ok(Some((out, summary)))
        }
        "releaseStream" => {
            // releaseStream may come as (tx, name) or (tx, null, name) or (name)
            let mut p2 = p;
            let mut tx_opt: Option<f64> = None;
            if !p2.is_empty() && p2[0] == 0x00 {
                tx_opt = Some(amf_read_number(&mut p2)?);
            }
            let mut had_null = false;
            if !p2.is_empty() && p2[0] == 0x05 {
                amf_read_null(&mut p2)?;
                had_null = true;
            }
            let orig_stream = if !p2.is_empty() && p2[0] == 0x02 {
                amf_read_string(&mut p2)?
            } else {
                String::new()
            };

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "releaseStream");
            if let Some(tx) = tx_opt {
                amf_write_number(&mut out, tx);
            }
            if had_null {
                amf_write_null(&mut out);
            }
            amf_write_string(&mut out, stream);
            let summary = format!("releaseStream: rewrote stream \"{}\" to \"{}\"", orig_stream, stream);
            Ok(Some((out, summary)))
        }
        "FCPublish" => {
            // FCPublish may be sent as (tx, name) or (name) or (tx, null, name)
            let mut p2 = p;
            let mut tx_opt: Option<f64> = None;
            if !p2.is_empty() && p2[0] == 0x00 {
                tx_opt = Some(amf_read_number(&mut p2)?);
            }
            let mut had_null = false;
            if !p2.is_empty() && p2[0] == 0x05 {
                amf_read_null(&mut p2)?;
                had_null = true;
            }
            let orig_stream = if !p2.is_empty() && p2[0] == 0x02 {
                amf_read_string(&mut p2)?
            } else {
                String::new()
            };

            let mut out = BytesMut::new();
            amf_write_string(&mut out, "FCPublish");
            if let Some(tx) = tx_opt {
                amf_write_number(&mut out, tx);
            }
            if had_null {
                amf_write_null(&mut out);
            }
            amf_write_string(&mut out, stream);
            let summary = format!("FCPublish: rewrote stream \"{}\" to \"{}\"", orig_stream, stream);
            Ok(Some((out, summary)))
        }
        _ => {
            debug!(command = %cmd, "no rewrite performed for AMF0 command");
            Ok(None)
        }
    }
}

// Peek AMF0 command name without mutating the original slice
pub fn amf_command_name(payload: &[u8]) -> Result<String> {
    let mut tmp = payload;
    let cmd = amf_read_string(&mut tmp)?;
    Ok(cmd)
}

/* ================= AMF0 helpers ================= */

#[derive(Clone)]
pub enum Amf0 {
    String(String),
    Number(f64),
    Boolean(bool),
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
        match p[0] {
            0x00 => v.push((key, Amf0::Number(amf_read_number(p)?))),
            0x01 => v.push((key, Amf0::Boolean(amf_read_boolean(p)?))),
            0x02 => v.push((key, Amf0::String(amf_read_string(p)?))),
            _ => return Err(anyhow!("unsupported amf type: {}", p[0])),
        }
    }
    trace!(pairs = v.len(), "amf_read_object parsed key-value pairs");
    Ok(v)
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
pub fn amf_write_object(b: &mut BytesMut, o: &[(String, Amf0)]) {
    b.put_u8(0x03);
    for (k, v) in o {
        b.put_u16(k.len() as u16);
        b.extend_from_slice(k.as_bytes());
        match v {
            Amf0::String(s) => amf_write_string(b, s),
            Amf0::Number(n) => amf_write_number(b, *n),
            Amf0::Boolean(flag) => amf_write_boolean(b, *flag),
        }
    }
    b.extend_from_slice(&[0, 0, 9]);
}
