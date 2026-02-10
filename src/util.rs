use bytes::BytesMut;
use std::net::{Ipv4Addr, Ipv6Addr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

use crate::error::{Context, Result};

pub trait PutU24 {
    fn put_u24(&mut self, v: u32);
}
impl PutU24 for BytesMut {
    fn put_u24(&mut self, v: u32) {
        self.extend_from_slice(&v.to_be_bytes()[1..]);
    }
}

pub async fn try_handle_socks5(stream: &mut TcpStream) -> Result<Option<String>> {
    let mut buf = [0u8; 2];
    stream.peek(&mut buf).await?;

    if buf[0] != 5 {
        return Ok(None);
    }

    stream.read_exact(&mut buf).await?;
    let nmethods = buf[1] as usize;
    let mut methods = vec![0u8; nmethods];
    stream.read_exact(&mut methods).await?;

    stream.write_all(&[5, 0]).await?;

    let mut req = [0u8; 4];
    stream.read_exact(&mut req).await?;

    let dest_addr = match req[3] {
        1 => {
            let mut ip = [0u8; 4];
            stream.read_exact(&mut ip).await?;
            Ipv4Addr::from(ip).to_string()
        }
        3 => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await?;
            let mut domain = vec![0u8; len[0] as usize];
            stream.read_exact(&mut domain).await?;
            String::from_utf8(domain).context("invalid domain")?
        }
        4 => {
            let mut ip = [0u8; 16];
            stream.read_exact(&mut ip).await?;
            Ipv6Addr::from(ip).to_string()
        }
        _ => return Err("invalid address type".into()),
    };

    let mut port = [0u8; 2];
    stream.read_exact(&mut port).await?;
    let port = u16::from_be_bytes(port);

    let full_addr = if port == 1935 {
        dest_addr
    } else {
        format!("{}:{}", dest_addr, port)
    };
    info!("SOCKS5 target: {}", full_addr);

    stream.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;

    Ok(Some(full_addr))
}

// 平台相关的原始目的地址获取
#[cfg(target_os = "linux")]
pub fn get_original_destination(socket: &TcpStream) -> Result<String> {
    use nix::sys::socket::{SockaddrIn, SockaddrIn6, getsockopt, sockopt};
    use std::os::unix::io::{AsRawFd, BorrowedFd};

    let fd = unsafe { BorrowedFd::borrow_raw(socket.as_raw_fd()) };

    // 尝试 IPv4
    if let Ok(addr) = getsockopt(&fd, sockopt::OriginalDst) {
        return Ok(SockaddrIn::from(addr).to_string());
    }

    // 尝试 IPv6
    if let Ok(addr) = getsockopt(&fd, sockopt::Ip6tOriginalDst) {
        return Ok(SockaddrIn6::from(addr).to_string());
    }

    Err(("Failed to get original destination").into())
}

#[cfg(not(target_os = "linux"))]
pub fn get_original_destination(_socket: &TcpStream) -> Result<String> {
    Err(("Platform not supported").into())
}
