use bytes::BytesMut;
use tokio::net::TcpStream;

use crate::error::Result;
pub trait PutU24 {
    fn put_u24(&mut self, v: u32);
}
impl PutU24 for BytesMut {
    fn put_u24(&mut self, v: u32) {
        self.extend_from_slice(&v.to_be_bytes()[1..]);
    }
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
