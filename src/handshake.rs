use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::debug;

pub const HANDSHAKE_SIZE: usize = 1536;

// Generate random bytes using std::random (Nightly)
fn gen_random_buffer(buf: &mut [u8]) {
    // Requires #![feature(random)]
    for b in buf.iter_mut() {
        *b = std::random::random(..);
    }
}

pub async fn handshake_with_client<S>(client: &mut S) -> Result<()> 
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin
{
    // RTMP Server Handshake
    // 1. Read C0
    let mut c0 = [0u8; 1];
    client.read_exact(&mut c0).await.context("read C0")?;
    let version = c0[0];
    debug!(version = version, "read C0 from client");

    // 2. Read C1
    let mut c1 = vec![0u8; HANDSHAKE_SIZE];
    client.read_exact(&mut c1).await.context("read C1")?;
    debug!("read C1 from client");

    // 3. Write S0 + S1 + S2
    // S0 = version (usually 3)
    // S1 = time(4) + zero(4) + random(1528)
    // S2 = c1_time(4) + c1_time2(4) + c1_random(1528) -> basically Echo of C1
    let mut s0s1s2 = Vec::with_capacity(1 + HANDSHAKE_SIZE * 2);
    s0s1s2.push(version); // S0

    // S1 construction
    let mut s1 = vec![0u8; HANDSHAKE_SIZE];
    // time (4 bytes) - mostly irrelevant for handshake but good to have
    let uptime = 0u32;
    s1[0..4].copy_from_slice(&uptime.to_be_bytes());
    // next 4 bytes are zero
    s1[4..8].copy_from_slice(&[0, 0, 0, 0]);
    // random fill
    gen_random_buffer(&mut s1[8..]);
    s0s1s2.extend_from_slice(&s1);

    // S2 construction = C1 (Echo)
    s0s1s2.extend_from_slice(&c1);

    client.write_all(&s0s1s2).await.context("write S0S1S2")?;
    debug!("sent S0+S1+S2 to client");

    // 4. Read C2
    let mut c2 = vec![0u8; HANDSHAKE_SIZE];
    client.read_exact(&mut c2).await.context("read C2")?;
    debug!("read C2 from client");

    // In a strict server, we'd verify C2 matches S1, but for a proxy we generally accept.
    debug!("handshake with client complete");
    Ok(())
}

pub async fn handshake_with_server<S>(server: &mut S) -> Result<()> 
where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin
{
    // RTMP Client Handshake
    // 1. Write C0 + C1
    let mut c0c1 = Vec::with_capacity(1 + HANDSHAKE_SIZE);
    c0c1.push(3); // C0: version 3

    let mut c1 = vec![0u8; HANDSHAKE_SIZE];
    // time (4 bytes)
    let uptime = 0u32;
    c1[0..4].copy_from_slice(&uptime.to_be_bytes());
    // zero (4 bytes)
    c1[4..8].copy_from_slice(&[0, 0, 0, 0]);
    // random
    gen_random_buffer(&mut c1[8..]);
    c0c1.extend_from_slice(&c1);

    server
        .write_all(&c0c1)
        .await
        .context("write C0C1 to server")?;
    debug!("sent C0+C1 to server");

    // 2. Read S0 + S1 + S2
    let mut s0 = [0u8; 1];
    server.read_exact(&mut s0).await.context("read S0")?;

    let mut s1 = vec![0u8; HANDSHAKE_SIZE];
    server.read_exact(&mut s1).await.context("read S1")?;

    let mut s2 = vec![0u8; HANDSHAKE_SIZE];
    server.read_exact(&mut s2).await.context("read S2")?;
    debug!("read S0+S1+S2 from server");

    // 3. Write C2 (Echo of S1)
    server
        .write_all(&s1)
        .await
        .context("write C2 to server")?;
    debug!("sent C2 to server");

    debug!("handshake with server complete");
    Ok(())
}
