use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::frame::{Frame, ProtoError};

/// Maximum `length` field (type + body). 16 KiB body plus a small header.
pub const MAX_FRAME_SIZE: usize = 16 * 1024;

pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Frame, ProtoError> {
    let mut lenb = [0u8; 4];
    r.read_exact(&mut lenb).await?;
    let len = u32::from_be_bytes(lenb) as usize;
    if len == 0 || len > MAX_FRAME_SIZE {
        return Err(ProtoError::BadLength(len));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Frame::decode(&buf)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    frame: &Frame,
) -> Result<(), ProtoError> {
    let payload = frame.encode();
    if payload.len() > MAX_FRAME_SIZE {
        return Err(ProtoError::BadLength(payload.len()));
    }
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&payload);
    w.write_all(&buf).await?;
    w.flush().await?;
    Ok(())
}
