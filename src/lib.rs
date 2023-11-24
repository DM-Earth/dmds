mod range;

/// Module containing in-memory IO handlers for testing.
#[cfg(test)]
pub mod mem_io_handle;
/// Module containing world implementation.
pub mod world;

/// Module containing tests.
#[cfg(test)]
mod tests;

use async_trait::async_trait;
use futures_lite::{AsyncRead, AsyncWrite};

pub use range::Error as RangeError;

/// Represents types stored directly in a dimensional world.
pub trait Data: Sized + Send + Sync + Unpin {
    /// Count of dimensions.
    const DIMS: usize;

    /// Gets the value of given dimension.
    ///
    /// Dimension index starts from 0, which should be
    /// a unique data such as the only id.
    fn value_of(&self, dim: usize) -> u64;

    /// Decode this type from given `Read` and dimensional values.
    fn decode<B: bytes::Buf>(dims: &[u64], buf: B) -> std::io::Result<Self>;

    /// Encode this type into bytes buffer.
    ///
    /// Note: You don't need to encode dimensional values.
    /// They will be encoded automatically.
    fn encode<B: bytes::BufMut>(&self, buf: B) -> std::io::Result<()>;
}

/// Trait representing IO handlers for dimensional worlds.
#[async_trait]
pub trait IoHandle: Send + Sync {
    /// Type of reader.
    type Read<'a>: AsyncRead + Unpin + Send + Sync + 'a
    where
        Self: 'a;

    /// Type of writer.
    type Write<'a>: WriteFinish + Unpin + Send + Sync + 'a
    where
        Self: 'a;

    /// Gets reader for given chunk position.
    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Read<'_>>;

    /// Gets writer for given chunk position.
    async fn write_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Write<'_>>;
}

/// Trait representing IO types that perform
/// some async actions after all bytes are wrote.
pub trait WriteFinish: AsyncWrite {
    /// Polls the async action.
    fn poll_finish(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>>;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("chunk position out of bound: {0}")]
    PosOutOfBound(RangeError),
}
