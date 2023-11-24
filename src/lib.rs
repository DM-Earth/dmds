/// Module containing range mappings.
mod range;

/// Module containing in-memory IO handlers for testing.
#[cfg(test)]
pub mod mem_io_handle;

/// Module containing world implementation.
mod world;

/// Module containing tests.
#[cfg(test)]
mod tests;

use async_trait::async_trait;
use futures_lite::{AsyncRead, AsyncWrite};

pub use world::{iter::Iter, iter::Lazy, Chunk, Select, World};

/// Represents types stored directly in a dimensional world.
pub trait Data: Sized + Send + Sync + Unpin {
    /// Count of dimensions.
    const DIMS: usize;

    /// Gets the value of required dimension.
    ///
    /// Dimension index starts from 0, which should be
    /// a unique data such as the only id.
    fn dim(&self, dim: usize) -> u64;

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

/// Represents error variants produced by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// IO Error.
    #[error("io err: {0}")]
    Io(std::io::Error),
    /// Requesting value has been taken.
    #[error("requesting value has been taken")]
    ValueTaken,
    /// Requesting value not found.
    #[error("requesting value not found")]
    ValueNotFound,
    /// Requested iterator, or stream, has been updated.
    #[error("requested stream has been updated.")]
    IterUpdated {
        expected: usize,
        current: Option<usize>,
    },
    /// Given value out of range.
    #[error("value {value} out of range [{}, {}]", range.0, range.1)]
    ValueOutOfRange { range: (u64, u64), value: u64 },
}
