mod range;
pub mod world;

use async_trait::async_trait;
use futures_lite::{AsyncRead, AsyncWrite};

/// Represents types stored directly in a dimensional world.
pub trait Data: Sized + Send + Sync {
    /// Count of dimensions.
    const DIMS: usize;

    /// Gets the value of given dimension.
    ///
    /// Dimension index starts from 0, which should be
    /// a unique data such as the only id.
    fn value_of(&self, dim: usize) -> u64;

    /// Decode this type from given `Read` and dimensional values.
    fn decode<R: std::io::Read>(&self, dims: &[u64], read: &mut R) -> std::io::Result<Self>;

    /// Encode this type into bytes with `Write`.
    ///
    /// Note: You don't need to encode dimensional values.
    /// They will be encoded automatically.
    fn encode<W: std::io::Write>(&self, dims: &[u64], write: &mut W) -> std::io::Result<()>;
}

#[async_trait]
pub trait IoHandle: Send + Sync {
    type Read: AsyncRead + Unpin + Send + Sync;
    type Write: AsyncWrite + Unpin + Send + Sync;

    async fn read_chunk(&self, pos: &[usize]) -> futures_lite::io::Result<Self::Read>;
    async fn write_chunk(&self, pos: &[usize]) -> futures_lite::io::Result<Self::Write>;
}
