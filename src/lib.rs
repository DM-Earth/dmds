mod range;
pub mod world;

use async_trait_fn::{async_trait, unboxed};
use futures_lite::{AsyncRead, AsyncWrite};

/// Represents types stored directly in a dimensional world.
pub trait Element {
    /// Count of dimensions.
    const DIMS: usize;

    /// Gets the value of given dimension.
    ///
    /// dimension index starts from 0, which should be
    /// a unique data such as the only id.
    fn value_of(&self, dim: usize) -> u64;
}

#[async_trait]
pub trait IoHandle {
    type Read: AsyncRead;
    type Write: AsyncWrite;

    #[unboxed]
    async fn read_chunk(&self, pos: &[usize]) -> Option<Self::Read>;

    #[unboxed]
    async fn write_chunk(&self, pos: &[usize]) -> Option<Self::Write>;
}
