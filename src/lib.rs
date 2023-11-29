/// Module containing range mappings.
mod range;

/// Module containing in-memory IO handlers for testing.
pub mod mem_io_handle;

/// Module containing world implementation.
mod world;

/// Module containing tests.
#[cfg(test)]
mod tests;

use std::ops::Deref;

use async_trait::async_trait;
use futures_lite::AsyncRead;

pub use world::{iter::Iter, iter::Lazy, Chunk, Dim, Select, World};

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

impl<const DIMS: usize> Data for [u64; DIMS] {
    const DIMS: usize = DIMS;

    #[inline]
    fn dim(&self, dim: usize) -> u64 {
        self[dim]
    }

    fn decode<B: bytes::Buf>(dims: &[u64], _buf: B) -> std::io::Result<Self> {
        let mut this = [0; DIMS];
        for (t, d) in this.iter_mut().zip(dims.iter()) {
            *t = *d
        }
        Ok(this)
    }

    #[inline]
    fn encode<B: bytes::BufMut>(&self, _buf: B) -> std::io::Result<()> {
        Ok(())
    }
}

/// Trait representing IO handlers for dimensional worlds.
#[async_trait]
pub trait IoHandle: Send + Sync {
    /// Type of reader.
    type Read<'a>: AsyncRead + Unpin + Send + Sync + 'a
    where
        Self: 'a;

    /// Gets reader for given chunk position.
    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Read<'_>>;
}

impl<P, T> IoHandle for P
where
    T: IoHandle + 'static,
    P: Deref<Target = T> + Send + Sync,
{
    type Read<'a> = T::Read<'a> where Self: 'a;

    #[doc = " Gets reader for given chunk position."]
    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    #[inline]
    fn read_chunk<'life0, 'async_trait, const DIMS: usize>(
        &'life0 self,
        pos: [usize; DIMS],
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = std::io::Result<Self::Read<'_>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.deref().read_chunk(pos)
    }
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

type Result<T> = std::result::Result<T, Error>;
