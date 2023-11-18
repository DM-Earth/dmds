mod range;
pub mod world;

use async_trait::async_trait;
use futures_lite::{AsyncRead, AsyncWrite};

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
    fn encode<B: bytes::BufMut>(&self, dims: &[u64], buf: B) -> std::io::Result<()>;
}

#[async_trait]
pub trait IoHandle: Send + Sync {
    type Read: AsyncRead + Unpin + Send + Sync;
    type Write: WriteFinish + Unpin + Send + Sync;

    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Read>;

    async fn write_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Write>;
}

/// Trait representing IO types that perform
/// some async actions after all bytes are wrote.
pub trait WriteFinish: AsyncWrite {
    fn poll_finish(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>>;
}

#[derive(Debug)]
struct UnpinRwlRead<'a, T: ?Sized>(async_lock::futures::Read<'a, T>);

impl<T: ?Sized + Unpin> Unpin for UnpinRwlRead<'_, T> {}
impl<'a, T: ?Sized + Unpin> std::future::Future for UnpinRwlRead<'a, T> {
    type Output = <async_lock::futures::Read<'a, T> as std::future::Future>::Output;

    #[inline]
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unsafe { std::pin::Pin::new_unchecked(&mut self.get_mut().0).poll(cx) }
    }
}
