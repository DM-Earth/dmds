use std::collections::HashMap;

use futures_lite::{AsyncRead, AsyncWrite};

use crate::IoHandle;

/// A simple in-memory storage implementing [`IoHandle`].
///
/// This is only for testing.
#[derive(Debug, Default)]
pub struct MemStorage {
    chunks: async_lock::RwLock<HashMap<String, (u32, Vec<u8>)>>,
}

impl MemStorage {
    /// Creates a new [`MemStorage`].
    #[inline]
    pub fn new() -> Self {
        Default::default()
    }

    /// Writes a chunk to this storage.
    pub async fn write_chunk<const DIMS: usize>(
        &self,
        data_version: u32,
        pos: [usize; DIMS],
    ) -> std::io::Result<Writer<'_>> {
        let mut chunk = String::new();
        for dim in pos.iter() {
            chunk.push_str(&dim.to_string());
            chunk.push('_');
        }
        chunk.pop();

        let mut write = self.chunks.write().await;
        write.insert(chunk.to_owned(), (data_version, vec![]));

        Ok(Writer {
            chunks: write,
            chunk,
        })
    }
}

impl IoHandle for MemStorage {
    type Read<'a> = Reader<'a> where Self: 'a;

    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<(u32, Self::Read<'_>)> {
        let mut chunk = String::new();
        for dim in pos.iter() {
            chunk.push_str(&dim.to_string());
            chunk.push('_');
        }
        chunk.pop();

        let read = self.chunks.read().await;
        if !read.contains_key(&chunk) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "chunk not found",
            ));
        }

        Ok((
            read[&chunk].0,
            Reader {
                chunks: read,
                chunk,
                ptr: 0,
            },
        ))
    }
}

/// A reader for [`MemStorage`].
#[derive(Debug)]
pub struct Reader<'a> {
    chunks: async_lock::RwLockReadGuard<'a, HashMap<String, (u32, Vec<u8>)>>,
    chunk: String,
    ptr: usize,
}

impl AsyncRead for Reader<'_> {
    #[inline]
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let mut chunk = &this.chunks.get(&this.chunk).unwrap().1[this.ptr..];
        let res = futures_lite::AsyncRead::poll_read(std::pin::Pin::new(&mut chunk), cx, buf);
        if let std::task::Poll::Ready(Ok(len)) = res {
            this.ptr += len;
        }
        res
    }

    #[inline]
    fn poll_read_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let mut chunk = &this.chunks.get(&this.chunk).unwrap().1[..];
        futures_lite::AsyncRead::poll_read_vectored(std::pin::Pin::new(&mut chunk), cx, bufs)
    }
}

/// A writer for [`MemStorage`].
#[derive(Debug)]
pub struct Writer<'a> {
    chunks: async_lock::RwLockWriteGuard<'a, HashMap<String, (u32, Vec<u8>)>>,
    chunk: String,
}

impl AsyncWrite for Writer<'_> {
    #[inline]
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        futures_lite::AsyncWrite::poll_write(
            std::pin::Pin::new(&mut this.chunks.get_mut(&this.chunk).unwrap().1),
            cx,
            buf,
        )
    }

    #[inline]
    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        futures_lite::AsyncWrite::poll_write_vectored(
            std::pin::Pin::new(&mut this.chunks.get_mut(&this.chunk).unwrap().1),
            cx,
            bufs,
        )
    }

    #[inline]
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        futures_lite::AsyncWrite::poll_flush(
            std::pin::Pin::new(&mut this.chunks.get_mut(&this.chunk).unwrap().1),
            cx,
        )
    }

    #[inline]
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        futures_lite::AsyncWrite::poll_close(
            std::pin::Pin::new(&mut this.chunks.get_mut(&this.chunk).unwrap().1),
            cx,
        )
    }
}
