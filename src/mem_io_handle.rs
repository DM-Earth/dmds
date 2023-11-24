use std::collections::HashMap;

use async_trait::async_trait;
use futures_lite::{AsyncRead, AsyncWrite};

use crate::{IoHandle, WriteFinish};

/// A simple in-memory storage implementing [`IoHandle`].
///
/// This is only for testing.
#[derive(Debug)]
pub struct MemStorage {
    chunks: async_lock::RwLock<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl IoHandle for MemStorage {
    type Read<'a> = Reader<'a> where Self: 'a;

    type Write<'a> = Writer<'a> where Self: 'a;

    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Read<'_>> {
        let mut chunk = String::new();
        for dim in pos.iter() {
            chunk.push_str(&dim.to_string());
            chunk.push('_');
        }
        chunk.pop();

        Ok(Reader {
            chunks: self.chunks.read().await,
            chunk,
        })
    }

    async fn write_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Write<'_>> {
        let mut chunk = String::new();
        for dim in pos.iter() {
            chunk.push_str(&dim.to_string());
            chunk.push('_');
        }
        chunk.pop();

        Ok(Writer {
            chunks: self.chunks.write().await,
            chunk,
        })
    }
}

/// A reader for [`Mem`].
#[derive(Debug)]
pub struct Reader<'a> {
    chunks: async_lock::RwLockReadGuard<'a, HashMap<String, Vec<u8>>>,
    chunk: String,
}

impl AsyncRead for Reader<'_> {
    #[inline]
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let mut chunk = &this.chunks.get(&this.chunk).unwrap()[..];
        futures_lite::AsyncRead::poll_read(std::pin::Pin::new(&mut chunk), cx, buf)
    }

    #[inline]
    fn poll_read_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let mut chunk = &this.chunks.get(&this.chunk).unwrap()[..];
        futures_lite::AsyncRead::poll_read_vectored(std::pin::Pin::new(&mut chunk), cx, bufs)
    }
}

/// A writer for [`Mem`].
#[derive(Debug)]
pub struct Writer<'a> {
    chunks: async_lock::RwLockWriteGuard<'a, HashMap<String, Vec<u8>>>,
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
            std::pin::Pin::new(this.chunks.get_mut(&this.chunk).unwrap()),
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
            std::pin::Pin::new(this.chunks.get_mut(&this.chunk).unwrap()),
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
            std::pin::Pin::new(this.chunks.get_mut(&this.chunk).unwrap()),
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
            std::pin::Pin::new(this.chunks.get_mut(&this.chunk).unwrap()),
            cx,
        )
    }
}

impl WriteFinish for Writer<'_> {
    #[inline]
    fn poll_finish(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
