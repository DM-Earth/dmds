use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::Poll,
};

use futures_lite::{stream::CountFuture, Stream};
use pin_project_lite::pin_project;

use crate::{Data, IoHandle};

use super::{select::Shape, World};

struct Lazy<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
}

struct FromBytesFuture<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    read: Pin<&'a mut Io::Read>,

    buf: FromBytesBuf,
}

enum FromBytesBuf {
    Read(Vec<u8>),
    Pening(usize),
    Done,
}

impl<T: Data, const DIMS: usize, Io: IoHandle> Future for FromBytesFuture<'_, T, DIMS, Io> {
    type Output = futures_lite::io::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match this.buf {
            FromBytesBuf::Read(ref mut slice) => T::decode(, , ),
            FromBytesBuf::Pening(_) => todo!(),
            FromBytesBuf::Done => todo!(),
        }
    }
}

enum ChunkIter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    Pre(Pin<Box<dyn std::future::Future<Output = futures_lite::io::Result<Io::Read>> + Send + 'a>>),
    InProcess(Io::Read, &'a World<T, DIMS, Io>),
}

impl<T: Data, const DIMS: usize, Io: IoHandle> Stream for ChunkIter<'_, T, DIMS, Io> {
    type Item = ();

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            ChunkIter::Pre(future) => if let Poll::Ready(value) = future.poll(cx) {},
            ChunkIter::InProcess(_, _) => todo!(),
        }
    }
}

pub struct Iter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    shape_iter: super::select::RawShapeIter<'a, DIMS>,
}

impl<T: Data, const DIMS: usize, Io: IoHandle> Stream for Iter<'_, T, DIMS, Io> {
    type Item = futures_lite::io::Result<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(ref mut future) = this.future {
            match future.as_mut().poll(cx) {
                std::task::Poll::Ready(Some(value)) => std::task::Poll::Ready(Some(value)),
                std::task::Poll::Ready(None) => {
                    if let Some(next) = this.shape_iter.next() {
                        let world = this.world;
                        *future = Box::pin(async move {
                            let read = world.io_handle.read_chunk(&next).await;
                            todo!()
                        });
                        std::pin::Pin::new(this).poll_next(cx)
                    } else {
                        std::task::Poll::Ready(None)
                    }
                }
                std::task::Poll::Pending => todo!(),
            }
        } else {
            todo!()
        }
    }
}
