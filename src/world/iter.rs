use std::pin::Pin;

use futures_lite::Stream;

use crate::{Element, IoHandle};

use super::{select::RawShape, World};

type Future<'a, T> =
    dyn std::future::Future<Output = Option<futures_lite::io::Result<T>>> + Send + 'a;

pub struct Iter<'a, T: Element, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    shape_iter: super::select::RawShapeIter<'a, DIMS>,
    future: Option<Pin<Box<Future<'a, T>>>>,
}

impl<T: Element, const DIMS: usize, Io: IoHandle> Stream for Iter<'_, T, DIMS, Io> {
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
