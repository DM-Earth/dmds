use futures_lite::Stream;

use crate::{Element, IoHandle};

use super::{select::RawShape, World};

pub struct Iter<'a, T: Element, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    shape_iter: super::select::RawShapeIter<'a, DIMS>,
    read: Option<Io::Read>,
}

enum ReadV<'a, Io: IoHandle + 'a> {
    Pending(Io::RetTypeOfReadChunk<'a, 'a, 'a>),
}

impl<T: Element, const DIMS: usize, Io: IoHandle> Stream for Iter<'_, T, DIMS, Io> {
    type Item = futures_lite::io::Result<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if let Some(ref mut read) = self.read {
            todo!()
        }
    }
}
