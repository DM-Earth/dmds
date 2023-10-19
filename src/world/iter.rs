use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{self, AtomicUsize},
        Arc, OnceLock, Weak,
    },
    task::Poll,
};

use bytes::BufMut;
use futures_lite::{ready, AsyncRead, Stream};

use crate::{Data, IoHandle};

use super::World;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io err: {0}")]
    Io(futures_lite::io::Error),
    #[error("requiring value has been taken")]
    ValueTaken,
    #[error("requiring value not found")]
    ValueNotFound,
    #[error("depending stream updated.")]
    IterUpdated {
        expected: usize,
        current: Option<usize>,
    },
}

/// A type polls value lazily and immutably.
pub struct Lazy<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    dims: [u64; DIMS],

    read_type: ReadType<DIMS>,
    value: OnceLock<Value<'a, T, DIMS>>,
}

enum ReadType<const DIMS: usize> {
    Mem([usize; DIMS]),
    Io(bytes::Bytes),
}

enum Value<'a, T: Data, const DIMS: usize> {
    Ref(super::Ref<'a, T, DIMS>),
    Direct(T),
}

impl<T: Data, const DIMS: usize, Io: IoHandle> Lazy<'_, T, DIMS, Io> {
    /// Gets info of dimensions of the value.
    #[inline]
    pub fn dims(&self) -> &[u64; DIMS] {
        &self.dims
    }

    /// Gets the value inside this initializer or initialize it
    /// if uninitialized.
    pub async fn get_or_init(&self) -> Result<&T, Error> {
        if let Some(value) = self.value.get() {
            return match value {
                Value::Ref(val) => Ok(val),
                Value::Direct(val) => Ok(val),
            };
        }

        match self.read_type {
            ReadType::Mem(chunk) => {
                let _ = self.value.set(Value::Ref(
                    self.world
                        .get(&chunk, self.dims[0])
                        .await
                        .ok_or(Error::ValueNotFound)?,
                ));

                Ok(if let Some(Value::Ref(val)) = self.value.get() {
                    val
                } else {
                    unreachable!()
                })
            }
            ReadType::Io(ref bytes) => {
                let _ = self.value.set(Value::Direct(
                    T::decode(&self.dims, bytes.clone()).map_err(Error::Io)?,
                ));

                Ok(if let Some(Value::Direct(val)) = self.value.get() {
                    val
                } else {
                    unreachable!()
                })
            }
        }
    }
}

struct FromBytes<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    _world: &'a World<T, DIMS, Io>,
    read: Pin<&'a mut Io::Read>,
    dims: &'a [u64; DIMS],
    len: usize,
    buf: Option<bytes::BytesMut>,
}

impl<T: Data, const DIMS: usize, Io: IoHandle> Future for FromBytes<'_, T, DIMS, Io> {
    type Output = futures_lite::io::Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        if let Some(ref mut buf) = this.buf {
            match ready!(this.read.as_mut().poll_read(cx, buf)) {
                Ok(act_len) => {
                    if act_len != this.len {
                        return Poll::Ready(Err(futures_lite::io::Error::new(
                            futures_lite::io::ErrorKind::UnexpectedEof,
                            format!("read {act_len} bytes, expected {} bytes", self.len),
                        )));
                    }
                }
                Err(err) => return Poll::Ready(Err(err)),
            }

            let Some(buf) = this.buf.take() else {
                unreachable!()
            };

            let buf = buf.freeze();
            Poll::Ready(T::decode(this.dims, buf))
        } else {
            let mut buf = bytes::BytesMut::with_capacity(this.len);
            buf.put_bytes(0, this.len);
            this.buf = Some(buf);
            Pin::new(this).poll(cx)
        }
    }
}

type IoReadFuture<'a, Io: IoHandle> =
    dyn std::future::Future<Output = futures_lite::io::Result<<Io as IoHandle>::Read>> + Send + 'a;

enum ChunkFromBytesIter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    Pre {
        world: &'a World<T, DIMS, Io>,
        future: Pin<Box<IoReadFuture<'a, Io>>>,
    },
    InProgress {
        world: &'a World<T, DIMS, Io>,
        read: Io::Read,
        progress: InProgress<DIMS>,
    },
}

enum InProgress<const DIMS: usize> {
    Dims([([u8; 8], bool); DIMS]),
    Len([u8; 4]),
    Data(bytes::BytesMut, usize),
}

enum U64Cache {
    Buf([u8; 8]),
    Num(u64),
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for ChunkFromBytesIter<'a, T, DIMS, Io> {
    type Item = std::io::Result<Lazy<'a, T, DIMS, Io>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            ChunkFromBytesIter::InProgress { read, progress, .. } => match progress {
                InProgress::Dims(_) => todo!(),
                InProgress::Len(buf) => match ready!(Pin::new(read).poll_read(cx, buf)) {
                    Ok(4) => {
                        let len = u32::from_be_bytes(*buf) as usize;
                        let mut bytes = bytes::BytesMut::with_capacity(len);
                        bytes.put_bytes(0, len);
                        *progress = InProgress::Data(bytes, len);
                        Pin::new(this).poll_next(cx)
                    }
                    Ok(len) => Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("read {len} bytes of length, expected 4 bytes"),
                    )))),
                    Err(err) => Poll::Ready(Some(Err(err))),
                },
                InProgress::Data(_, _) => todo!(),
            },
            ChunkFromBytesIter::Pre { world, future } => {
                *this = Self::InProgress {
                    world: *world,
                    read: match ready!(future.as_mut().poll(cx)) {
                        Ok(val) => val,
                        Err(err) => return std::task::Poll::Ready(Some(Err(err))),
                    },
                    progress: InProgress::Dims([([0; 8], false); DIMS]),
                };

                Pin::new(this).poll_next(cx)
            }
        }
    }
}

pub struct Iter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    shape_iter: super::select::RawShapeIter<'a, DIMS>,
}

impl<T: Data, const DIMS: usize, Io: IoHandle> Stream for Iter<'_, T, DIMS, Io> {
    type Item = ();

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        todo!()
    }
}
