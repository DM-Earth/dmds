use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{self, AtomicUsize},
        Arc, OnceLock, Weak,
    },
    task::Poll,
};

use async_lock::{RwLock, RwLockReadGuardArc};
use bytes::BufMut;
use futures_lite::{ready, AsyncRead, Stream};

use crate::{Data, IoHandle};

use super::{Chunk, Pos, World};

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

    read_type: ReadType<'a, T, DIMS>,
    value: OnceLock<Value<'a, T, DIMS>>,
}

enum ReadType<'a, T, const DIMS: usize> {
    Mem {
        chunk: Arc<dashmap::mapref::one::Ref<'a, Pos<DIMS>, RwLock<Chunk<T>>>>,
        guard: RwLockReadGuardArc<Chunk<T>>,
    },
    Io(bytes::Bytes),
}

enum Value<'a, T: Data, const DIMS: usize> {
    Ref(RefArc<'a, T, DIMS>),
    Direct(T),
}

struct RefArc<'a, T, const DIMS: usize> {
    chunk: Arc<dashmap::mapref::one::Ref<'a, Pos<DIMS>, RwLock<Chunk<T>>>>,
    guard_vec: RwLockReadGuardArc<Chunk<T>>,
    guard: RwLockReadGuardArc<T>,
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
            ReadType::Mem { chunk, guard } => {
                let _ = self.value.set(Value::Ref(
                    self.world
                        .get(&chunk, self.dims[0])
                        .await
                        .ok_or(Error::ValueNotFound)?,
                ));

                Ok(if let Some(Value::Ref(val)) = self.value.get() {
                    &val.guard
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

type IoReadFuture<'a, Io> =
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
    Dims([U64Cache; DIMS]),
    Len([u8; 4], [u64; DIMS]),
    Data(bytes::BytesMut, [u64; DIMS]),
}

#[derive(Clone, Copy)]
enum U64Cache {
    Buf([u8; 8]),
    Num(u64),
}

impl Default for U64Cache {
    #[inline]
    fn default() -> Self {
        Self::Buf([0; 8])
    }
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for ChunkFromBytesIter<'a, T, DIMS, Io> {
    type Item = std::io::Result<Lazy<'a, T, DIMS, Io>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            ChunkFromBytesIter::InProgress {
                read,
                progress,
                world,
            } => match progress {
                InProgress::Dims(dims) => {
                    for dim in &mut *dims {
                        if let U64Cache::Buf(buf) = dim {
                            match ready!(Pin::new(&mut *read).poll_read(cx, buf)) {
                                Ok(8) => (),
                                Ok(actlen) => {
                                    return Poll::Ready(Some(Err(std::io::Error::new(
                                        std::io::ErrorKind::UnexpectedEof,
                                        format!("read {actlen} bytes of length, expected 8 bytes"),
                                    ))))
                                }
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            }
                            *dim = U64Cache::Num(u64::from_be_bytes(*buf));
                        }
                    }

                    *progress = InProgress::Len(
                        [0; 4],
                        (*dims).map(|e| {
                            if let U64Cache::Num(num) = e {
                                num
                            } else {
                                unreachable!()
                            }
                        }),
                    );

                    Pin::new(this).poll_next(cx)
                }
                InProgress::Len(buf, dims) => match ready!(Pin::new(read).poll_read(cx, buf)) {
                    Ok(4) => {
                        let len = u32::from_be_bytes(*buf) as usize;
                        let mut bytes = bytes::BytesMut::with_capacity(len);
                        bytes.put_bytes(0, len);
                        *progress = InProgress::Data(bytes, *dims);
                        Pin::new(this).poll_next(cx)
                    }
                    Ok(len) => Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("read {len} bytes of length, expected 4 bytes"),
                    )))),
                    Err(err) => Poll::Ready(Some(Err(err))),
                },
                InProgress::Data(buf, dims) => {
                    let len = buf.len();
                    match ready!(Pin::new(&mut *read).poll_read(cx, buf)) {
                        Ok(len_act) => {
                            if len == len_act {
                                Poll::Ready(Some(Ok(Lazy {
                                    world: *world,
                                    dims: *dims,
                                    read_type: ReadType::Io(buf.clone().freeze()),
                                    value: OnceLock::new(),
                                })))
                            } else {
                                Poll::Ready(Some(Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    format!("read {len_act} bytes of length, expected {len} bytes"),
                                ))))
                            }
                        }
                        Err(err) => Poll::Ready(Some(Err(err))),
                    }
                }
            },
            ChunkFromBytesIter::Pre { world, future } => {
                *this = Self::InProgress {
                    world: *world,
                    read: match ready!(future.as_mut().poll(cx)) {
                        Ok(val) => val,
                        Err(err) => return std::task::Poll::Ready(Some(Err(err))),
                    },
                    progress: InProgress::Dims([Default::default(); DIMS]),
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
