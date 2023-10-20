use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::Poll,
};

use async_lock::{RwLock, RwLockReadGuard};
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
pub struct Lazy<'a, T: Data, const DIMS: usize> {
    id: u64,
    // this should always be initialized in Io mode.
    dims: OnceLock<[u64; DIMS]>,

    read_type: ReadType<'a, T, DIMS>,
    value: OnceLock<Value<'a, T, DIMS>>,
}

enum ReadType<'a, T, const DIMS: usize> {
    Mem {
        chunk: Arc<dashmap::mapref::one::Ref<'a, Pos<DIMS>, RwLock<Chunk<T>>>>,
        guard: Arc<RwLockReadGuard<'a, Chunk<T>>>,
        lock: &'a RwLock<T>,
    },
    Io(bytes::Bytes),
}

enum Value<'a, T: Data, const DIMS: usize> {
    Ref(RefArc<'a, T, DIMS>),
    Direct(T),
}

struct RefArc<'a, T, const DIMS: usize> {
    _chunk: Arc<dashmap::mapref::one::Ref<'a, Pos<DIMS>, RwLock<Chunk<T>>>>,
    _guard_vec: Arc<RwLockReadGuard<'a, Chunk<T>>>,
    guard: RwLockReadGuard<'a, T>,
}

impl<T: Data, const DIMS: usize> Lazy<'_, T, DIMS> {
    /// Gets info of dimensions of the value.
    pub async fn dims(&self) -> Result<&[u64; DIMS], Error> {
        if let Some(dims) = self.dims.get() {
            return Ok(dims);
        }

        let val = self.get_or_init().await?;
        let mut dims = [0_u64; DIMS];
        dims[0] = self.id;

        for (index, dim) in dims.iter_mut().enumerate() {
            if index == 0 {
                continue;
            }
            *dim = val.value_of(index);
        }

        Ok(self.dims.get_or_init(|| dims))
    }

    /// Gets the inner value or initialize it if it's uninitialized.
    pub async fn get_or_init(&self) -> Result<&T, Error> {
        if let Some(value) = self.value.get() {
            return match value {
                Value::Ref(val) => Ok(&val.guard),
                Value::Direct(val) => Ok(val),
            };
        }

        match &self.read_type {
            ReadType::Mem { chunk, guard, lock } => {
                let rg = lock.read().await;
                Ok(
                    if let Value::Ref(val) = self.value.get_or_init(|| {
                        Value::Ref({
                            RefArc {
                                _chunk: chunk.clone(),
                                _guard_vec: guard.clone(),
                                guard: rg,
                            }
                        })
                    }) {
                        &val.guard
                    } else {
                        unreachable!()
                    },
                )
            }
            ReadType::Io(ref bytes) => {
                let _ = self.value.set(Value::Direct(
                    T::decode(self.dims.get().unwrap(), bytes.clone()).map_err(Error::Io)?,
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

enum ChunkFromIoIter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    Pre {
        _world: &'a World<T, DIMS, Io>,
        future: Pin<Box<IoReadFuture<'a, Io>>>,
    },
    InProgress {
        read: Io::Read,
        progress: InProgress<DIMS>,
    },
}

/// Represents the progress of reading a data from bytes.
///
/// # Ordering
///
/// - Dimensions
/// - Data length in bytes
/// - Data bytes
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

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for ChunkFromIoIter<'a, T, DIMS, Io> {
    type Item = std::io::Result<Lazy<'a, T, DIMS>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            ChunkFromIoIter::InProgress { read, progress, .. } => match progress {
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
                                    id: dims[0],
                                    dims: {
                                        let lock = OnceLock::new();
                                        lock.set(*dims).unwrap();
                                        lock
                                    },
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
            ChunkFromIoIter::Pre { future, .. } => {
                *this = Self::InProgress {
                    read: match ready!(future.as_mut().poll(cx)) {
                        Ok(val) => val,
                        Err(err) => {
                            return if err.kind() == std::io::ErrorKind::NotFound {
                                Poll::Ready(None)
                            } else {
                                Poll::Ready(Some(Err(err)))
                            }
                        }
                    },
                    progress: InProgress::Dims([Default::default(); DIMS]),
                };

                Pin::new(this).poll_next(cx)
            }
        }
    }
}

struct ChunkFromMemIter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    _world: &'a World<T, DIMS, Io>,

    chunk: Arc<dashmap::mapref::one::Ref<'a, Pos<DIMS>, RwLock<Chunk<T>>>>,
    guard: Arc<RwLockReadGuard<'a, Chunk<T>>>,

    iter: std::slice::Iter<'a, (u64, RwLock<T>)>,
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Iterator for ChunkFromMemIter<'a, T, DIMS, Io> {
    type Item = Lazy<'a, T, DIMS>;

    fn next(&mut self) -> Option<Self::Item> {
        let (id, lock) = self.iter.next()?;
        Some(Lazy {
            id: *id,
            dims: OnceLock::new(),
            read_type: ReadType::Mem {
                chunk: self.chunk.clone(),
                guard: self.guard.clone(),
                lock,
            },
            value: OnceLock::new(),
        })
    }
}

pub struct Iter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,

    shape: super::select::RawShapeIter<'a, DIMS>,
    current: ChunkIter<'a, T, DIMS, Io>,
}

enum ChunkIter<'a, T: Data, const DIMS: usize, Io: IoHandle> {
    Io(ChunkFromIoIter<'a, T, DIMS, Io>),
    MemReadChunk {
        map_ref: Arc<dashmap::mapref::one::Ref<'a, Pos<DIMS>, RwLock<Chunk<T>>>>,
        fut: async_lock::futures::Read<'a, Chunk<T>>,
    },
    Mem(ChunkFromMemIter<'a, T, DIMS, Io>),
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for Iter<'a, T, DIMS, Io> {
    type Item = Result<Lazy<'a, T, DIMS>, Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.current {
            ChunkIter::Io(ref mut iter) => {
                if let Some(val) = ready!(Pin::new(iter).poll_next(cx)) {
                    return Poll::Ready(Some(val.map_err(Error::Io)));
                }
            }
            ChunkIter::Mem(ref mut iter) => {
                if let Some(val) = iter.next() {
                    return Poll::Ready(Some(Ok(val)));
                }
            }
            ChunkIter::MemReadChunk {
                ref map_ref,
                ref mut fut,
            } => {
                let guard = ready!(Pin::new(fut).poll(cx));
                this.current = ChunkIter::Mem(ChunkFromMemIter {
                    _world: this.world,
                    chunk: map_ref.clone(),
                    iter: unsafe { std::mem::transmute(guard.iter()) },
                    guard: Arc::new(guard),
                })
            }
        }

        if let Some(pos) = this.shape.next() {
            if let Some(chunk_l) = this.world.cache.get(&pos) {
                this.current = ChunkIter::MemReadChunk {
                    fut: unsafe { std::mem::transmute(chunk_l.value().read()) },
                    map_ref: Arc::new(chunk_l),
                };
            } else {
                this.current = ChunkIter::Io(ChunkFromIoIter::Pre {
                    _world: this.world,
                    future: this.world.io_handle.read_chunk(pos),
                });
            }

            Pin::new(this).poll_next(cx)
        } else {
            Poll::Ready(None)
        }
    }
}
