use std::{
    future::Future,
    pin::Pin,
    sync::{atomic, Arc, OnceLock},
    task::Poll,
};

use async_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use bytes::BufMut;
use futures_lite::{ready, AsyncRead, FutureExt, Stream};

use crate::{Data, IoHandle};

use super::{ChunkBuf, ChunkData, Pos, World};

/// A type load data lazily.
#[derive(Debug)]
pub struct Lazy<'w, T, const DIMS: usize, Io: IoHandle> {
    world: &'w World<T, DIMS, Io>,
    /// Pre-loaded identifier of this data.
    id: u64,

    /// Lazy-loaded dimensions of this data.
    /// This should always be initialized in IO mode.
    dims: OnceLock<[u64; DIMS]>,
    /// Position of chunk this data belongs to.
    chunk: Pos<DIMS>,

    method: LoadMethod<'w, T, DIMS>,
    /// The inner data.
    value: OnceLock<LazyInner<'w, T, DIMS>>,
}

/// Method of loading data.
#[derive(Debug)]
enum LoadMethod<'w, T, const DIMS: usize> {
    /// Load data from buffer pool in `World`.
    Mem {
        chunk: Arc<ChunkBuf<T, DIMS>>,
        guard: Arc<RwLockReadGuard<'w, ChunkData<T>>>,
        lock: &'w RwLock<T>,
    },

    /// Decode data from bytes.
    Io(bytes::Bytes),
}

#[derive(Debug)]
enum LazyInner<'w, T, const DIMS: usize> {
    Ref(RefArc<'w, T, DIMS>),
    RefMut(RefMutArc<'w, T, DIMS>),
    Direct(T),
}

#[derive(Debug)]
struct RefArc<'w, T, const DIMS: usize> {
    _chunk: Arc<ChunkBuf<T, DIMS>>,
    _guard: Arc<RwLockReadGuard<'w, ChunkData<T>>>,
    guard: Option<RwLockReadGuard<'w, T>>,
}

#[derive(Debug)]
struct RefMutArc<'w, T, const DIMS: usize> {
    _chunk: Arc<ChunkBuf<T, DIMS>>,
    _guard: Arc<RwLockReadGuard<'w, ChunkData<T>>>,
    guard: RwLockWriteGuard<'w, T>,
}

impl<'w, T: Data, const DIMS: usize, Io: IoHandle> Lazy<'w, T, DIMS, Io> {
    #[inline]
    pub fn id(&self) -> u64 {
        self.id
    }

    pub(super) fn into_inner(self) -> Option<T> {
        if let LazyInner::Direct(val) = self.value.into_inner()? {
            Some(val)
        } else {
            None
        }
    }

    /// Gets info of dimensions of the value.
    pub async fn dims(&self) -> Result<&[u64; DIMS], crate::Error> {
        if let Some(dims) = self.dims.get() {
            return Ok(dims);
        }

        let val = self.get().await?;
        let mut dims = [0_u64; DIMS];
        dims[0] = self.id;

        for (index, dim) in dims.iter_mut().enumerate() {
            if index != 0 {
                *dim = val.value_of(index);
            }
        }

        Ok(self.dims.get_or_init(|| dims))
    }

    /// Gets the inner value immutably or initialize it
    /// if it's uninitialized.
    pub async fn get(&self) -> Result<&T, crate::Error> {
        match self.value.get() {
            Some(LazyInner::Ref(val)) => val.guard.as_deref().ok_or(crate::Error::ValueNotFound),
            Some(LazyInner::RefMut(val)) => Ok(&val.guard),
            Some(LazyInner::Direct(val)) => Ok(val),
            None => self.init().await,
        }
    }

    /// Initialize the inner value immutably.
    pub(super) async fn init(&self) -> Result<&T, crate::Error> {
        match self.method {
            LoadMethod::Mem {
                ref chunk,
                ref guard,
                lock,
            } => {
                let rg = lock.read().await;

                if let LazyInner::Ref(val) = self.value.get_or_init(|| {
                    LazyInner::Ref({
                        RefArc {
                            _chunk: chunk.clone(),
                            _guard: guard.clone(),
                            guard: Some(rg),
                        }
                    })
                }) {
                    Ok(val.guard.as_deref().unwrap())
                } else {
                    unreachable!()
                }
            }
            LoadMethod::Io(ref bytes) => {
                let _ = self.value.set(LazyInner::Direct(
                    T::decode(self.dims.get().unwrap(), bytes.clone()).map_err(crate::Error::Io)?,
                ));

                if let Some(LazyInner::Direct(val)) = self.value.get() {
                    Ok(val)
                } else {
                    unreachable!()
                }
            }
        }
    }

    /// Gets the inner value mutably or initialize it
    /// if it's uninitialized.
    pub async fn get_mut(&mut self) -> Result<&mut T, crate::Error> {
        if let Some(LazyInner::RefMut(val)) = self
            .value
            .get_mut()
            // SAFETY: strange issue here, mysterious. Only this way could pass the compilation.
            .map::<&'w mut LazyInner<'w, T, DIMS>, _>(|e| unsafe {
                &mut *(e as *mut LazyInner<'w, T, DIMS>)
            })
        {
            Ok(&mut val.guard)
        } else {
            self.init_mut().await
        }
    }

    /// Initialize the inner value mutably.
    pub(super) async fn init_mut(&mut self) -> Result<&mut T, crate::Error> {
        match self.method {
            LoadMethod::Mem {
                ref chunk,
                ref guard,
                lock,
            } => {
                if let Some(LazyInner::Ref(val)) = self.value.get_mut() {
                    val.guard = None;
                }

                // Set the value with locking behavior,
                // or override the value directly if value already exists.
                if let Some((val, dst)) = self
                    .value
                    .set(LazyInner::RefMut(RefMutArc {
                        _chunk: chunk.clone(),
                        _guard: guard.clone(),
                        // Obtain the value lock's write guard here
                        guard: lock.write().await,
                    }))
                    .err()
                    .zip(self.value.get_mut())
                {
                    *dst = val
                }
                chunk.writes.fetch_add(1, atomic::Ordering::AcqRel);
            }
            LoadMethod::Io(_) => unsafe {
                self.load_chunk().await?;
            },
        }

        if let Some(LazyInner::RefMut(val)) = self.value.get_mut() {
            Ok(&mut val.guard)
        } else {
            unreachable!()
        }
    }

    /// Remove this data from the chunk buffer.
    ///
    /// If the chunk buffer does not exist, the chunk will
    /// be loaded into buffer pool.
    pub async fn burn(self) -> Result<(), crate::Error> {
        let this = self.get().await?;
        let id = this.value_of(0);
        let chunk = self.world.chunk_buf_of_data_or_load(this).await?;
        chunk.remove(id).await;

        Ok(())
    }

    /// Load the chunk buffer this data belongs to to the buffer pool,
    /// and fill this instance's lazy value with target data in chunk.
    async unsafe fn load_chunk(&mut self) -> Result<Arc<ChunkBuf<T, DIMS>>, crate::Error> {
        let chunk = self.world.load_chunk_buf(self.chunk).await;
        // Guard of a chunk.
        type Guard<'a, T> = RwLockReadGuard<'a, Vec<(u64, RwLock<T>)>>;
        // SAFETY: wrapping lifetime to 'w.
        let guard: Arc<Guard<'w, T>> = Arc::new(std::mem::transmute(chunk.data.read().await));
        let lock = &*(&guard
            .iter()
            .find(|e| e.0 == self.id)
            .ok_or(crate::Error::ValueNotFound)?
            .1 as *const RwLock<T>);

        if let Some(LazyInner::Ref(val)) = self.value.get_mut() {
            val.guard = None;
        }

        // Set the value with locking behavior,
        // or override the value directly if value already exists.
        if let Some((val, dst)) = self
            .value
            .set(LazyInner::RefMut(RefMutArc {
                _chunk: chunk.clone(),
                _guard: guard.clone(),
                guard: lock.write().await,
            }))
            .err()
            .zip(self.value.get_mut())
        {
            *dst = val
        }

        self.method = LoadMethod::Mem {
            lock,
            guard,
            chunk: chunk.clone(),
        };
        Ok(chunk)
    }
}

type IoReadFuture<'a, Io> = dyn std::future::Future<Output = futures_lite::io::Result<<Io as IoHandle>::Read<'a>>>
    + Send
    + 'a;

enum ChunkFromIoIter<'a, T, const DIMS: usize, Io: IoHandle> {
    Pre {
        world: &'a World<T, DIMS, Io>,
        chunk: [usize; DIMS],
        future: Pin<Box<IoReadFuture<'a, Io>>>,
    },
    InProgress {
        world: &'a World<T, DIMS, Io>,
        chunk: [usize; DIMS],
        read: Io::Read<'a>,
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
    Dims([U64Buf; DIMS]),
    Len([u8; 4], [u64; DIMS]),
    Data(bytes::BytesMut, [u64; DIMS]),
}

#[derive(Clone, Copy)]
enum U64Buf {
    Buf([u8; 8]),
    Num(u64),
}

impl Default for U64Buf {
    #[inline]
    fn default() -> Self {
        Self::Buf([0; 8])
    }
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for ChunkFromIoIter<'a, T, DIMS, Io> {
    type Item = std::io::Result<Lazy<'a, T, DIMS, Io>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this {
            ChunkFromIoIter::InProgress {
                read,
                progress,
                chunk,
                world,
            } => match progress {
                InProgress::Dims(dims) => {
                    for dim in &mut *dims {
                        if let U64Buf::Buf(buf) = dim {
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
                            *dim = U64Buf::Num(u64::from_be_bytes(*buf));
                        }
                    }

                    *progress = InProgress::Len(
                        [0; 4],
                        (*dims).map(|e| {
                            if let U64Buf::Num(num) = e {
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
                                let lock = OnceLock::new();
                                lock.set(*dims).unwrap();

                                Poll::Ready(Some(Ok(Lazy {
                                    id: dims[0],
                                    dims: lock,
                                    method: LoadMethod::Io(buf.clone().freeze()),
                                    value: OnceLock::new(),
                                    chunk: *chunk,
                                    world,
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
            ChunkFromIoIter::Pre {
                future,
                chunk,
                world,
            } => {
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
                    chunk: *chunk,
                    world: *world,
                };

                Pin::new(this).poll_next(cx)
            }
        }
    }
}

struct ChunkFromMemIter<'a, T, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    chunk_pos: [usize; DIMS],

    chunk: Arc<ChunkBuf<T, DIMS>>,
    guard: Arc<RwLockReadGuard<'a, ChunkData<T>>>,

    iter: std::slice::Iter<'a, (u64, RwLock<T>)>,
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Iterator for ChunkFromMemIter<'a, T, DIMS, Io> {
    type Item = Lazy<'a, T, DIMS, Io>;

    fn next(&mut self) -> Option<Self::Item> {
        let (id, lock) = self.iter.next()?;
        Some(Lazy {
            id: *id,
            chunk: self.chunk_pos,
            dims: OnceLock::new(),
            method: LoadMethod::Mem {
                chunk: self.chunk.clone(),
                guard: self.guard.clone(),
                lock,
            },
            value: OnceLock::new(),
            world: self.world,
        })
    }
}

/// An async iterator (namely stream) that iterates over a selection
/// of chunks.
pub struct Iter<'a, T, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,

    shape: super::select::RawShapeIter<'a, DIMS>,
    current: Option<ChunkIter<'a, T, DIMS, Io>>,
}

type ReadLockFut<'a, T> =
    dyn std::future::Future<Output = async_lock::RwLockReadGuard<'a, Vec<(u64, RwLock<T>)>>> + 'a;

enum ChunkIter<'a, T, const DIMS: usize, Io: IoHandle> {
    Io(ChunkFromIoIter<'a, T, DIMS, Io>),
    MemReadChunk {
        map_ref: Arc<ChunkBuf<T, DIMS>>,
        fut: Pin<Box<ReadLockFut<'a, T>>>,
        pos: [usize; DIMS],
    },
    Mem(ChunkFromMemIter<'a, T, DIMS, Io>),
}

impl<'a, T, const DIMS: usize, Io: IoHandle> Iter<'a, T, DIMS, Io> {
    #[inline]
    pub(super) const fn new(
        world: &'a World<T, DIMS, Io>,
        shape: super::select::RawShapeIter<'a, DIMS>,
    ) -> Self {
        Self {
            world,
            shape,
            current: None,
        }
    }
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for Iter<'a, T, DIMS, Io> {
    type Item = Result<Lazy<'a, T, DIMS, Io>, crate::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.current {
            Some(ChunkIter::Io(ref mut iter)) => {
                if let Some(val) = ready!(Pin::new(iter).poll_next(cx)) {
                    return Poll::Ready(Some(val.map_err(crate::Error::Io)));
                }
            }
            Some(ChunkIter::Mem(ref mut iter)) => {
                if let Some(val) = iter.next() {
                    return Poll::Ready(Some(Ok(val)));
                }
            }
            Some(ChunkIter::MemReadChunk {
                ref map_ref,
                ref mut fut,
                pos,
            }) => {
                let guard = ready!(Pin::new(fut).poll(cx));
                this.current = Some(ChunkIter::Mem(ChunkFromMemIter {
                    world: this.world,
                    chunk: map_ref.clone(),
                    iter: unsafe { std::mem::transmute(guard.iter()) },
                    guard: Arc::new(guard),
                    chunk_pos: pos,
                }))
            }
            None => (),
        }

        if let Some(pos) = this.shape.next() {
            if let Some(chunk_l) = this.world.chunk_bufs.get(&pos) {
                this.current = Some(ChunkIter::MemReadChunk {
                    // SAFETY: wrapping lifetime
                    fut: unsafe { std::mem::transmute(chunk_l.value().data.read().boxed()) },
                    map_ref: chunk_l.value().clone(),
                    pos,
                });
            } else {
                this.current = Some(ChunkIter::Io(ChunkFromIoIter::Pre {
                    world: this.world,
                    future: this.world.io_handle.read_chunk(pos),
                    chunk: pos,
                }));
            }

            Pin::new(this).poll_next(cx)
        } else {
            Poll::Ready(None)
        }
    }
}
