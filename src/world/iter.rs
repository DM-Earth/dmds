use std::{
    collections::btree_map,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{atomic, Arc, OnceLock},
    task::Poll,
};

use async_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use bytes::BufMut;
use futures_lite::{AsyncRead, FutureExt, Stream};

use crate::{Data, IoHandle};

use super::{Chunk, ChunkData, Pos, World};

/// `Some` variant of [`futures_lite::ready`].
macro_rules! ready_opt {
    ($e:expr $(,)?) => {
        match $e {
            core::task::Poll::Ready(t) => t,
            core::task::Poll::Pending => return Some(core::task::Poll::Pending),
        }
    };
}

/// A cell loads data in world lazily.
#[derive(Debug)]
pub struct Lazy<'w, T, const DIMS: usize, Io: IoHandle> {
    world: &'w World<T, DIMS, Io>,
    /// Pre-initialized identifier of this data.
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
        chunk: Arc<Chunk<T, DIMS>>,
        guard: Arc<RwLockReadGuard<'w, ChunkData<T>>>,
        lock: &'w RwLock<Option<T>>,
    },

    /// Decode data from bytes.
    Io { version: u32, bin: bytes::Bytes },
}

#[derive(Debug)]
enum LazyInner<'w, T, const DIMS: usize> {
    Ref(RefArc<'w, T, DIMS>),
    RefMut(RefMutArc<'w, T, DIMS>),
    Direct(T),
}

#[derive(Debug)]
struct RefArc<'w, T, const DIMS: usize> {
    _chunk: Arc<Chunk<T, DIMS>>,
    _guard: Arc<RwLockReadGuard<'w, ChunkData<T>>>,
    guard: Option<RwLockReadGuard<'w, Option<T>>>,
}

#[derive(Debug)]
struct RefMutArc<'w, T, const DIMS: usize> {
    _chunk: Arc<Chunk<T, DIMS>>,
    _guard: Arc<RwLockReadGuard<'w, ChunkData<T>>>,
    guard: RwLockWriteGuard<'w, Option<T>>,
}

impl<'w, T: Data, const DIMS: usize, Io: IoHandle> Lazy<'w, T, DIMS, Io> {
    /// Gets the value of dimension `0` of this data.
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
    pub async fn dims(&self) -> crate::Result<&[u64; DIMS]> {
        if let Some(dims) = self.dims.get() {
            return Ok(dims);
        }

        let val = self.get().await?;
        let mut dims = [0_u64; DIMS];
        dims[0] = self.id;

        for (index, dim) in dims.iter_mut().enumerate() {
            if index != 0 {
                *dim = val.dim(index);
            }
        }

        Ok(self.dims.get_or_init(|| dims))
    }

    /// Gets the inner value immutably or initialize it
    /// if it's uninitialized.
    pub async fn get(&self) -> crate::Result<&T> {
        match self.value.get() {
            Some(LazyInner::Ref(val)) => val
                .guard
                .as_deref()
                .ok_or(crate::Error::ValueNotFound)
                .and_then(|val| val.as_ref().ok_or(crate::Error::ValueMoved)),
            Some(LazyInner::RefMut(val)) => val.guard.as_ref().ok_or(crate::Error::ValueMoved),
            Some(LazyInner::Direct(val)) => Ok(val),
            None => self.init().await,
        }
    }

    /// Initialize the inner value immutably.
    pub(super) async fn init(&self) -> crate::Result<&T> {
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
                    val.guard
                        .as_deref()
                        .unwrap()
                        .as_ref()
                        .ok_or(crate::Error::ValueMoved)
                } else {
                    unreachable!()
                }
            }
            LoadMethod::Io { version, ref bin } => {
                let _ = self.value.set(LazyInner::Direct(
                    T::decode(version, self.dims.get().unwrap(), bin.clone())
                        .map_err(crate::Error::Io)?,
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
    ///
    /// Make sure to call [`Self::close`] after modification
    /// related to dimensional values.
    pub async fn get_mut(&mut self) -> crate::Result<&mut T> {
        if let Some(LazyInner::RefMut(val)) = self
            .value
            .get_mut()
            // SAFETY: strange issue here, mysterious. Only this way could pass the compilation.
            .map::<&'w mut LazyInner<'w, T, DIMS>, _>(|e| unsafe {
                &mut *(e as *mut LazyInner<'w, T, DIMS>)
            })
        {
            val.guard.as_mut().ok_or(crate::Error::ValueMoved)
        } else {
            self.init_mut().await
        }
    }

    /// Move this data into the chunk it should belongs to
    /// if this data is not suited in the chunk it belongs to now.
    pub async fn close(self) -> crate::Result<()> {
        let mut this = self;
        let (world, old_chunk) = (this.world, this.chunk);
        let Some(LazyInner::RefMut(guard)) = this.value.get_mut() else {
            return Ok(());
        };
        debug_assert!(guard.guard.is_some());
        let val = guard.guard.as_mut().unwrap();

        let chunk = world.chunk_buf_of_data_or_load(val).await?;
        if chunk.pos() != &old_chunk {
            let val = guard.guard.take().unwrap();
            let _ = chunk.try_insert(val).await;
        }

        Ok(())
    }

    /// Initialize the inner value mutably.
    pub(super) async fn init_mut(&mut self) -> crate::Result<&mut T> {
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
            LoadMethod::Io { .. } => unsafe {
                self.load_chunk().await?;
            },
        }

        if let Some(LazyInner::RefMut(val)) = self.value.get_mut() {
            val.guard.as_mut().ok_or(crate::Error::ValueMoved)
        } else {
            unreachable!()
        }
    }

    /// Remove this data from the chunk buffer.
    ///
    /// If the chunk buffer does not exist, the chunk will
    /// be loaded into buffer pool.
    pub async fn destroy(self) -> crate::Result<()> {
        let this = self.get().await?;
        let id = this.dim(0);
        let chunk = self.world.chunk_buf_of_data_or_load(this).await?;
        chunk.remove(id).await;

        Ok(())
    }

    /// Load the chunk buffer this data belongs to to the buffer pool,
    /// and fill this instance's lazy value with target data in chunk.
    async unsafe fn load_chunk(&mut self) -> crate::Result<Arc<Chunk<T, DIMS>>> {
        let chunk = self.world.load_chunk_buf(self.chunk).await;
        // Guard of a chunk.
        type Guard<'a, T> = RwLockReadGuard<'a, super::ChunkData<T>>;

        // SAFETY: wrapping lifetime to 'w.
        let guard: Arc<Guard<'w, T>> = Arc::new(std::mem::transmute(chunk.data.read().await));
        let lock: &'w RwLock<Option<T>> =
            std::mem::transmute(guard.get(&self.id).ok_or(crate::Error::ValueNotFound)?);

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

type IoReadFuture<'a, Io> = dyn std::future::Future<Output = std::io::Result<(u32, <Io as IoHandle>::Read<'a>)>>
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
        version: u32,
        progress: InProgress<DIMS>,
    },
}

impl<T: Debug, const DIMS: usize, Io: IoHandle + Debug> std::fmt::Debug
    for ChunkFromIoIter<'_, T, DIMS, Io>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pre { world, chunk, .. } => f
                .debug_struct("Pre")
                .field("world", world)
                .field("chunk", chunk)
                .finish(),
            Self::InProgress { world, chunk, .. } => f
                .debug_struct("InProgress")
                .field("world", world)
                .field("chunk", chunk)
                .finish(),
        }
    }
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

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> ChunkFromIoIter<'a, T, DIMS, Io> {
    #[allow(clippy::type_complexity)]
    fn poll_next_inner(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Option<std::task::Poll<Option<std::io::Result<Lazy<'a, T, DIMS, Io>>>>> // None => call this func again.
    {
        let this = self;
        match this {
            ChunkFromIoIter::InProgress {
                read,
                progress,
                chunk,
                world,
                version,
            } => match progress {
                InProgress::Dims(dims) => {
                    for dim in &mut *dims {
                        if let U64Buf::Buf(buf) = dim {
                            match ready_opt!(Pin::new(&mut *read).poll_read(cx, buf)) {
                                Ok(8) => (),
                                Ok(0) => return Some(Poll::Ready(None)),
                                Ok(actlen) => {
                                    return Some(Poll::Ready(Some(Err(std::io::Error::new(
                                        std::io::ErrorKind::UnexpectedEof,
                                        format!("read {actlen} bytes of length, expected 8 bytes"),
                                    )))))
                                }
                                Err(err) => return Some(Poll::Ready(Some(Err(err)))),
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

                    None
                }
                InProgress::Len(buf, dims) => match ready_opt!(Pin::new(read).poll_read(cx, buf)) {
                    Ok(4) => {
                        let len = u32::from_be_bytes(*buf) as usize;
                        let mut bytes = bytes::BytesMut::with_capacity(len);
                        bytes.put_bytes(0, len);
                        *progress = InProgress::Data(bytes, *dims);
                        None
                    }
                    Ok(len) => Some(Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("read {len} bytes of length, expected 4 bytes"),
                    ))))),
                    Err(err) => Some(Poll::Ready(Some(Err(err)))),
                },
                InProgress::Data(buf, dims) => {
                    let len = buf.len();
                    match ready_opt!(Pin::new(&mut *read).poll_read(cx, buf)) {
                        Ok(len_act) => {
                            if len == len_act {
                                let lock = OnceLock::new();
                                lock.set(*dims).unwrap();
                                let id = dims[0];

                                let buf = buf.clone().freeze();

                                *progress = InProgress::Dims([U64Buf::Buf([0; 8]); DIMS]);

                                Some(Poll::Ready(Some(Ok(Lazy {
                                    id,
                                    dims: lock,
                                    method: LoadMethod::Io {
                                        bin: buf,
                                        version: *version,
                                    },
                                    value: OnceLock::new(),
                                    chunk: *chunk,
                                    world,
                                }))))
                            } else {
                                Some(Poll::Ready(Some(Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    format!("read {len_act} bytes of length, expected {len} bytes"),
                                )))))
                            }
                        }
                        Err(err) => Some(Poll::Ready(Some(Err(err)))),
                    }
                }
            },
            ChunkFromIoIter::Pre {
                future,
                chunk,
                world,
            } => {
                let (version, read) = match ready_opt!(future.as_mut().poll(cx)) {
                    Ok(val) => val,
                    Err(err) => {
                        return if err.kind() == std::io::ErrorKind::NotFound {
                            Some(Poll::Ready(None))
                        } else {
                            Some(Poll::Ready(Some(Err(err))))
                        }
                    }
                };
                *this = Self::InProgress {
                    read,
                    progress: InProgress::Dims([Default::default(); DIMS]),
                    chunk: *chunk,
                    world: *world,
                    version,
                };

                None
            }
        }
    }
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for ChunkFromIoIter<'a, T, DIMS, Io> {
    type Item = std::io::Result<Lazy<'a, T, DIMS, Io>>;

    #[inline]
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(val) = this.poll_next_inner(cx) {
                return val;
            }
        }
    }
}

#[derive(Debug)]
struct ChunkFromMemIter<'a, T, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    chunk_pos: [usize; DIMS],

    chunk: Arc<Chunk<T, DIMS>>,
    guard: Arc<RwLockReadGuard<'a, ChunkData<T>>>,

    map_interact: ChunkFromMemIterMapInteract<'a, T>,
}

#[derive(Debug)]
enum ChunkFromMemIterMapInteract<'a, T> {
    /// Specify the target of iteration. With the hint,
    /// the iterator will only iterate these hinted values,
    /// so that other unneeded values will not be iterated.
    Hint(ArcSliceIter<u64>),
    Iter(btree_map::Iter<'a, u64, RwLock<Option<T>>>),
}

#[derive(Debug)]
struct ArcSliceIter<T> {
    ptr: usize,
    data: Arc<[T]>,
}

impl<T: Copy> Iterator for ArcSliceIter<T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr < self.data.len() {
            let val = self.data[self.ptr];
            self.ptr += 1;
            Some(val)
        } else {
            None
        }
    }
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> ChunkFromMemIter<'a, T, DIMS, Io> {
    fn next_inner(&mut self) -> Option<Option<Lazy<'a, T, DIMS, Io>>> {
        let (id, lock);
        match self.map_interact {
            ChunkFromMemIterMapInteract::Hint(ref mut hint) => {
                if let Some(id_hint) = hint.next() {
                    if let Some(l) = self.guard.get(&id_hint) {
                        id = id_hint;
                        lock = unsafe {
                            //SAFETY: wrapping lifetime
                            std::mem::transmute(l)
                        };
                    } else {
                        return None;
                    }
                } else {
                    return Some(None);
                }
            }
            ChunkFromMemIterMapInteract::Iter(ref mut iter) => {
                let Some((i, l)) = iter.next() else {
                    return Some(None);
                };
                id = *i;
                lock = l;
            }
        }

        Some(Some(Lazy {
            id,
            chunk: self.chunk_pos,
            dims: OnceLock::new(),
            method: LoadMethod::Mem {
                chunk: self.chunk.clone(),
                guard: self.guard.clone(),
                lock,
            },
            value: OnceLock::new(),
            world: self.world,
        }))
    }
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Iterator for ChunkFromMemIter<'a, T, DIMS, Io> {
    type Item = Lazy<'a, T, DIMS, Io>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(val) = self.next_inner() {
                return val;
            }
        }
    }
}

/// An async iterator (namely stream) that iterates over a selection
/// of chunks.
#[derive(Debug)]
pub struct Iter<'a, T, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,

    shape: super::select::ShapeIter<'a, DIMS>,
    current: Option<ChunkIter<'a, T, DIMS, Io>>,

    hint: Arc<[u64]>,
}

type ReadLockFut<'a, T> =
    dyn std::future::Future<Output = async_lock::RwLockReadGuard<'a, super::ChunkData<T>>> + 'a;

enum ChunkIter<'a, T, const DIMS: usize, Io: IoHandle> {
    Io(ChunkFromIoIter<'a, T, DIMS, Io>),
    MemReadChunk {
        map_ref: Arc<Chunk<T, DIMS>>,
        fut: Pin<Box<ReadLockFut<'a, T>>>,
        pos: [usize; DIMS],
    },
    Mem(ChunkFromMemIter<'a, T, DIMS, Io>),
}

impl<'a, T, const DIMS: usize, Io: IoHandle> Iter<'a, T, DIMS, Io> {
    #[inline]
    pub(super) fn new(
        world: &'a World<T, DIMS, Io>,
        shape: super::select::ShapeIter<'a, DIMS>,
        hint: Arc<[u64]>,
    ) -> Self {
        Self {
            world,
            shape,
            current: None,
            hint,
        }
    }
}

impl<T: Debug, const DIMS: usize, Io: IoHandle + Debug> std::fmt::Debug
    for ChunkIter<'_, T, DIMS, Io>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(arg0) => f.debug_tuple("Io").field(arg0).finish(),
            Self::MemReadChunk { map_ref, pos, .. } => f
                .debug_struct("MemReadChunk")
                .field("map_ref", map_ref)
                .field("pos", pos)
                .finish(),
            Self::Mem(arg0) => f.debug_tuple("Mem").field(arg0).finish(),
        }
    }
}

unsafe impl<'a, R, T, const DIMS: usize, Io> Send for Iter<'a, T, DIMS, Io>
where
    R: Send,
    T: Send,
    Io: IoHandle<Read<'a> = R>,
{
}

unsafe impl<'a, R, T, const DIMS: usize, Io> Sync for Iter<'a, T, DIMS, Io>
where
    R: Sync,
    T: Sync,
    Io: IoHandle<Read<'a> = R>,
{
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Iter<'a, T, DIMS, Io> {
    #[allow(clippy::type_complexity)]
    fn poll_next_inner(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Option<std::task::Poll<Option<crate::Result<Lazy<'a, T, DIMS, Io>>>>> // None => call this func again.
    {
        match self.current {
            Some(ChunkIter::Io(ref mut iter)) => {
                if let Some(val) = ready_opt!(Pin::new(iter).poll_next(cx)) {
                    return Some(Poll::Ready(Some(val.map_err(crate::Error::Io))));
                }
            }
            Some(ChunkIter::Mem(ref mut iter)) => {
                if let Some(val) = iter.next() {
                    return Some(Poll::Ready(Some(Ok(val))));
                }
            }
            Some(ChunkIter::MemReadChunk {
                ref map_ref,
                ref mut fut,
                pos,
            }) => {
                let guard = ready_opt!(Pin::new(fut).poll(cx));
                self.current = Some(ChunkIter::Mem(ChunkFromMemIter {
                    world: self.world,
                    chunk: map_ref.clone(),
                    map_interact: if self.hint.is_empty() {
                        ChunkFromMemIterMapInteract::Iter(unsafe {
                            //SAFETY: wrapping lifetime
                            std::mem::transmute(guard.iter())
                        })
                    } else {
                        ChunkFromMemIterMapInteract::Hint(ArcSliceIter {
                            ptr: 0,
                            data: self.hint.clone(),
                        })
                    },
                    guard: Arc::new(guard),
                    chunk_pos: pos,
                }));
                return None;
            }
            None => (),
        }

        if let Some(pos) = self.shape.next() {
            if let Some(chunk_l) = self.world.chunks_buf.get(&pos) {
                self.current = Some(ChunkIter::MemReadChunk {
                    // SAFETY: wrapping lifetime
                    fut: unsafe { std::mem::transmute(chunk_l.value().data.read().boxed()) },
                    map_ref: chunk_l.value().clone(),
                    pos,
                });
            } else if self.world.io_handle.hint_is_valid(&pos) {
                self.current = Some(ChunkIter::Io(ChunkFromIoIter::Pre {
                    world: self.world,
                    future: self.world.io_handle.read_chunk(pos),
                    chunk: pos,
                }));
            } else {
                self.current = None;
                return None;
            }

            None
        } else {
            Some(Poll::Ready(None))
        }
    }
}

impl<'a, T: Data, const DIMS: usize, Io: IoHandle> Stream for Iter<'a, T, DIMS, Io> {
    type Item = crate::Result<Lazy<'a, T, DIMS, Io>>;

    #[inline]
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(val) = this.poll_next_inner(cx) {
                return val;
            }
        }
    }
}
