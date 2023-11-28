pub mod iter;
mod select;

use std::{
    ops::{RangeBounds, RangeInclusive},
    sync::{atomic::AtomicUsize, Arc},
};

use async_lock::RwLock;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use futures_lite::StreamExt;

use crate::{range::DimMapping, Data, IoHandle};

use self::select::{PosBox, Shape};

// As for now, array lengths don't support generic parameters,
// so it's necessary to declare another constant param.
//
// See #43408 (rust-lang/rust).

pub type Pos<const DIMS: usize> = [usize; DIMS];
type ChunkData<T> = Vec<(u64, RwLock<T>)>;

/// A buffered chunk storing data in memory.
///
/// # Data layout
///
/// A chunk should be saved in bytes, so all items should be saved in bytes.
/// A saved chunk is a combination of bytes of items, so it should be `[item0][item1][item2]..` .
///
/// Layout of an item should be like this (for example if we have 2+ dimensions):
///
/// ```txt
///   Dimension Values
///   ________________________________________________________________________
///  /                                                                       /
/// ┌───────────────────────┬───────────────────────┬───────────────────────┬──────────────────────────────┬──────┐
/// │ Dim Value 0 (u64, be) │ Dim Value 1 (u64, be) │ .. (other dimensions) │ Data Length (u32, be, bytes) │ Data │
/// └───────────────────────┴───────────────────────┴───────────────────────┴──────────────────────────────┴──────┘
/// ```
#[derive(Debug)]
pub struct Chunk<T, const DIMS: usize> {
    /// The inner data of this chunk.
    pub(crate) data: RwLock<ChunkData<T>>,

    /// Indicates whether this chunk has been updated.
    writes: AtomicUsize,

    /// `Mutex` indicates whether a task is currently
    /// writing this chunk.
    lock_w: std::sync::Mutex<()>,

    mappings: Arc<[DimMapping; DIMS]>,
    pos: Pos<DIMS>,
}

impl<T, const DIMS: usize> Chunk<T, DIMS> {
    /// Gets the position of this chunk.
    #[inline]
    pub fn pos(&self) -> &Pos<DIMS> {
        &self.pos
    }

    /// Gets the count of writes to this chunk buffer.
    #[inline]
    pub fn writes(&self) -> usize {
        self.writes.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Remove the data with given id from this chunk buffer.
    ///
    /// Returns the removed data if it exists.
    pub async fn remove(&self, id: u64) -> Option<T> {
        let mut w = self.data.write().await;
        if let Some(index) = w.iter().position(|v| v.0 == id) {
            let (_, d) = w.remove(index);
            Some(d.into_inner())
        } else {
            None
        }
    }
}

impl<T: Data, const DIMS: usize> Chunk<T, DIMS> {
    /// Write this chunk to the given buffer, as bytes.
    ///
    /// After writing to the bytes buffer successfully,
    /// the writes count will be reset. See [`Self::writes`].
    ///
    /// For the data layout, see [`World`].
    pub async fn write_buf<B: BufMut>(&self, mut buf: B) -> std::io::Result<()> {
        // Obtain the write lock guard
        let Ok(_) = self.lock_w.try_lock() else {
            return Ok(());
        };

        let datas_read = self.data.read().await;
        for data in datas_read.iter() {
            let data_read = data.1.read().await;
            debug_assert_eq!(data.0, data_read.dim(0), "data id should be immutable");
            buf.put_u64(data.0);
            for dim_i in 1..T::DIMS {
                buf.put_u64(data_read.dim(dim_i));
            }

            let mut bytes = BytesMut::new();
            data_read.encode(&mut bytes)?;
            buf.put_u32(bytes.len() as u32);
            buf.put(bytes);
        }
        Ok(())
    }

    /// Insert the given data to this chunk buffer.
    ///
    /// If data with the id already exists, the data
    /// will be replaced by the given data, and be
    /// returned.
    ///
    /// # Panics
    ///
    /// Panics when dimension values of the given data
    /// is invalid for this chunk.
    pub async fn insert(&self, data: T) -> Option<T> {
        let mut vals = [0; DIMS];
        for (i, val) in vals.iter_mut().enumerate() {
            *val = data.dim(i);
        }

        assert!(self.vals_in_range(vals), "given data is invalid to this chunk. data dimension values: {vals:?}, chunk position: {:?}", self.pos);

        {
            let r = self.data.write().await;
            if let Some((_, d)) = r.iter().find(|v| v.0 == vals[0]) {
                let mut wd = d.write().await;
                let mut data = data;
                std::mem::swap(&mut data, &mut wd);
                return Some(data);
            }
        }

        self.data.write().await.push((vals[0], RwLock::new(data)));
        None
    }

    /// Insert the given data to this chunk buffer.
    ///
    /// If data with the id already exists, or the
    /// dimension values of the given data is invalid
    /// for this chunk, the given data will be returned
    /// in `Err` variant in `Result`.
    pub async fn try_insert(&self, data: T) -> Result<(), T> {
        let mut vals = [0; DIMS];
        for (i, val) in vals.iter_mut().enumerate() {
            *val = data.dim(i);
        }

        if !self.vals_in_range(vals) {
            return Err(data);
        }

        let mut w = self.data.write().await;

        if w.iter_mut().any(|v| v.0 == vals[0]) {
            Err(data)
        } else {
            w.push((vals[0], RwLock::new(data)));
            Ok(())
        }
    }

    /// Validates the given chunk position to dim mappings
    /// of this world.
    #[inline]
    fn vals_in_range(&self, dim_vals: [u64; DIMS]) -> bool {
        for ((a, b), c) in self
            .mappings
            .iter()
            .zip(self.pos.into_iter())
            .zip(dim_vals.into_iter())
        {
            if !a.chunk_of(c).map_or(false, |e| e == b) {
                return false;
            }
        }
        true
    }
}

/// A world containing chunks, in multi-dimensions.
#[derive(Debug)]
pub struct World<T, const DIMS: usize, Io: IoHandle> {
    /// Buffered chunks of this world, for modifying data.
    pub(crate) chunks_buf: DashMap<Pos<DIMS>, Arc<Chunk<T, DIMS>>>,

    /// The IO handler.
    io_handle: Io,

    /// Dimension information of this world.
    mappings: Arc<[DimMapping; DIMS]>,

    /// Limit of buffered chunks in this world,
    /// if possible to clean.
    ///
    /// `0` means no limit.
    chunks_limit: usize,
}

/// Describes information of a single dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dim<R> {
    /// Range of values in this dimension.
    pub range: R,

    /// Count of items per chunk in this dimension.
    pub items_per_chunk: usize,
}

impl<T, const DIMS: usize, Io: IoHandle> World<T, DIMS, Io> {
    /// Set the count limit of buffered chunks in this world.
    #[inline]
    pub fn set_chunks_limit(&mut self, limit: Option<usize>) {
        self.chunks_limit = limit.unwrap_or_default()
    }

    /// Select from a value in the given dimension.
    pub fn select(&self, dim: usize, value: u64) -> Select<'_, T, DIMS, Io> {
        const TEMP_RANGE: RangeInclusive<usize> = 0..=0;
        let mut arr = [TEMP_RANGE; DIMS];

        for (index, (value1, map)) in arr.iter_mut().zip(self.mappings.iter()).enumerate() {
            if index == dim {
                if let Ok(v) = map.chunk_of(value) {
                    *value1 = v..=v
                } else {
                    return Select {
                        world: self,
                        shape: Shape::None,
                    };
                }
            } else {
                *value1 = map.chunk_range()
            }
        }

        Select {
            world: self,
            shape: Shape::Single(PosBox::new(arr)),
        }
    }

    /// Select a range of chunks in the given dimension.
    pub fn range_select(
        &self,
        dim: usize,
        range: impl RangeBounds<u64> + Clone,
    ) -> Select<'_, T, DIMS, Io> {
        const TEMP_RANGE: RangeInclusive<usize> = 0..=0;
        let mut arr = [TEMP_RANGE; DIMS];

        for (index, value) in arr.iter_mut().enumerate() {
            if index == dim {
                if let Ok(v) = self.mappings[index].chunks_of(range.clone()) {
                    *value = v
                } else {
                    return Select {
                        world: self,
                        shape: Shape::None,
                    };
                }
            } else {
                *value = self.mappings[index].chunk_range()
            }
        }

        Select {
            world: self,
            shape: Shape::Single(PosBox::new(arr)),
        }
    }

    #[inline]
    fn should_clean_buf_pool(&self) -> bool {
        self.chunks_buf.len() > self.chunks_limit && self.chunks_limit != 0
    }

    /// Get the buffered chunk with given position.
    #[inline]
    pub fn chunk_buf_of_pos(&self, pos: Pos<DIMS>) -> Option<Arc<Chunk<T, DIMS>>> {
        self.chunks_buf.get(&pos).map(|r| r.clone())
    }

    /// Validates the given chunk position to dim mappings
    /// of this world.
    #[inline]
    fn pos_in_range(&self, pos: Pos<DIMS>) -> crate::Result<()> {
        for (map, val) in self.mappings.iter().zip(pos.into_iter()) {
            map.in_range(val as u64)?;
        }
        Ok(())
    }
}

impl<T: Data, const DIMS: usize, Io: IoHandle> World<T, DIMS, Io> {
    /// Creates a new world.
    ///
    /// # Panics
    ///
    /// Panics when count of given dimensions and the
    /// dimension count of data are different.
    pub fn new<R>(dims: [Dim<R>; DIMS], io_handle: Io) -> Self
    where
        R: std::ops::RangeBounds<u64>,
    {
        assert_eq!(
            T::DIMS,
            DIMS,
            "dimensions count of type and generic parameter should be equal"
        );
        assert_ne!(DIMS, 0, "there should be at least 1 dimensions");

        Self {
            chunks_buf: DashMap::new(),
            chunks_limit: 24,
            mappings: Arc::new(
                dims.map(|value| DimMapping::new(value.range, value.items_per_chunk)),
            ),
            io_handle,
        }
    }

    /// Gets the chunk buffer of given chunk position, through IO if not exist.
    ///
    /// If the requested chunk does not exist, an empty chunk buffer
    /// will be created for use.
    async fn load_chunk_buf(&self, pos: Pos<DIMS>) -> Arc<Chunk<T, DIMS>> {
        if let Some(val) = self.chunks_buf.get(&pos) {
            return val.clone();
        }

        // Clean buffer pool if it reaches the limit.
        if self.should_clean_buf_pool() {
            self.chunks_buf
                .retain(|_, chunk| chunk.writes.load(std::sync::atomic::Ordering::Acquire) > 0);
        }

        let mut items = Vec::new();

        {
            let selection = Select {
                world: self,
                shape: Shape::Single(PosBox::new(pos.map(|e| e..=e))),
            };

            let mut stream = selection.iter();

            while let Some(Ok(item)) = stream.next().await {
                let _ = item.init().await;
                let id = item.id();
                if let Some(e) = item.into_inner() {
                    items.push((id, RwLock::new(e)))
                }
            }
        }

        // There are await points before this point, so
        // double-check if the chunk exists.
        if let Some(val) = self.chunks_buf.get(&pos) {
            return val.clone();
        }

        let arc = Arc::new(Chunk {
            data: RwLock::new(items),
            writes: AtomicUsize::new(0),
            lock_w: std::sync::Mutex::new(()),
            mappings: self.mappings.clone(),
            pos,
        });
        self.chunks_buf.insert(pos, arc.clone());
        arc
    }

    /// Get the buffered chunk with given position.
    /// If the requested chunk buffer does not exist,
    /// a new chunk buffer will be loaded or created.
    pub async fn chunk_buf_of_pos_or_load(
        &self,
        pos: Pos<DIMS>,
    ) -> crate::Result<Arc<Chunk<T, DIMS>>> {
        if let Some(val) = self.chunk_buf_of_pos(pos) {
            Ok(val)
        } else {
            self.pos_in_range(pos)?;
            Ok(self.load_chunk_buf(pos).await)
        }
    }

    /// Gets the buffered chunk which the given data should be stored in.
    #[inline]
    pub fn chunk_buf_of_data(&self, data: &T) -> crate::Result<Option<Arc<Chunk<T, DIMS>>>> {
        Ok(self.chunk_buf_of_pos(self.chunk_pos_of_data(data)?))
    }

    /// Gets the buffered chunk which the given data should be stored in.
    /// If the requested chunk buffer does not exist,
    /// a new chunk buffer will be loaded or created.
    #[inline]
    pub async fn chunk_buf_of_data_or_load(&self, data: &T) -> crate::Result<Arc<Chunk<T, DIMS>>> {
        self.chunk_buf_of_pos_or_load(self.chunk_pos_of_data(data)?)
            .await
    }

    /// Gets position of chunk which the given data should be stored in.
    pub fn chunk_pos_of_data(&self, data: &T) -> crate::Result<Pos<DIMS>> {
        let mut pos = [0; DIMS];
        for (i, (val, map)) in pos.iter_mut().zip(self.mappings.iter()).enumerate() {
            *val = map.chunk_of(data.dim(i))?;
        }
        Ok(pos)
    }

    /// Insert the given data to the chunk buffer which the data should be stored in.
    ///
    /// If the chunk buffer does not exist, a new chunk buffer will be loaded or created.
    ///
    /// If data with the id already exists, the data
    /// will be replaced by the given data, and be
    /// returned.
    #[inline]
    pub async fn insert(&self, data: T) -> crate::Result<Option<T>> {
        Ok(self
            .chunk_buf_of_data_or_load(&data)
            .await?
            .insert(data)
            .await)
    }

    /// Insert the given data to the chunk buffer which the data should be stored in.
    ///
    /// If the chunk buffer does not exist, a new chunk buffer will be loaded or created.
    ///
    /// If data with the id already exists, or the
    /// dimension values of the given data is invalid
    /// for this chunk, the given data will be returned
    /// in `Err` variant in `Result`.
    #[inline]
    pub async fn try_insert(&self, data: T) -> Result<(), T> {
        if let Ok(chunk) = self.chunk_buf_of_data_or_load(&data).await {
            chunk.try_insert(data).await
        } else {
            Err(data)
        }
    }
}

/// A selection of chunks.
pub struct Select<'w, T, const DIMS: usize, Io: IoHandle> {
    world: &'w World<T, DIMS, Io>,
    shape: Shape<DIMS>,
}

impl<T, const DIMS: usize, Io: IoHandle> Select<'_, T, DIMS, Io> {
    /// Select a range of chunks in the given dimension,
    /// and intersect with current selection.
    #[inline]
    pub fn range_and(&mut self, dim: usize, range: impl RangeBounds<u64> + Clone) {
        if let Shape::Single(v) = self.world.range_select(dim, range).shape {
            self.shape.intersect(v)
        }
    }

    /// Select from a value in the given dimension,
    /// and intersect with current selection.
    #[inline]
    pub fn and(&mut self, dim: usize, value: u64) {
        if let Shape::Single(v) = self.world.select(dim, value).shape {
            self.shape.intersect(v)
        }
    }

    /// Select a range of chunks in the given dimension,
    /// and combine with current selection.
    #[inline]
    pub fn range_plus(&mut self, dim: usize, range: impl RangeBounds<u64> + Clone) {
        self.shape += self.world.range_select(dim, range).shape
    }

    /// Select from a value in the given dimension,
    /// and combine with current selection.
    #[inline]
    pub fn plus(&mut self, dim: usize, value: u64) {
        self.shape += self.world.select(dim, value).shape
    }

    /// Returns an async iterator (namely `Stream`) of this selection
    /// that iterate data items.
    #[inline]
    pub fn iter(&self) -> iter::Iter<'_, T, DIMS, Io> {
        iter::Iter::new(self.world, self.shape.iter())
    }
}
