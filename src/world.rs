pub mod iter;
mod select;

use std::{
    ops::{RangeBounds, RangeInclusive},
    sync::{atomic::AtomicU16, Arc},
};

use async_lock::RwLock;
use dashmap::DashMap;
use futures_lite::StreamExt;

use crate::{range::SingleDimMapping, Data, IoHandle};

use self::select::{PosBox, Shape};

// As for now, array lengths don't support generic parameters,
// so it's necessary to declare another constant param.
//
// See #43408 (rust-lang/rust).

pub type Pos<const DIMS: usize> = [usize; DIMS];
type ChunkData<T> = Vec<(u64, RwLock<T>)>;

/// A buffered chunk.
#[derive(Debug)]
struct Chunk<T> {
    /// The inner data of this chunk.
    data: RwLock<ChunkData<T>>,

    /// Indicates whether this chunk has been updated.
    writes: AtomicU16,
    /// Lock indicates whether a task is currently
    /// writing this chunk.
    lock_w: async_lock::Mutex<()>,
}

/// A world representing collections of single type of
/// data stored in multi-dimensional chunks.
///
/// # Data layout of a chunk
///
/// Chunks should be saved in bytes, so all items should be saved in bytes.
/// A saved chunk is a combination of bytes of items, so it should be `[item0][item1][item2]..`.
///
/// Layout of an item should be like this: (for example, if we have 2+ dimensions)
///
/// ```txt
///   Dimension Values
///   ________________________________________________________________________
///  /                                                                       /
/// ┌───────────────────────┬───────────────────────┬───────────────────────┬──────────────────────────────┬──────┐
/// │ Dim Value 0 (u64, be) │ Dim Value 1 (u64, be) │ .. (other dimensions) │ Data Length (u32, be, bytes) │ Data │
/// └───────────────────────┴───────────────────────┴───────────────────────┴──────────────────────────────┴──────┘
/// ```
pub struct World<T, const DIMS: usize, Io: IoHandle> {
    /// Buffered chunks of this world, for modifying data.
    buf_pool: DashMap<Pos<DIMS>, Arc<Chunk<T>>>,
    io_handle: Io,

    /// Dimension information of this world.
    mappings: [SingleDimMapping; DIMS],
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

        for (index, value1) in arr.iter_mut().enumerate() {
            if index == dim {
                if let Ok(v) = self.mappings[index].chunk_of(value) {
                    *value1 = v..=v
                } else {
                    return Select {
                        world: self,
                        slice: Shape::None,
                    };
                }
            } else {
                *value1 = self.mappings[index].chunk_range()
            }
        }

        Select {
            world: self,
            slice: Shape::Single(PosBox::new(arr)),
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
                        slice: Shape::None,
                    };
                }
            } else {
                *value = self.mappings[index].chunk_range()
            }
        }

        Select {
            world: self,
            slice: Shape::Single(PosBox::new(arr)),
        }
    }

    #[inline]
    fn should_clean_buf_pool(&self) -> bool {
        self.buf_pool.len() > self.chunks_limit && self.chunks_limit != 0
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
            buf_pool: DashMap::new(),
            chunks_limit: 24,
            mappings: dims.map(|value| SingleDimMapping::new(value.range, value.items_per_chunk)),
            io_handle,
        }
    }

    /// Load the chunk of given chunk position, through IO operation.
    async fn load_chunk(&self, pos: Pos<DIMS>) {
        if self.buf_pool.contains_key(&pos) {
            return;
        }

        // Clean buffer pool if it reaches the limit.
        if self.should_clean_buf_pool() {
            self.buf_pool
                .retain(|_, chunk| chunk.writes.load(std::sync::atomic::Ordering::Acquire) > 0);
        }

        let selection = Select {
            world: self,
            slice: Shape::Single(PosBox::new(pos.map(|e| e..=e))),
        };

        let mut stream = selection.iter();
        let mut items = Vec::new();

        while let Some(res) = stream.next().await {
            if let Ok(item) = res {
                let _ = item.init().await;
                let id = item.id();
                if let Some(e) = item.into_inner() {
                    items.push((id, RwLock::new(e)))
                }
            }
        }

        // There are await points before this point, so
        // double-check if the chunk exists.
        if self.buf_pool.contains_key(&pos) {
            return;
        }

        self.buf_pool.insert(
            pos,
            Arc::new(Chunk {
                data: RwLock::new(items),
                writes: AtomicU16::new(0),
                lock_w: async_lock::Mutex::new(()),
            }),
        );
    }
}

/// A selection of chunks.
pub struct Select<'a, T, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    slice: Shape<DIMS>,
}

impl<T, const DIMS: usize, Io: IoHandle> Select<'_, T, DIMS, Io> {
    /// Select a range of chunks in the given dimension,
    /// and intersect with current selection.
    #[inline]
    pub fn range_and(&mut self, dim: usize, range: impl RangeBounds<u64> + Clone) {
        if let Shape::Single(v) = self.world.range_select(dim, range).slice {
            self.slice.intersect(v)
        }
    }

    /// Select from a value in the given dimension,
    /// and intersect with current selection.
    #[inline]
    pub fn and(&mut self, dim: usize, value: u64) {
        if let Shape::Single(v) = self.world.select(dim, value).slice {
            self.slice.intersect(v)
        }
    }

    /// Select a range of chunks in the given dimension,
    /// and combine with current selection.
    #[inline]
    pub fn range_plus(&mut self, dim: usize, range: impl RangeBounds<u64> + Clone) {
        self.slice += self.world.range_select(dim, range).slice
    }

    /// Select from a value in the given dimension,
    /// and combine with current selection.
    #[inline]
    pub fn plus(&mut self, dim: usize, value: u64) {
        self.slice += self.world.select(dim, value).slice
    }

    /// Returns an async iterator (or `Stream`) of this selection
    /// that iterate data items.
    #[inline]
    pub fn iter(&self) -> iter::Iter<'_, T, DIMS, Io> {
        iter::Iter {
            world: self.world,
            shape: self.slice.iter(),
            current: None,
        }
    }
}
