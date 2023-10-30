pub mod iter;
mod select;

use std::{
    ops::{RangeBounds, RangeInclusive},
    sync::{atomic::AtomicBool, Arc},
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

/// A cached chunk.
#[derive(Debug)]
struct Chunk<T, Io: IoHandle> {
    /// The inner data of this chunk.
    data: RwLock<ChunkData<T>>,

    /// The future used to be polled after most
    /// operations to this chunk,
    /// to write this chunk through IO operations
    /// without awating in the operation future.
    write_fut: std::sync::Mutex<Option<Io::Write>>,

    /// Indicates whether this chunk has been updated.
    dirty: AtomicBool,
}

/// A world representing collections of single type of
/// data stored in multi-dimensional chunks.
pub struct World<T, const DIMS: usize, Io: IoHandle> {
    /// Cached chunks of this world, for modifying data.
    cache: DashMap<Pos<DIMS>, Arc<Chunk<T, Io>>>,

    /// Dimension information of this world.
    mappings: [SingleDimMapping; DIMS],

    /// IO handler.
    io_handle: Io,
}

/// Descripts information of a single dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DimDescriptor<R> {
    /// Range of values in this dimension.
    pub range: R,

    /// Count of items per chunk in this dimension.
    pub items_per_chunk: usize,
}

impl<T: Data, const DIMS: usize, Io: IoHandle> World<T, DIMS, Io> {
    /// Creates a new world.
    ///
    /// # Panics
    ///
    /// Panics when count of given dimensions and the
    /// dimension count of data are different.
    pub fn new<R>(dims: [DimDescriptor<R>; DIMS], io_handle: Io) -> Self
    where
        R: std::ops::RangeBounds<u64>,
    {
        assert_eq!(
            T::DIMS,
            DIMS,
            "dimensions count of type and generic parameter should be equal"
        );

        Self {
            cache: DashMap::new(),
            mappings: dims.map(|value| SingleDimMapping::new(value.range, value.items_per_chunk)),
            io_handle,
        }
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

    /// Load the chunk of given chunk position, through IO operation.
    async fn load_chunk(&self, pos: Pos<DIMS>) -> std::io::Result<()> {
        if self.cache.contains_key(&pos) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "chunk already exists in cache",
            ));
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
        if self.cache.contains_key(&pos) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "chunk already exists in cache",
            ));
        }

        self.cache.insert(
            pos,
            Arc::new(Chunk {
                data: RwLock::new(items),
                write_fut: std::sync::Mutex::new(None),
                dirty: AtomicBool::new(false),
            }),
        );

        Ok(())
    }
}

/// Celection of chunks.
pub struct Select<'a, T, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    slice: Shape<DIMS>,
}

impl<T: Data, const DIMS: usize, Io: IoHandle> Select<'_, T, DIMS, Io> {
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
    /// that iterate datas.
    #[inline]
    pub fn iter(&self) -> iter::Iter<'_, T, DIMS, Io> {
        iter::Iter {
            world: self.world,
            shape: self.slice.iter(),
            current: None,
        }
    }
}
