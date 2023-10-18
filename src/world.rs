pub mod iter;
mod select;

use std::{
    ops::{RangeBounds, RangeInclusive},
    path::PathBuf,
};

use async_lock::RwLock;
use dashmap::{mapref, DashMap};

use crate::{range::SingleDimMapping, Element, IoHandle};

use self::select::{PosBox, Shape};

// As for now, array lengths don't support generic parameters,
// so it's necessary to declare another constant param.
// See #43408 (rust-lang/rust).

pub type Pos<const DIMS: usize> = [usize; DIMS];

pub struct World<T: Element, const DIMS: usize, Io: IoHandle> {
    cache: DashMap<Pos<DIMS>, RwLock<Vec<RwLock<T>>>>,
    path: PathBuf,
    mappings: [SingleDimMapping; DIMS],
    io_handle: Io,
}

pub struct DimDescriptor<R> {
    pub range: R,
    pub elements_per_chunk: usize,
}

impl<T: Element, const DIMS: usize, Io: IoHandle> World<T, DIMS, Io> {
    #[inline]
    pub fn new<R>(root: PathBuf, dims: [DimDescriptor<R>; DIMS], io_handle: Io) -> Self
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
            path: root,
            mappings: dims
                .map(|value| SingleDimMapping::new(value.range, value.elements_per_chunk)),
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
}

pub struct Ref<'a, T: Element, const DIMS: usize> {
    map_g: mapref::one::Ref<'a, Pos<DIMS>, RwLock<Vec<RwLock<T>>>>,
    vec_g: async_lock::RwLockReadGuard<'a, Vec<RwLock<T>>>,
    lock_g: async_lock::RwLockReadGuard<'a, T>,
}

pub struct RefMut<'a, T: Element, const DIMS: usize> {
    map_g: mapref::one::Ref<'a, Pos<DIMS>, RwLock<Vec<RwLock<T>>>>,
    vec_g: async_lock::RwLockReadGuard<'a, Vec<RwLock<T>>>,
    lock_g: async_lock::RwLockWriteGuard<'a, T>,
}

pub struct Select<'a, T: Element, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    slice: Shape<DIMS>,
}

impl<T: Element, const DIMS: usize, Io: IoHandle> Select<'_, T, DIMS, Io> {
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
}
