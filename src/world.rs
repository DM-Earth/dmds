use std::{ops::RangeInclusive, path::PathBuf};

use async_lock::RwLock;
use dashmap::{mapref, DashMap};

use crate::{range::SingleDimMapping, Element};

// As for now, array lengths don't support generic parameters,
// so it's necessary to declare another constant param.
// See #43408 (rust-lang/rust).

pub type Pos<const DIMS: usize> = [usize; DIMS];

pub struct World<T, const DIMS: usize> {
    cache: DashMap<Pos<DIMS>, RwLock<Vec<RwLock<T>>>>,
    path: PathBuf,
    mappings: [SingleDimMapping; DIMS],
}

pub struct DimPartDescriptor<R> {
    pub range: R,
    pub elements_per_chunk: usize,
}

impl<T: Element, const DIMS: usize> World<T, DIMS> {
    #[inline]
    pub fn new<R>(root: PathBuf, dim_parts: [DimPartDescriptor<R>; DIMS]) -> Self
    where
        R: std::ops::RangeBounds<u64>,
    {
        assert_eq!(
            T::DIMS,
            DIMS,
            "dimensions count of type and generic parameter should be equaled"
        );

        Self {
            cache: DashMap::new(),
            path: root,
            mappings: dim_parts
                .map(|value| SingleDimMapping::new(value.range, value.elements_per_chunk)),
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

struct PosBox<const DIMS: usize> {
    /// The most negative point of the box.
    start: Pos<DIMS>,
    /// The most positive point of the box.
    end: Pos<DIMS>,
}

impl<const DIMS: usize> PosBox<DIMS> {
    /// Constructs a box from given ranges of each dimensions.
    ///
    /// # Panics
    ///
    /// Panics if any range's start > end.
    #[inline]
    fn from_ranges(ranges: [RangeInclusive<usize>; DIMS]) -> Self {
        Self {
            start: ranges.map(|value| {
                debug_assert!(
                    value.start() <= value.end(),
                    "start should le than end of range {value:?}"
                );
                *value.start()
            }),
            end: ranges.map(|value| *value.end()),
        }
    }
}

struct RawShapeSlice<const DIMS: usize> {
    boxes: Vec<PosBox<DIMS>>,
}
