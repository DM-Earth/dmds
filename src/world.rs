mod select;

use std::path::PathBuf;

use async_lock::RwLock;
use dashmap::{mapref, DashMap};

use crate::{range::SingleDimMapping, Element};

use self::select::RawShape;

// As for now, array lengths don't support generic parameters,
// so it's necessary to declare another constant param.
// See #43408 (rust-lang/rust).

pub type Pos<const DIMS: usize> = [usize; DIMS];

pub struct World<T, const DIMS: usize> {
    cache: DashMap<Pos<DIMS>, RwLock<Vec<RwLock<T>>>>,
    path: PathBuf,
    mappings: [SingleDimMapping; DIMS],
}

pub struct DimDescriptor<R> {
    pub range: R,
    pub elements_per_chunk: usize,
}

impl<T: Element, const DIMS: usize> World<T, DIMS> {
    #[inline]
    pub fn new<R>(root: PathBuf, dims: [DimDescriptor<R>; DIMS]) -> Self
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
            mappings: dims
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

pub struct Select<'a, T: Element, const DIMS: usize> {
    world: &'a World<T, DIMS>,
    slice: RawShape<DIMS>,
}

impl<T: Element, const DIMS: usize> Select<'_, T, DIMS> {}
