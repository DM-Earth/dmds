/*
    As for now, array lengths don't support generic parameters,
    so it's necessary to declare another constant param.
    See #43408 (rust-lang/rust).
*/

use async_lock::RwLock;
use dashmap::{mapref, DashMap};

use crate::Element;

pub struct World<T: Element, const DIMS: usize> {
    cache: DashMap<[usize; DIMS], Vec<RwLock<T>>>,
}

pub struct Ref<'a, T: Element, const DIMS: usize> {
    map_g: mapref::one::Ref<'a, [usize; DIMS], Vec<RwLock<T>>>,
    lock_g: async_lock::RwLockReadGuard<'a, T>,
}

pub struct RefMut<'a, T: Element, const DIMS: usize> {
    map_g: mapref::one::Ref<'a, [usize; DIMS], Vec<RwLock<T>>>,
    lock_g: async_lock::RwLockWriteGuard<'a, T>,
}
