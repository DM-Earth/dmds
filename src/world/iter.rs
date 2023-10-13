use crate::{Element, IoHandle};

use super::{select::RawShape, World};

pub struct Iter<'a, T: Element, const DIMS: usize, Io: IoHandle> {
    world: &'a World<T, DIMS, Io>,
    slice: &'a RawShape<DIMS>,
}
