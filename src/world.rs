use std::{
    iter::Sum,
    ops::{Add, AddAssign, RangeBounds, RangeInclusive},
    path::PathBuf,
};

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

/// Represents a box in dimensional space.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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
    /// Panics if any range's start > end. (DEBUG)
    fn new(ranges: [RangeInclusive<usize>; DIMS]) -> Self {
        let mut start_range = [0; DIMS];
        for i in ranges.iter().enumerate() {
            let value = i.1;
            debug_assert!(
                value.start() <= value.end(),
                "start should le than end of range {value:?}"
            );
            start_range[i.0] = *value.start()
        }

        Self {
            start: start_range,
            end: ranges.map(|value| *value.end()),
        }
    }

    /// Whether this box contains another box in space.
    #[inline]
    fn contains(&self, rhs: &Self) -> bool {
        self.start
            .iter()
            .enumerate()
            .all(|(index, value)| rhs.start[index] >= *value)
            && self
                .end
                .iter()
                .enumerate()
                .all(|(index, value)| rhs.end[index] <= *value)
    }

    /// Returns the intersection of this and another box.
    #[inline]
    fn intersect(self, rhs: Self) -> Option<Self> {
        const TEMP_RANGE: RangeInclusive<usize> = 0..=0;
        let mut ranges = [TEMP_RANGE; DIMS];

        for (index, value) in self.start.iter().enumerate() {
            let range = std::cmp::max(*value, rhs.start[index])
                ..=std::cmp::min(self.end[index], rhs.end[index]);
            if range.end() <= range.start() {
                return None;
            }
            ranges[index] = range;
        }

        Some(Self::new(ranges))
    }
}

impl<const DIMS: usize> Add for PosBox<DIMS> {
    type Output = RawShape<DIMS>;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        if self.contains(&rhs) {
            RawShape::Single(self)
        } else if rhs.contains(&self) {
            RawShape::Single(rhs)
        } else {
            RawShape::Multiple(vec![self, rhs])
        }
    }
}

#[cfg(test)]
mod box_tests {
    use crate::world::RawShape;

    use super::PosBox;

    #[test]
    fn creation() {
        let b = PosBox::new([2..=5, 10..=24]);

        assert_eq!(b.start, [2, 10]);
        assert_eq!(b.end, [5, 24]);
    }

    #[test]
    fn contain() {
        let b = PosBox {
            start: [2, 10],
            end: [5, 24],
        };

        assert!(b.contains(&PosBox {
            start: [3, 10],
            end: [3, 8],
        }))
    }

    #[test]
    fn intersect() {
        let b0 = PosBox::new([0..=10, 1..=11]);
        let b1 = PosBox::new([7..=17, 2..=5]);
        assert_eq!(b0.intersect(b1), Some(PosBox::new([7..=10, 2..=5])));

        let b2 = PosBox::new([7..=17, 11..=12]);
        assert!(b0.intersect(b2).is_none());
    }

    #[test]
    fn add() {
        let b0 = PosBox::new([0..=10, 1..=11]);
        let b1 = PosBox::new([1..=10, 3..=4]);
        assert_eq!(b0 + b1, RawShape::Single(b0));

        let b2 = PosBox::new([7..=17, 2..=5]);
        assert_eq!(b0 + b2, RawShape::Multiple(vec![b0, b2]));
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
enum RawShape<const DIMS: usize> {
    None,
    Single(PosBox<DIMS>),
    Multiple(Vec<PosBox<DIMS>>),
}

impl<const DIMS: usize> RawShape<DIMS> {
    fn intersect(&mut self, target: PosBox<DIMS>) {
        match self {
            RawShape::Single(value) => {
                if let Some(result) = value.intersect(target) {
                    *value = result
                } else {
                    *self = Self::None
                }
            }
            RawShape::Multiple(values) => {
                *self = values
                    .iter()
                    .filter_map(|value| value.intersect(target))
                    .sum()
            }
            _ => (),
        }
    }
}

mod rss_imp {
    use super::RawShape;

    #[inline(always)]
    pub(super) fn add<const DIMS: usize>(
        s: &RawShape<DIMS>,
        rhs: &RawShape<DIMS>,
    ) -> RawShape<DIMS> {
        match (s, rhs) {
            (RawShape::Single(v0), RawShape::Single(v1)) => *v0 + *v1,
            (RawShape::Single(v0), RawShape::Multiple(v1))
            | (RawShape::Multiple(v1), RawShape::Single(v0)) => {
                let mut vv1 = v1.clone();
                if !vv1.contains(v0) {
                    vv1.push(*v0);
                }
                RawShape::Multiple(vv1)
            }
            (RawShape::Multiple(v0), RawShape::Multiple(v1)) => {
                let mut vv1 = v1.iter().filter(|v| !v0.contains(v)).copied().collect();
                let mut vv0 = v0.clone();
                vv0.append(&mut vv1);
                RawShape::Multiple(vv0)
            }
            (this, RawShape::None) | (RawShape::None, this) => this.clone(),
        }
    }
}

impl<const DIMS: usize> Add for RawShape<DIMS> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (RawShape::Single(v0), RawShape::Single(v1)) => v0 + v1,
            (RawShape::Single(v0), RawShape::Multiple(mut v1))
            | (RawShape::Multiple(mut v1), RawShape::Single(v0)) => {
                if !v1.contains(&v0) {
                    v1.push(v0);
                }
                RawShape::Multiple(v1)
            }
            (RawShape::Multiple(mut v0), RawShape::Multiple(mut v1)) => {
                v1 = v1.into_iter().filter(|v| !v0.contains(v)).collect();
                v0.append(&mut v1);
                RawShape::Multiple(v0)
            }
            (this, RawShape::None) | (RawShape::None, this) => this,
        }
    }
}

impl<const DIMS: usize> AddAssign for RawShape<DIMS> {
    fn add_assign(&mut self, rhs: Self) {
        *self = rss_imp::add(self, &rhs)
    }
}

impl<const DIMS: usize> Sum<PosBox<DIMS>> for RawShape<DIMS> {
    #[inline]
    fn sum<I: Iterator<Item = PosBox<DIMS>>>(iter: I) -> Self {
        iter.map(|v| Self::Single(v)).sum()
    }
}

impl<const DIMS: usize> Sum for RawShape<DIMS> {
    fn sum<I: Iterator<Item = Self>>(mut iter: I) -> Self {
        let mut this = Some(iter.next().unwrap_or(Self::None));
        while let Some(value) = iter.next() {
            this = Some(this.take().unwrap() + value)
        }
        this.unwrap()
    }
}
