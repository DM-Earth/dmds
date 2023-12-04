use std::{
    iter::Sum,
    ops::{Add, AddAssign, RangeInclusive},
};

use super::Pos;

/// Represents a box in dimensional space.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PosBox<const DIMS: usize> {
    /// The most negative point of the box.
    start: Pos<DIMS>,
    /// The most positive point of the box.
    end: Pos<DIMS>,
}

#[derive(Debug)]
pub struct PosBoxIter<'a, const DIMS: usize> {
    pos_box: &'a PosBox<DIMS>,
    next: Pos<DIMS>,
    done: bool,
}

impl<const DIMS: usize> PosBoxIter<'_, DIMS> {
    fn bump(&mut self, dim: usize) -> bool {
        if dim == DIMS {
            false
        } else if self.next[dim] < self.pos_box.end[dim] {
            self.next[dim] += 1;
            true
        } else {
            self.next[dim] = self.pos_box.start[dim];
            self.bump(dim + 1)
        }
    }
}

impl<const DIMS: usize> Iterator for PosBoxIter<'_, DIMS> {
    type Item = Pos<DIMS>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.next;
        let done = self.done;
        self.done = !self.bump(0);
        if done {
            None
        } else {
            Some(res)
        }
    }
}

impl<const DIMS: usize> PosBox<DIMS> {
    /// Constructs a box from given ranges of each dimensions.
    ///
    /// # Panics
    ///
    /// Panics if any range's start > end. (DEBUG)
    pub(crate) fn new(ranges: [RangeInclusive<usize>; DIMS]) -> Self {
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

    /// Returen an iterator of this box.
    #[inline]
    fn iter(&self) -> PosBoxIter<'_, DIMS> {
        PosBoxIter {
            pos_box: self,
            next: self.start,
            done: false,
        }
    }
}

impl<const DIMS: usize> Add for PosBox<DIMS> {
    type Output = Shape<DIMS>;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        if self.contains(&rhs) {
            Shape::Single(self)
        } else if rhs.contains(&self) {
            Shape::Single(rhs)
        } else {
            Shape::Multiple(vec![self, rhs])
        }
    }
}

#[cfg(test)]
mod box_tests {
    use super::{PosBox, Shape};

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
        assert_eq!(b0 + b1, Shape::Single(b0));

        let b2 = PosBox::new([7..=17, 2..=5]);
        assert_eq!(b0 + b2, Shape::Multiple(vec![b0, b2]));
    }

    #[test]
    fn iter() {
        let b = PosBox::new([1..=2, 1..=2]);
        let mut iter = b.iter();
        assert_eq!(iter.next(), Some([1, 1]));
        assert_eq!(iter.next(), Some([2, 1]));
        assert_eq!(iter.next(), Some([1, 2]));
        assert_eq!(iter.next(), Some([2, 2]));
        assert_eq!(iter.next(), None);
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Shape<const DIMS: usize> {
    None,
    Single(PosBox<DIMS>),
    Multiple(Vec<PosBox<DIMS>>),
}

#[derive(Debug)]
pub struct ShapeIter<'a, const DIMS: usize> {
    shape: &'a Shape<DIMS>,
    current: Option<(usize, PosBoxIter<'a, DIMS>)>,
    done: Vec<Pos<DIMS>>,
}

impl<const DIMS: usize> ShapeIter<'_, DIMS> {
    fn fetch_next_usable_iter<'a>(
        shape: &'a Shape<DIMS>,
        iter: &mut PosBoxIter<'a, DIMS>,
        index: &mut usize,
    ) -> Option<Pos<DIMS>> {
        if *index + 1
            < if let Shape::Multiple(vec) = shape {
                vec.len()
            } else {
                1
            }
        {
            let mut iter_next = if let Shape::Multiple(vec) = shape {
                vec.get(*index + 1).unwrap().iter()
            } else {
                unreachable!()
            };
            if let Some(value) = iter_next.next() {
                *iter = iter_next;
                *index += 1;
                Some(value)
            } else {
                Self::fetch_next_usable_iter(shape, iter, index)
            }
        } else {
            None
        }
    }

    #[inline]
    fn bump(&mut self) -> Option<Pos<DIMS>> {
        if let Some(ref mut iter) = self.current {
            if let Some(value) = iter.1.next() {
                Some(value)
            } else if let Some(value) =
                Self::fetch_next_usable_iter(self.shape, &mut iter.1, &mut iter.0)
            {
                Some(value)
            } else {
                self.current = None;
                None
            }
        } else {
            None
        }
    }
}

impl<const DIMS: usize> Iterator for ShapeIter<'_, DIMS> {
    type Item = Pos<DIMS>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(v0) = self.bump() {
            if self.done.contains(&v0) {
                self.next()
            } else {
                self.done.push(v0);
                Some(v0)
            }
        } else {
            None
        }
    }
}

impl<const DIMS: usize> Shape<DIMS> {
    pub fn intersect(&mut self, target: PosBox<DIMS>) {
        match self {
            Shape::Single(value) => {
                if let Some(result) = value.intersect(target) {
                    *value = result
                } else {
                    *self = Self::None
                }
            }
            Shape::Multiple(values) => {
                *self = values
                    .iter()
                    .filter_map(|value| value.intersect(target))
                    .sum()
            }
            _ => (),
        }
    }

    pub fn iter(&self) -> ShapeIter<'_, DIMS> {
        ShapeIter {
            shape: self,
            current: match self {
                Shape::None => None,
                Shape::Single(value) => Some((0, value.iter())),
                Shape::Multiple(values) => values.first().map(|e| (0, e.iter())),
            },
            done: vec![],
        }
    }
}

mod rss_imp {
    use super::Shape;

    pub(super) fn add<const DIMS: usize>(s: &Shape<DIMS>, rhs: &Shape<DIMS>) -> Shape<DIMS> {
        match (s, rhs) {
            (Shape::Single(v0), Shape::Single(v1)) => *v0 + *v1,
            (Shape::Single(v0), Shape::Multiple(v1)) | (Shape::Multiple(v1), Shape::Single(v0)) => {
                let mut vv1 = v1.clone();
                if !vv1.contains(v0) {
                    vv1.push(*v0);
                }
                Shape::Multiple(vv1)
            }
            (Shape::Multiple(v0), Shape::Multiple(v1)) => {
                let mut vv1 = v1.iter().filter(|v| !v0.contains(v)).copied().collect();
                let mut vv0 = v0.clone();
                vv0.append(&mut vv1);
                Shape::Multiple(vv0)
            }
            (this, Shape::None) | (Shape::None, this) => this.clone(),
        }
    }
}

impl<const DIMS: usize> Add for Shape<DIMS> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Shape::Single(v0), Shape::Single(v1)) => v0 + v1,
            (Shape::Single(v0), Shape::Multiple(mut v1))
            | (Shape::Multiple(mut v1), Shape::Single(v0)) => {
                if !v1.contains(&v0) {
                    v1.push(v0);
                }
                Shape::Multiple(v1)
            }
            (Shape::Multiple(mut v0), Shape::Multiple(mut v1)) => {
                v1.retain(|v| !v0.contains(v));
                v0.append(&mut v1);
                Shape::Multiple(v0)
            }
            (this, Shape::None) | (Shape::None, this) => this,
        }
    }
}

impl<const DIMS: usize> AddAssign for Shape<DIMS> {
    fn add_assign(&mut self, rhs: Self) {
        *self = rss_imp::add(self, &rhs)
    }
}

impl<const DIMS: usize> Sum<PosBox<DIMS>> for Shape<DIMS> {
    fn sum<I: Iterator<Item = PosBox<DIMS>>>(iter: I) -> Self {
        iter.map(Self::Single).sum()
    }
}

impl<const DIMS: usize> Sum for Shape<DIMS> {
    fn sum<I: Iterator<Item = Self>>(mut iter: I) -> Self {
        let mut this = Some(iter.next().unwrap_or(Self::None));
        for value in iter {
            this = Some(this.take().unwrap() + value)
        }
        this.unwrap()
    }
}

#[cfg(test)]
mod raw_shape_tests {
    use super::{PosBox, Shape};

    #[test]
    fn iter() {
        let all_possible_values = [[0, 0], [0, 1], [1, 0], [1, 1], [1, 2], [1, 3]];
        let shape = Shape::Multiple(vec![
            PosBox::new([0..=1, 0..=1]),
            PosBox::new([1..=1, 1..=3]),
        ]);

        let vec: Vec<_> = shape.iter().collect();

        assert!(vec.iter().all(|e| all_possible_values.contains(e)));
        assert!(all_possible_values.iter().all(|e| vec.contains(e)));
        assert_eq!(vec.len(), all_possible_values.len());
    }
}
