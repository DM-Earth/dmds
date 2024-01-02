use std::ops::{RangeBounds, RangeInclusive};

/// Maps single-dimension value to chunks in a certain range.
#[derive(Debug)]
pub struct DimMapping {
    range: RangeInclusive<u64>,
    /// Elements per chunk.
    spacing: u64,
    /// Max count of chunks.
    chunks_len: usize,
}

impl DimMapping {
    /// Creates a new mapping from given `range` and `elements_per_chunk`.
    ///
    /// # Panics
    ///
    /// Panics if the given `range` is not divisible by `elements_per_chunk`.
    pub fn new(range_bounds: impl RangeBounds<u64>, elements_per_chunk: u64) -> Self {
        let range: RangeInclusive<u64> = Wrapper(range_bounds).into();
        let diff = (*range.end() - *range.start()) as u128 + 1;
        let spacing = elements_per_chunk as u128;

        assert_eq!(
            diff % spacing,
            0,
            "length of range [{}, {}] is not divisible by {spacing}.",
            range.start(),
            range.end()
        );

        Self {
            range,
            spacing: spacing as u64,
            chunks_len: (diff / spacing) as usize,
        }
    }

    /// Gets the chunk position of a value in this mapping.
    pub fn chunk_of(&self, mut value: u64) -> crate::Result<usize> {
        self.in_range(value)?;
        value -= *self.range.start();
        let pos = (value / self.spacing) as usize;
        debug_assert!(pos < self.chunks_len);
        Ok(pos)
    }

    /// Gets range of chunks from given range bounds in this mapping.
    pub fn chunks_of(&self, range: impl RangeBounds<u64>) -> crate::Result<RangeInclusive<usize>> {
        Ok(match range.start_bound() {
            std::ops::Bound::Included(value) => self.chunk_of(*value)?,
            std::ops::Bound::Excluded(value) => self.chunk_of(*value + 1)?,
            std::ops::Bound::Unbounded => 0,
        }..=match range.end_bound() {
            std::ops::Bound::Included(value) => self.chunk_of(*value)?,
            std::ops::Bound::Excluded(value) => self.chunk_of(*value - 1)?,
            std::ops::Bound::Unbounded => self.chunks_len - 1,
        })
    }

    #[inline]
    pub fn in_range(&self, value: u64) -> crate::Result<()> {
        if self.range.contains(&value) {
            Ok(())
        } else {
            Err(crate::Error::ValueOutOfRange {
                range: (*self.range.start(), *self.range.end()),
                value,
            })
        }
    }

    #[inline]
    pub fn chunk_range(&self) -> RangeInclusive<usize> {
        0..=self.chunks_len - 1
    }
}

#[cfg(test)]
mod single_dim_map_tests {
    use super::DimMapping;

    #[test]
    fn chunk_locating() {
        let map = DimMapping::new(1..=9, 3);

        assert_eq!(map.chunk_of(1_u64).unwrap(), 0);
        assert_eq!(map.chunk_of(2_u64).unwrap(), 0);
        assert_eq!(map.chunk_of(5_u64).unwrap(), 1);
        assert_eq!(map.chunk_of(9_u64).unwrap(), 2);

        assert!(map.chunk_of(114_u64).is_err());
    }

    #[test]
    #[should_panic]
    fn invalid_creating() {
        let _ = DimMapping::new(1..=9, 4);
    }

    #[test]
    fn chunks_ranging() {
        let map = DimMapping::new(1..=9, 3);

        assert_eq!(map.chunks_of(2_u64..7_u64).unwrap(), 0..=1);
        assert_eq!(map.chunks_of(2_u64..=7_u64).unwrap(), 0..=2);
        assert_eq!(map.chunks_of(..7_u64).unwrap(), 0..=1);
        assert_eq!(map.chunks_of(5_u64..).unwrap(), 1..=2);
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Wrapper<T>(pub T);

impl<T> From<Wrapper<T>> for RangeInclusive<u64>
where
    T: RangeBounds<u64>,
{
    #[inline(always)]
    fn from(val: Wrapper<T>) -> Self {
        (match val.0.start_bound() {
            std::ops::Bound::Included(value) => *value,
            std::ops::Bound::Excluded(value) => value + 1,
            std::ops::Bound::Unbounded => 0,
        })..=(match val.0.end_bound() {
            std::ops::Bound::Included(value) => *value,
            std::ops::Bound::Excluded(value) => value - 1,
            std::ops::Bound::Unbounded => u64::MAX,
        })
    }
}
