use std::ops::{RangeBounds, RangeInclusive};

/// Maps single-dimension value to chunks in a certain range.
pub struct SingleDimMapping {
    range: RangeInclusive<u64>,
    spacing: u64,
    chunks_len: usize,
}

impl SingleDimMapping {
    /// Creates a new mapping from given `range` and `elements_per_chunk`.
    ///
    /// # Panics
    ///
    /// Panics if the given `range` is not divisible by `elements_per_chunk`.
    pub fn new(range: RangeInclusive<u64>, elements_per_chunk: usize) -> Self {
        let diff = *range.end() - *range.start() + 1;
        let spacing = elements_per_chunk as u64;

        assert!(
            diff % spacing == 0,
            "length of range [{}, {}] is not divisible by {spacing}.",
            range.start(),
            range.end()
        );

        Self {
            range,
            spacing,
            chunks_len: (diff / spacing) as usize,
        }
    }

    /// Gets the chunk position of a value in this mapping.
    pub fn chunk_of<T>(&self, value: T) -> Result<usize, SingleDimMappingError>
    where
        T: Into<u64>,
    {
        let mut raw = value.into();
        self.in_range(raw)?;
        raw -= *self.range.start();
        let pos = (raw / self.spacing) as usize;
        assert!(pos < self.chunks_len);
        Ok(pos)
    }

    /// Gets range of chunks from given range bounds in this mapping.
    pub fn chunks_of<T>(
        &self,
        range: impl RangeBounds<T>,
    ) -> Result<RangeInclusive<usize>, SingleDimMappingError>
    where
        T: Into<u64> + Copy,
    {
        let start = match range.start_bound() {
            std::ops::Bound::Included(value) => self.chunk_of(Into::<u64>::into(*value))?,
            std::ops::Bound::Excluded(value) => self.chunk_of(Into::<u64>::into(*value) + 1)?,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(value) => self.chunk_of(Into::<u64>::into(*value))?,
            std::ops::Bound::Excluded(value) => self.chunk_of(Into::<u64>::into(*value) - 1)?,
            std::ops::Bound::Unbounded => self.chunks_len - 1,
        };

        Ok(start..=end)
    }

    #[inline(always)]
    fn in_range(&self, value: u64) -> Result<(), SingleDimMappingError> {
        if self.range.contains(&value) {
            Ok(())
        } else {
            Err(SingleDimMappingError::ValueOutOfRange {
                range: (*self.range.start(), *self.range.end()),
                value,
            })
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SingleDimMappingError {
    #[error("value {value} out of range [{}, {}]", range.0, range.1)]
    ValueOutOfRange { range: (u64, u64), value: u64 },
}

#[cfg(test)]
mod single_dim_map_tests {
    use super::SingleDimMapping;

    #[test]
    fn chunk_locating() {
        let map = SingleDimMapping::new(1..=9, 3);

        assert_eq!(map.chunk_of(1_u64).unwrap(), 0);
        assert_eq!(map.chunk_of(2_u64).unwrap(), 0);
        assert_eq!(map.chunk_of(5_u64).unwrap(), 1);
        assert_eq!(map.chunk_of(9_u64).unwrap(), 2);

        assert!(map.chunk_of(114_u64).is_err());
    }

    #[test]
    #[should_panic]
    fn invalid_creating() {
        let _ = SingleDimMapping::new(1..=9, 4);
    }

    #[test]
    fn chunks_ranging() {
        let map = SingleDimMapping::new(1..=9, 3);

        assert_eq!(map.chunks_of(2_u64..7_u64).unwrap(), 0..=1);
        assert_eq!(map.chunks_of(2_u64..=7_u64).unwrap(), 0..=2);
        assert_eq!(map.chunks_of(..7_u64).unwrap(), 0..=1);
        assert_eq!(map.chunks_of(5_u64..).unwrap(), 1..=2);
    }
}
