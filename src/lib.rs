mod range;
pub mod world;

/// Represents types stored directly in a dimensional world.
pub trait Element {
    /// Count of dimensions.
    const DIMS: usize;

    /// Gets the value of given dimension.
    ///
    /// dimension index starts from 0, which should be
    /// a unique data such as the only id.
    fn value_of(&self, dim: usize) -> u64;
}
