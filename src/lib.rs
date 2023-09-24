mod range;
pub mod world;

/// Represents types stored directly in a dimentional world.
pub trait Element {
    /// Count of dimentions.
    const DIMS: usize;

    /// Gets the value of given dimention.
    ///
    /// Dimention index starts from 0, which should be
    /// a unique data such as the only id.
    fn value_of(&self, dim: usize) -> u64;
}
