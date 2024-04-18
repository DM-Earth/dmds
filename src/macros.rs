/// Creates a new world.
///
/// See [`crate::World::new`] for more information.
///
/// # Examples
///
/// ```
/// # use dmds::{*, mem_io_handle::*};
/// let world: World<[u64; 2], 2, _> = world! {
///     MemStorage::new(), 16 => ..1024, 8 => ..128
/// };
/// # let _ = world;
/// ```
///
/// # Panics
///
/// Panics when count of given dimensions and the
/// dimension count of data are different.
#[macro_export]
macro_rules! world {
    ($io:expr, $($ipc:literal | $dr:expr),+$(,)?) => {
        $crate::world!($io, $($ipc => $dr),*)
    };
    ($io:expr, $($ipc:expr => $dr:expr),+$(,)?) => {
        $crate::World::new([$($crate::Dim::new($ipc, $dr),)+], $io)
    };
}
