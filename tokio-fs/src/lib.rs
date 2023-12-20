use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use dashmap::DashSet;
use dmds::IoHandle;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};

#[cfg(test)]
mod tests;

/// File system handler.
#[derive(Debug)]
pub struct FsHandle {
    /// Root path of the database location.
    root: PathBuf,
    /// Whether the file path should be flat.
    flat: bool,

    /// Set of chunk positions that is not available.
    invalid_chunks: DashSet<Box<[usize]>>,
}

#[async_trait]
impl IoHandle for FsHandle {
    type Read<'a> = FsReader<'a> where Self: 'a;

    #[inline]
    fn hint_is_valid(&self, pos: &[usize]) -> bool {
        !self.invalid_chunks.contains(pos)
    }

    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<(u32, Self::Read<'_>)> {
        let path = self.path(&pos);
        let result = File::open(&path).await;

        if let Err(ref err) = result {
            if err.kind() == std::io::ErrorKind::NotFound {
                if self.invalid_chunks.len() > Self::IC_CAPACITY {
                    self.invalid_chunks.clear();
                }
                self.invalid_chunks.insert(Box::new(pos));
            }
        }
        let mut file = BufReader::new(result?);
        let mut buf = [0_u8; 4];
        file.read_exact(&mut &mut buf[..]).await?;

        Ok((
            u32::from_be_bytes(buf),
            FsReader {
                _handle: self,
                file,
            },
        ))
    }
}

impl FsHandle {
    const IC_CAPACITY: usize = 1024 * 10;

    /// Create a new file system handler.
    pub fn new<P: AsRef<Path>>(root: P, flat: bool) -> Self {
        Self {
            root: root.as_ref().to_owned(),
            flat,
            invalid_chunks: DashSet::new(),
        }
    }

    /// Write a chunk to the file system.
    pub async fn write_chunk<const DIMS: usize, T: dmds::Data>(
        &self,
        chunk: &dmds::Chunk<T, DIMS>,
    ) -> std::io::Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u32(T::VERSION);
        chunk.write_buf(&mut buf).await?;
        let buf = buf.freeze();

        let path = self.path(chunk.pos());
        let mut path_pop = path.clone();
        path_pop.pop();
        let path_pop = path_pop;
        if tokio::fs::read_dir(&path_pop).await.is_err() {
            tokio::fs::create_dir_all(path_pop).await?;
        }
        let mut file = File::create(path).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &buf).await?;
        tokio::io::AsyncWriteExt::flush(&mut file).await?;
        file.sync_all().await?;
        self.invalid_chunks.remove(&chunk.pos()[..]);
        Ok(())
    }

    fn path(&self, pos: &[usize]) -> PathBuf {
        if self.flat {
            let mut chunk = String::new();
            for dim in pos {
                chunk.push_str(&dim.to_string());
                chunk.push('_');
            }
            chunk.pop();

            let mut buf = self.root.to_owned();
            buf.push(chunk);
            buf
        } else {
            let mut buf = self.root.to_owned();
            for dim in pos {
                buf.push(dim.to_string());
            }
            buf
        }
    }
}

/// Reader for the file system.
#[derive(Debug)]
pub struct FsReader<'a> {
    _handle: &'a FsHandle,
    file: BufReader<File>,
}

impl futures::AsyncRead for FsReader<'_> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut buf = tokio::io::ReadBuf::new(buf);
        futures::ready!(tokio::io::AsyncRead::poll_read(
            Pin::new(&mut self.get_mut().file),
            cx,
            &mut buf
        ))?;
        std::task::Poll::Ready(Ok(buf.filled().len()))
    }
}

/// Shutdown handler for the world.
///
/// This handler will write all the dirty chunks to the file system
/// in a blocking thread when dropped.
#[derive(Debug)]
pub struct ShutdownHandle<T: dmds::Data, const DIMS: usize>
where
    T: Send + 'static,
{
    world: Arc<dmds::World<T, DIMS, FsHandle>>,
}

impl<T: dmds::Data + Send + 'static, const DIMS: usize> ShutdownHandle<T, DIMS> {
    /// Create a new shutdown handler.
    #[inline]
    pub fn new(world: Arc<dmds::World<T, DIMS, FsHandle>>) -> Self {
        Self { world }
    }
}

impl<T: dmds::Data + Send + 'static, const DIMS: usize> Drop for ShutdownHandle<T, DIMS> {
    fn drop(&mut self) {
        let world = self.world.clone();
        let join = std::thread::spawn(move || {
            let iter = world.chunks().filter_map(|chunk| {
                if chunk.writes() > 0 {
                    let chunk = chunk.value().clone();
                    let world = world.clone();
                    Some(async move {
                        let chunk = chunk;
                        world.io_handle().write_chunk(&chunk).await.unwrap();
                    })
                } else {
                    None
                }
            });

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(futures::future::join_all(iter));
        });

        let _result = join.join();
    }
}

/// Runs the daemon.
///
/// This function will write all dirty chunk buffers to the file system.
/// A [`ShutdownHandle`] will be created to write all the dirty chunks
/// when the daemon is ended.
pub async fn daemon<T: dmds::Data, const DIMS: usize>(
    world: Arc<dmds::World<T, DIMS, FsHandle>>,
    write_interval: Duration,
) where
    T: Send + 'static,
{
    const LEAST_WRITES: usize = 1;

    let _ = ShutdownHandle::new(world.clone());

    loop {
        tokio::time::sleep(write_interval).await;

        let iter = world.chunks().filter_map(|chunk| {
            if chunk.writes() >= LEAST_WRITES {
                let chunk = chunk.value().clone();
                let world = world.clone();
                Some(async move {
                    let chunk = chunk;
                    let _ = world.io_handle().write_chunk(&chunk).await;
                })
            } else {
                None
            }
        });

        futures::future::join_all(iter).await;
    }
}
