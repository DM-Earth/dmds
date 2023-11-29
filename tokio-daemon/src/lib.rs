use std::{path::PathBuf, pin::Pin};

use async_trait::async_trait;
use bytes::BytesMut;
use dashmap::DashSet;
use dmds::IoHandle;
use tokio::{fs::File, io::BufReader};

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

    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Read<'_>> {
        if self.invalid_chunks.contains(&pos[..]) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "requested file not found",
            ));
        }

        let path = self.path(&pos);
        let result = File::open(&path).await;

        if let Err(ref err) = result {
            if err.kind() == std::io::ErrorKind::NotFound {
                if self.invalid_chunks.len() > Self::IP_CAPACITY {
                    self.invalid_chunks.clear();
                }
                self.invalid_chunks.insert(Box::new(pos));
            }
        }

        Ok(FsReader {
            _handle: self,
            file: BufReader::new(result?),
        })
    }
}

impl FsHandle {
    const IP_CAPACITY: usize = 1024 * 10;

    /// Write a chunk to the file system.
    pub async fn write_chunk<const DIMS: usize, T: dmds::Data>(
        &self,
        chunk: &dmds::Chunk<T, DIMS>,
    ) -> std::io::Result<()> {
        let mut buf = BytesMut::new();
        chunk.write_buf(&mut buf).await?;
        let buf = buf.freeze();

        let path = self.path(chunk.pos());
        let mut file = File::options().write(true).append(false).open(path).await?;
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

#[derive(Debug)]
pub struct FsReader<'a> {
    _handle: &'a FsHandle,
    file: BufReader<File>,
}

impl futures_lite::AsyncRead for FsReader<'_> {
    #[inline]
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut buf = tokio::io::ReadBuf::new(buf);
        futures_lite::ready!(tokio::io::AsyncRead::poll_read(
            Pin::new(&mut self.get_mut().file),
            cx,
            &mut buf
        ))?;
        std::task::Poll::Ready(Ok(buf.filled().len()))
    }
}
