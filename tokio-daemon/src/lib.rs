use std::{path::PathBuf, pin::Pin};

use async_trait::async_trait;
use dashmap::DashSet;
use dmds::IoHandle;
use tokio::{fs::File, io::BufReader};

#[derive(Debug)]
pub struct FsHandle {
    path: PathBuf,
    flat: bool,

    invalid_paths: DashSet<PathBuf>,
}

#[async_trait]
impl IoHandle for FsHandle {
    type Read<'a> = FsReader<'a> where Self: 'a;

    async fn read_chunk<const DIMS: usize>(
        &self,
        pos: [usize; DIMS],
    ) -> std::io::Result<Self::Read<'_>> {
        let path = self.path(&pos);

        if self.invalid_paths.contains(&path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "requested file not found",
            ));
        }

        let result = File::open(&path).await;

        if let Err(ref err) = result {
            if err.kind() == std::io::ErrorKind::NotFound {
                self.invalid_paths.insert(path);
            }
        }

        Ok(FsReader {
            _handle: self,
            file: BufReader::new(result?),
        })
    }
}

impl FsHandle {
    pub async fn write_chunk<const DIMS: usize, T: dmds::Data>(
        &self,
        chunk: &dmds::Chunk<T, DIMS>,
    ) -> std::io::Result<()> {
        let path = self.path(chunk.pos());
        todo!()
    }

    fn path(&self, pos: &[usize]) -> PathBuf {
        if self.flat {
            let mut chunk = String::new();
            for dim in pos {
                chunk.push_str(&dim.to_string());
                chunk.push('_');
            }
            chunk.pop();

            let mut buf = self.path.to_owned();
            buf.push(chunk);
            buf
        } else {
            let mut buf = self.path.to_owned();
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
